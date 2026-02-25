"""
=============================================================================
Source Discovery Module — 키워드 기반 자동 소스 탐색 엔진
=============================================================================
목적:
    키워드(예: "치매", "고혈압")를 입력받아 크롤링 가능한 소스(PDF/HTML)를
    자동으로 탐색하고 DiscoveredSource 리스트를 반환한다.

탐색 소스:
    1. Europe PMC REST API — Open Access 논문 PDF
    2. 중앙치매센터(NID) 자료실 — 국내 가이드북 PDF

필수 패키지:
    pip install requests beautifulsoup4 lxml

사용 예시:
    from discover import discover
    sources = discover("치매", max_results=5)
    for s in sources:
        print(f"[{s.format}] {s.title} → {s.url}")
=============================================================================
"""

import re
import logging
from dataclasses import dataclass
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("Discovery")

# ============================================================================
# 키워드 → 영문 매핑 (Europe PMC 검색용)
# ============================================================================
KEYWORD_MAP = {
    "치매": "dementia",
    "고혈압": "hypertension",
    "당뇨": "diabetes",
    "뇌졸중": "stroke",
    "파킨슨": "parkinson",
    "욕창": "pressure ulcer",
    "낙상": "fall prevention",
    "요실금": "urinary incontinence",
    "연하장애": "dysphagia",
    "섬망": "delirium",
}

# 공통 간호/요양 관련 필수 동반 검색어 (영문 — Europe PMC API AND 조건)
NURSING_TERMS = "caregiver OR nursing OR caregiving OR \"long-term care\""

# 한글 요양보호 필수 동반 키워드 (KCI/NID 클라이언트 측 필터링)
NURSING_TERMS_KO = ["요양보호", "간호", "돌봄", "부양", "장기요양", "케어", "간병"]


# ============================================================================
# 데이터 클래스
# ============================================================================
@dataclass
class DiscoveredSource:
    """탐색으로 발견된 개별 소스 정보"""
    title: str
    url: str
    format: str                  # "pdf" 또는 "html"
    source_name: str             # 예: "Europe PMC", "중앙치매센터"
    language: str = "en"         # "en" 또는 "ko"
    license_type: str = ""
    extra: dict = None           # DOI, PMCID 등 부가 정보

    def __post_init__(self):
        if self.extra is None:
            self.extra = {}


# ============================================================================
# 1. Europe PMC API 탐색
# ============================================================================
EUROPEPMC_API = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"


def discover_europepmc(keyword_en: str, max_results: int = 5) -> List[DiscoveredSource]:
    """
    Europe PMC REST API로 Open Access 논문 PDF를 탐색.

    검색 전략:
        - keyword AND (caregiver OR nursing) AND OPEN_ACCESS:Y
        - hasPDF:Y인 결과만 필터
        - PMC ID를 사용하여 PDF URL 생성
    """
    query = f'"{keyword_en}" AND ({NURSING_TERMS}) AND OPEN_ACCESS:Y'
    params = {
        "query": query,
        "format": "json",
        "pageSize": max_results * 2,  # hasPDF 필터링 대비 여유분
        "resultType": "lite",
    }

    logger.info(f"  [Europe PMC] 검색: {query}")

    try:
        resp = requests.get(EUROPEPMC_API, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"  [Europe PMC] API 호출 실패: {e}")
        return []

    results = data.get("resultList", {}).get("result", [])
    hit_count = data.get("hitCount", 0)
    logger.info(f"  [Europe PMC] 총 {hit_count}건 중 {len(results)}건 수신")

    sources = []
    for item in results:
        if item.get("hasPDF") != "Y":
            continue
        pmcid = item.get("pmcid", "")
        if not pmcid:
            continue

        # PMC PDF URL 생성
        pdf_url = f"https://europepmc.org/backend/ptpmcrender.fcgi?accid={pmcid}&blobtype=pdf"

        source = DiscoveredSource(
            title=item.get("title", "Untitled"),
            url=pdf_url,
            format="pdf",
            source_name="Europe PMC",
            language="en",
            license_type="Open Access",
            extra={
                "pmcid": pmcid,
                "pmid": item.get("pmid", ""),
                "doi": item.get("doi", ""),
                "journal": item.get("journalTitle", ""),
                "pub_year": item.get("pubYear", ""),
                "authors": item.get("authorString", ""),
            },
        )
        sources.append(source)
        if len(sources) >= max_results:
            break

    logger.info(f"  [Europe PMC] PDF 소스 {len(sources)}개 발견")
    return sources


# ============================================================================
# 2. KCI OAI-PMH 탐색 (한글 학술논문)
# ============================================================================
KCI_OAI_ENDPOINT = "https://open.kci.go.kr/oai/request"


def discover_kci(keyword_ko: str, max_results: int = 5) -> List[DiscoveredSource]:
    """
    KCI OAI-PMH로 한글 학술논문을 탐색.

    전략:
        1. OAI-PMH ListRecords로 최근 논문 메타데이터 수확
        2. dc:title, dc:subject, dc:description에서 키워드 + 요양보호 동반 키워드 필터링
        3. dc:identifier에서 DOI/원문 URL 추출
    """
    logger.info(f"  [KCI] OAI-PMH 탐색: '{keyword_ko}'")

    try:
        from sickle import Sickle
    except ImportError:
        logger.warning("  [KCI] sickle 미설치 — pip install sickle")
        return []

    try:
        sickle = Sickle(KCI_OAI_ENDPOINT, max_retries=2, timeout=15)
        # 최근 1년간 논문 수확
        from datetime import datetime, timedelta
        today = datetime.now()
        from_date = (today - timedelta(days=365)).strftime("%Y-%m-%d")
        until_date = today.strftime("%Y-%m-%d")

        records = sickle.ListRecords(
            metadataPrefix="oai_dc",
            set="ARTI",
            **{"from": from_date, "until": until_date}
        )
    except Exception as e:
        logger.warning(f"  [KCI] OAI-PMH 연결 실패: {e}")
        return []

    sources = []
    scanned = 0
    max_scan = max_results * 100  # 최대 스캔 수 (필터링 대비)

    for record in records:
        scanned += 1
        if scanned > max_scan:
            break

        try:
            meta = record.metadata
        except Exception:
            continue

        # 메타데이터 추출 (None 값 필터링)
        titles = [s for s in meta.get("title", []) if s]
        subjects = [s for s in meta.get("subject", []) if s]
        descriptions = [s for s in meta.get("description", []) if s]
        identifiers = [s for s in meta.get("identifier", []) if s]

        # 전체 텍스트 결합 (필터링용)
        full_text = " ".join(titles + subjects + descriptions).lower()

        # 키워드 매칭 — 키워드 + 요양보호 동반 키워드 모두 포함해야 함
        has_keyword = keyword_ko in full_text
        has_nursing = any(term in full_text for term in NURSING_TERMS_KO)

        if not (has_keyword and has_nursing):
            continue

        # DOI 또는 원문 URL 추출
        url = ""
        doi = ""
        for ident in identifiers:
            if "doi.org" in str(ident):
                doi = str(ident)
                url = doi
            elif str(ident).startswith("http"):
                url = str(ident)

        if not url:
            continue

        title = titles[0] if titles else "제목 없음"
        source = DiscoveredSource(
            title=title,
            url=url,
            format="html",  # KCI 원문은 대부분 HTML 페이지
            source_name="KCI",
            language="ko",
            license_type="학술논문",
            extra={
                "doi": doi,
                "subjects": subjects,
                "abstract": descriptions[0] if descriptions else "",
            },
        )
        sources.append(source)
        logger.info(f"  [KCI] 발견: {title[:50]}...")

        if len(sources) >= max_results:
            break

    logger.info(f"  [KCI] {scanned}건 스캔 → {len(sources)}개 소스 발견")
    return sources


# ============================================================================
# 3. 중앙치매센터(NID) 자료실 탐색
# ============================================================================
NID_RESOURCE_URLS = [
    "https://www.nid.or.kr/info/dataroom_list.aspx",
]


def discover_nid(keyword_ko: str, max_results: int = 5) -> List[DiscoveredSource]:
    """
    중앙치매센터 자료실에서 키워드 관련 PDF를 탐색.

    전략:
        - 자료실 목록 페이지를 HTML 크롤링
        - 제목에 키워드가 포함된 항목의 다운로드 링크 추출
        - .pdf, .hwp 확장자 링크 수집
    """
    logger.info(f"  [NID] 자료실 탐색: '{keyword_ko}'")
    sources = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }

    for base_url in NID_RESOURCE_URLS:
        try:
            resp = requests.get(base_url, headers=headers, timeout=15)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")
        except requests.RequestException as e:
            logger.warning(f"  [NID] 페이지 접근 실패: {base_url} ({e})")
            continue

        # 모든 링크에서 PDF/HWP 다운로드 URL 추출
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            title = a_tag.get_text(strip=True)

            # PDF 링크 필터
            is_pdf = (
                href.lower().endswith(".pdf")
                or "download" in href.lower()
                or "file" in href.lower()
            )
            if not is_pdf:
                continue

            # 키워드 매칭 (제목 또는 주변 텍스트에 키워드 포함)
            context = title
            parent = a_tag.find_parent("tr") or a_tag.find_parent("li")
            if parent:
                context = parent.get_text(strip=True)

            if keyword_ko not in context:
                continue

            # 상대 URL → 절대 URL 변환
            if href.startswith("/"):
                href = f"https://www.nid.or.kr{href}"
            elif not href.startswith("http"):
                href = f"https://www.nid.or.kr/{href}"

            source = DiscoveredSource(
                title=title or "NID 자료",
                url=href,
                format="pdf",
                source_name="중앙치매센터",
                language="ko",
                license_type="공공저작물 자유이용",
            )
            sources.append(source)
            if len(sources) >= max_results:
                break

    logger.info(f"  [NID] PDF 소스 {len(sources)}개 발견")
    return sources


# ============================================================================
# 통합 탐색 함수
# ============================================================================
def discover(keyword: str, max_results: int = 5) -> List[DiscoveredSource]:
    """
    키워드를 입력받아 크롤링 가능한 소스를 자동 탐색.

    Args:
        keyword: 한국어 키워드 (예: "치매", "고혈압")
        max_results: 소스당 최대 결과 수

    Returns:
        DiscoveredSource 리스트
    """
    logger.info("=" * 60)
    logger.info(f"[Source Discovery] 키워드: '{keyword}'")
    logger.info("=" * 60)

    all_sources: List[DiscoveredSource] = []

    # 1. Europe PMC (영문 키워드 변환)
    keyword_en = KEYWORD_MAP.get(keyword, keyword)
    logger.info(f"  영문 키워드: '{keyword_en}'")
    pmc_sources = discover_europepmc(keyword_en, max_results=max_results)
    all_sources.extend(pmc_sources)

    # 2. KCI OAI-PMH (한글 학술논문 — 요양보호 동반 키워드 필터링)
    # kci_sources = discover_kci(keyword, max_results=max_results)
    # all_sources.extend(kci_sources)

    # 3. NID 자료실 (한국어 키워드)
    nid_sources = discover_nid(keyword, max_results=max_results)
    all_sources.extend(nid_sources)

    # 결과 요약
    logger.info("-" * 60)
    logger.info(f"[Discovery 완료] 총 {len(all_sources)}개 소스 발견")
    for i, s in enumerate(all_sources, 1):
        lang_flag = "🇰🇷" if s.language == "ko" else "🇺🇸"
        logger.info(f"  {i}. {lang_flag} [{s.format.upper()}] {s.title[:60]}...")
    logger.info("=" * 60)

    return all_sources


# ============================================================================
# 단독 실행 테스트
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print()
    print("=" * 60)
    print(" Source Discovery 단독 테스트")
    print("=" * 60)
    print()

    results = discover("치매", max_results=3)
    print()
    for i, src in enumerate(results, 1):
        print(f"  [{i}] {src.title[:70]}")
        print(f"      URL: {src.url[:80]}")
        print(f"      형식: {src.format} | 언어: {src.language} | 출처: {src.source_name}")
        if src.extra:
            print(f"      DOI: {src.extra.get('doi', 'N/A')}")
        print()
