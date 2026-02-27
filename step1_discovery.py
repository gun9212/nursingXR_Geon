"""
=============================================================================
Source Discovery Module — 키워드 기반 자동 소스 탐색 엔진
=============================================================================
목적:
    키워드(예: "치매", "고혈압")를 입력받아 크롤링 가능한 소스(PDF/HTML)를
    자동으로 탐색하고 DiscoveredSource 리스트를 반환한다.

탐색 소스:
    1. Europe PMC REST API — Open Access 논문 PDF
    2. PubMed E-utilities API — 의학 문헌 검색 (PDF/HTML)
    3. Wikipedia 검색 — 관련 한글 위키피디아 항목 (HTML)
    4. 중앙치매센터(NID) 자료실 — 국내 가이드북 PDF

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
from pathlib import Path
# from playwright.sync_api import sync_playwright

import requests
from bs4 import BeautifulSoup

try:
    from openai import OpenAI
    import os
    from dotenv import load_dotenv
    load_dotenv()
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except ImportError:
    client = None

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

        # PMC XML API 엔드포인트 생성
        xml_url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"

        source = DiscoveredSource(
            title=item.get("title", "Untitled"),
            url=xml_url,
            format="xml",
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

PROJECT_ROOT = Path(__file__).resolve().parent
PDF_DIR = PROJECT_ROOT / "pdf"
PDF_DIR.mkdir(parents=True, exist_ok=True)


def discover_nid(keyword_ko: str, max_results: int = 5) -> List[DiscoveredSource]:
    """
    [Skip]: Playwright is currently disabled to speed up the environment setup.
    """
    logger.info(f"  [NID] Playwright is disabled. Skipping NID scraping for '{keyword_ko}'...")
    return []

    # Original code commented out to avoid playwright execution
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            search_url = f"https://www.nid.or.kr/info/dataroom_list.aspx?sword={keyword_ko}"
            page.goto(search_url, timeout=30000)
            
            # 목록 테이블 기다리기
            try:
                page.wait_for_selector(".body_table th.th_title a", timeout=10000)
            except Exception:
                logger.info("  [NID] 검색 결과가 없습니다.")
                browser.close()
                return []

            rows = page.locator(".body_table th.th_title a")
            count = rows.count()
            
            article_links = []
            for i in range(count):
                if len(article_links) >= max_results:
                    break
                link = rows.nth(i)
                title = link.inner_text().strip()
                href = link.get_attribute("href")
                
                # 제목에 키워드가 있는지 확인
                if keyword_ko in title and href:
                    article_links.append((title, href))

            for title, href in article_links:
                if len(sources) >= max_results:
                    break
                    
                article_url = f"https://www.nid.or.kr/info/{href}"
                article_page = context.new_page()
                try:
                    article_page.goto(article_url, timeout=30000)
                    # 다운로드 버튼(fn_download 포함) 찾기
                    pdf_links = article_page.locator("a[onclick*='/download/download.aspx']")
                    pdf_count = pdf_links.count()
                    
                    for j in range(pdf_count):
                        pdf_link = pdf_links.nth(j)
                        pdf_text = pdf_link.inner_text().lower()
                        
                        if ".pdf" in pdf_text or "download" in pdf_text:
                            logger.info(f"  [NID] 다운로드 트리거 발견: {pdf_text}")
                            with article_page.expect_download(timeout=60000) as download_info:
                                pdf_link.click()
                                
                            download = download_info.value
                            import re
                            safe_name = re.sub(r'[^\w\-.]', '_', title[:40]) + ".pdf"
                            download_path = PDF_DIR / safe_name
                            download.save_as(download_path)
                            
                            logger.info(f"  [NID] 로컬 PDF 저장 완료: {download_path.name}")
                            
                            source = DiscoveredSource(
                                title=title,
                                url=str(download_path.absolute()),
                                format="local_pdf", # 다운로드 불필요
                                source_name="중앙치매센터",
                                language="ko",
                                license_type="공공저작물 자유이용"
                            )
                            sources.append(source)
                            break # 게시글당 PDF 하나만
                            
                except Exception as e:
                    logger.warning(f"  [NID] 개별 게시글 크롤링 실패 ({article_url}): {e}")
                finally:
                    article_page.close()
                    
            browser.close()
            
    except Exception as e:
        logger.warning(f"  [NID] Playwright 동적 크롤링 전체 실패: {e}")

    logger.info(f"  [NID] PDF 소스 {len(sources)}개 획득")
    return sources


# ============================================================================
# 4. PubMed API 탐색
# ============================================================================
def discover_pubmed(keyword_en: str, max_results: int = 5) -> List[DiscoveredSource]:
    """
    NCBI E-utilities API를 사용하여 PubMed 논문 검색.
    Free Full Text 필터를 적용하며, 가급적 PMC 연동 링크(pdf) 또는 PubMed 링크(html)를 반환.
    """
    logger.info(f"  [PubMed] E-utilities 탐색: '{keyword_en}'")
    sources = []
    
    search_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    query = f'"{keyword_en}" AND ({NURSING_TERMS}) AND free full text[sb]'
    params = {
        "db": "pubmed",
        "term": query,
        "retmode": "json",
        "retmax": max_results
    }
    try:
        resp = requests.get(search_url, params=params, timeout=15)
        resp.raise_for_status()
        pmids = resp.json().get("esearchresult", {}).get("idlist", [])
    except Exception as e:
        logger.warning(f"  [PubMed] ESearch 실패: {e}")
        return []
        
    if not pmids:
        logger.info("  [PubMed] 검색 결과가 없습니다.")
        return []
        
    summary_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    summary_params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "json"
    }
    try:
        resp = requests.get(summary_url, params=summary_params, timeout=15)
        resp.raise_for_status()
        summary_data = resp.json().get("result", {})
    except Exception as e:
        logger.warning(f"  [PubMed] ESummary 실패: {e}")
        return []

    for pmid in pmids:
        item = summary_data.get(pmid, {})
        if not item:
            continue
            
        title = item.get("title", "Untitled")
        
        pmcid = ""
        for articleid in item.get("articleids", []):
            if articleid.get("idtype") == "pmcid":
                raw_pmcid = articleid.get("value", "")
                # Ex: "pmc-id: PMC12924979;" -> "PMC12924979"
                match = re.search(r'(PMC?\d+)', raw_pmcid)
                if match:
                    pmcid = match.group(1)
                else:
                    pmcid = raw_pmcid.strip()
                break
                
        doi = ""
        for articleid in item.get("articleids", []):
            if articleid.get("idtype") == "doi":
                doi = articleid.get("value")
                break
                
        if pmcid:
            # PMC에 연동된 문서는 무조건 XML API 사용 (가장 빠르고 안정적)
            # 주의: PubMed 결과의 pmcid는 "PMC" 접두사가 없을 수도 있으므로 확인
            if not pmcid.startswith("PMC"):
                pmcid_str = f"PMC{pmcid}"
            else:
                pmcid_str = pmcid
            url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid_str}/fullTextXML"
            fmt = "xml"
            license_type = "Open Access XML"
        else:
            url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
            fmt = "html"
            license_type = "Free Full Text (HTML)"
            
        source = DiscoveredSource(
            title=title,
            url=url,
            format=fmt,
            source_name="PubMed",
            language="en",
            license_type=license_type,
            extra={
                "pmid": pmid,
                "pmcid": pmcid,
                "doi": doi,
                "journal": item.get("fulljournalname", ""),
            }
        )
        sources.append(source)
        
    logger.info(f"  [PubMed] 소스 {len(sources)}개 발견")
    return sources


# ============================================================================
# 5. Wikipedia 탐색
# ============================================================================
def discover_wikipedia(keyword_ko: str, max_results: int = 5) -> List[DiscoveredSource]:
    """
    wikipedia 패키지를 이용하여 한국어 위키피디아 페이지 검색.
    """
    logger.info(f"  [Wikipedia] 위키백과 탐색: '{keyword_ko}'")
    sources = []
    try:
        import wikipedia
    except ImportError:
        logger.warning("  [Wikipedia] wikipedia 미설치 — pip install wikipedia")
        return []
        
    try:
        wikipedia.set_lang("ko")
        # 키워드와 관련된 문서를 검색
        search_results = wikipedia.search(keyword_ko, results=max_results)
        
        for title in search_results:
            try:
                page = wikipedia.page(title, auto_suggest=False)
                source = DiscoveredSource(
                    title=page.title,
                    url=page.url,
                    format="html",
                    source_name="Wikipedia",
                    language="ko",
                    license_type="CC BY-SA",
                )
                sources.append(source)
            except wikipedia.exceptions.DisambiguationError as e:
                logger.info(f"    - '{title}' 식별 애매함 (Disambiguation) 건너뜀")
                continue
            except wikipedia.exceptions.PageError:
                continue
            except Exception as e:
                logger.warning(f"    - '{title}' 페이지 로드 실패: {e}")
                continue
                
    except Exception as e:
        logger.warning(f"  [Wikipedia] API 호출 실패: {e}")
        
    logger.info(f"  [Wikipedia] 소스 {len(sources)}개 발견")
    return sources


# ============================================================================
# 통합 탐색 함수
# ============================================================================
def translate_keyword_to_english(keyword: str) -> str:
    """
    미정의 한글 키워드가 들어올 경우 LLM을 통해 자동 영문 번역을 수행합니다.
    """
    if keyword in KEYWORD_MAP:
        return KEYWORD_MAP[keyword]
        
    if not client:
        return keyword

    try:
        logger.info(f"  [AI Translation] '{keyword}'의 영문 매핑이 없어 자동 번역을 시도합니다...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a medical & nursing term translator. Translate the given Korean keyword directly into the most appropriate, concise English scientific term. Return ONLY the translated English term, nothing else. For example, if given '당뇨', return 'diabetes'."},
                {"role": "user", "content": keyword}
            ],
            temperature=0.0
        )
        translated = response.choices[0].message.content.strip()
        logger.info(f"  [AI Translation] '{keyword}' -> '{translated}'")
        return translated
    except Exception as e:
        logger.warning(f"  [AI Translation] 번역 실패 ({e}). 원본 키워드를 사용합니다.")
        return keyword

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
    keyword_en = translate_keyword_to_english(keyword)
    logger.info(f"  영문 키워드: '{keyword_en}'")
    pmc_sources = discover_europepmc(keyword_en, max_results=max_results)
    all_sources.extend(pmc_sources)
    
    # 2. PubMed API (영문 검색)
    pubmed_sources = discover_pubmed(keyword_en, max_results=max_results)
    all_sources.extend(pubmed_sources)

    # 3. Wikipedia API (한글 검색)
    wiki_sources = discover_wikipedia(keyword, max_results=max_results)
    all_sources.extend(wiki_sources)

    # 4. KCI OAI-PMH (한글 학술논문 — 요양보호 동반 키워드 필터링)
    # kci_sources = discover_kci(keyword, max_results=max_results)
    # all_sources.extend(kci_sources)

    # 5. NID 자료실 (한국어 키워드)
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
