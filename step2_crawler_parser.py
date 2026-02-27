"""
=============================================================================
CrawlPipeline — 단일 소스 크롤링·파싱·검증 파이프라인
=============================================================================
목적:
    DiscoveredSource(discover.py)가 반환한 소스 1건을 입력받아:
    [다운로드 → 텍스트 추출 → 노이즈 제거 → Regex 트리 구조화 → JSON 스키마 검증]
    을 수행하고 구조화된 JSON 파일을 저장한다.

    ★ 절대 요약 금지(No Summarization): 원문 100% Verbatim 보존
    ★ 최대 깊이 파싱(Deep Bullet Point Parsing): sub_items까지 분리
    ★ 교육적 완결성: 뉘앙스·사례 하나도 누락 불가

필수 패키지:
    pip install requests beautifulsoup4 lxml jsonschema PyMuPDF
=============================================================================
"""

import re
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List

# pip install requests beautifulsoup4 lxml PyMuPDF
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

try:
    from llama_parse import LlamaParse
    import nest_asyncio
    nest_asyncio.apply()
    load_dotenv()
except ImportError:
    print("[오류] pip install llama-parse python-dotenv")
    raise

from step1_discovery import DiscoveredSource

# ============================================================================
# 로깅
# ============================================================================
logger = logging.getLogger("CrawlPipeline")

# ============================================================================
# 경로 상수
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_PATH = PROJECT_ROOT / "schema.json"
OUTPUT_DIR = PROJECT_ROOT / "output"
RAW_DIR = OUTPUT_DIR / "raw"
PROCESSED_DIR = OUTPUT_DIR / "processed"
PDF_DIR = PROJECT_ROOT / "pdf"
KST = timezone(timedelta(hours=9))

# ============================================================================
# PDF 노이즈 패턴 (최소화 - 쪽 번호 등만 제거)
# ============================================================================
NOISE_PATTERNS = {
    "page_number": re.compile(
        r"^[-─—]\s*\d+\s*[-─—]$|"
        r"^p\.?\s*\d+$|"
        r"^page\s*\d+$|"
        r"^\d{1,4}\s*$",
        re.IGNORECASE
    ),
    "footer_repeated": re.compile(
        r"^(보건복지부|국민건강보험공단|요양보호사\s*양성\s*표준교재|중앙치매센터)\s*$"
    ),
    "copyright": re.compile(
        r"copyright|ⓒ|©|all\s+rights\s+reserved|공공누리",
        re.IGNORECASE
    ),
    "pdf_artifact": re.compile(
        r"^[\x00-\x1f]+$|"
        r"^\s*[·•\-]{3,}\s*$|"
        r"^\s{20,}$"
    ),
}


# 더이상 트리 구조 DocumentData 클래스들을 사용하지 않습니다.


# ============================================================================
# 핵심 파이프라인
# ============================================================================
class CrawlPipeline:
    """
    단일 DiscoveredSource를 다운로드 → 파싱 → 검증 → JSON 저장.

    PDF/HTML 자동 분기:
        - source.format == "pdf" → requests 다운로드 + PyMuPDF 추출
        - source.format == "html" → requests + BeautifulSoup 추출
    """

    def __init__(self, source: DiscoveredSource, keyword: str):
        self.source = source
        self.keyword = keyword
        self.raw_text = ""
        self.parsed_data = None
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        PDF_DIR.mkdir(parents=True, exist_ok=True)

        logger.info(f"  파이프라인 초기화: [{source.format.upper()}] {source.title[:60]}")

    # ================================================================
    # Phase 2: 데이터 수집 — PDF/HTML 자동 분기
    # ================================================================
    def fetch_data(self) -> str:
        url = self.source.url
        logger.info(f"  [Fetch] 다운로드/텍스트추출 시작: {url[:80]}")

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9",
        }

        if self.source.source_name == "Wikipedia":
            self.raw_text = self._fetch_wikipedia()
        elif self.source.format == "local_pdf":
            local_path = Path(url.removeprefix("file://"))
            logger.info(f"  [Fetch] 로컬 PDF 바로 읽기 시작: {local_path.name}")
            self.raw_text = self._extract_text_from_pdf(local_path)
        elif self.source.format == "xml":
            self.raw_text = self._fetch_xml(url, headers)
        elif self.source.format == "pdf":
            self.raw_text = self._fetch_pdf(url, headers)
        else:
            self.raw_text = self._fetch_html(url, headers)

        logger.info(f"  [Fetch] 완료: {len(self.raw_text):,}글자, "
                    f"{len(self.raw_text.splitlines()):,}라인")
        return self.raw_text

    def _fetch_wikipedia(self) -> str:
        """위키피디아 패키지를 통해 깔끔한 순수 텍스트를 바로 가져옵니다."""
        import wikipedia
        wikipedia.set_lang("ko")
        try:
            page = wikipedia.page(self.source.title, auto_suggest=False)
            logger.info(f"  [Wikipedia] '{page.title}' 콘텐츠 로드 완료")

            safe_name = re.sub(r'[^\w\-.]', '_', self.source.title[:40]) + ".txt"
            txt_path = RAW_DIR / safe_name
            try:
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(page.content)
                logger.info(f"  [Wikipedia] 원문 텍스트 백업 저장: {txt_path.name}")
            except Exception as e:
                logger.warning(f"  [Wikipedia] 원문 텍스트 백업 저장 실패: {e}")

            return page.content
        except Exception as e:
            raise ValueError(f"Wikipedia 문서 가져오기 실패: {e}")

    def _fetch_pdf(self, url: str, headers: dict) -> str:
        headers["Accept"] = "application/pdf,*/*"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Increase timeout to 60s for large PDFs
                resp = requests.get(url, headers=headers, timeout=60, stream=True)
                resp.raise_for_status()
                break # Success
            except requests.RequestException as e:
                logger.warning(f"  [Fetch] PDF 다운로드 실패 (시도 {attempt+1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise ConnectionError(f"PDF 다운로드 최종 실패: {e} → {url}")
                import time
                time.sleep(2) # Backoff

        # 파일명 결정
        safe_name = re.sub(r'[^\w\-.]', '_', self.source.title[:40]) + ".pdf"
        pdf_path = PDF_DIR / safe_name

        with open(pdf_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"  [PDF] 저장: {pdf_path.name} ({pdf_path.stat().st_size:,} bytes)")

        return self._extract_text_from_pdf(pdf_path)

    def _fetch_html(self, url: str, headers: dict) -> str:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                resp.encoding = "utf-8"
                break
            except requests.RequestException as e:
                logger.warning(f"  [Fetch] HTML 크롤링 실패 (시도 {attempt+1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise ConnectionError(f"HTML 크롤링 최종 실패: {e} → {url}")
                import time
                time.sleep(2)

        # 원본 HTML 저장
        safe_name = re.sub(r'[^\w\-.]', '_', self.source.title[:40]) + ".html"
        html_path = RAW_DIR / safe_name
        try:
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(resp.text)
            logger.info(f"  [HTML] 원본 파일 백업 저장: {html_path.name}")
        except Exception as e:
            logger.warning(f"  [HTML] 원본 파일 백업 저장 실패: {e}")

        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup.find_all(["script", "style", "nav", "aside", "footer"]):
            tag.decompose()
        content = soup.find("div", class_="content") or soup.find("main") or soup.body
        return content.get_text(separator="\n", strip=True) if content else ""

    def _fetch_xml(self, url: str, headers: dict) -> str:
        """Europe PMC XML 구조(JATS)에서 순수 텍스트만 추출"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                logger.warning(f"  [Fetch] XML 크롤링 실패 (시도 {attempt+1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise ConnectionError(f"XML API 호출 최종 실패: {e} → {url}")
                import time
                time.sleep(2)

        soup = BeautifulSoup(resp.content, "xml")
        
        # 레퍼런스, 표, 수식 등 잡음 제거
        for ref in soup.find_all(["ref-list", "table-wrap", "table", "fig", "disp-formula", "ack", "fn-group"]):
            ref.decompose()
            
        sections = []
        
        # 초록(Abstract) 추출
        abstract = soup.find("abstract")
        if abstract:
            sections.append("=== Abstract ===")
            sections.append(abstract.get_text(separator="\n", strip=True))
        
        # 본문(Body) 추출
        body = soup.find("body")
        if body:
            sections.append("\n=== Main Text ===")
            for sec in body.find_all("sec", recursive=False):
                title = sec.find("title")
                if title:
                    sections.append(f"\n## {title.get_text(strip=True)}")
                    title.decompose()
                sections.append(sec.get_text(separator="\n", strip=True))
                
        extracted_text = "\n".join(sections)
        logger.info(f"  [XML] JATS 구조 텍스트 추출 완료 → {len(extracted_text):,}글자")
        return extracted_text

    def _extract_text_from_pdf(self, pdf_path: Path) -> str:
        logger.info(f"  [LlamaParse] 텍스트 마크다운 추출 시작: {pdf_path.name}")
        try:
            parser = LlamaParse(result_type="markdown")
            
            # 파일 크기 검사 (20MB 기준)
            file_size_mb = pdf_path.stat().st_size / (1024 * 1024)
            if file_size_mb > 20.0:
                logger.warning(f"  [LlamaParse] 대용량 PDF 감지 ({file_size_mb:.1f}MB). 50장 단위 분할 파싱을 시도합니다.")
                return self._extract_large_pdf(pdf_path, parser)
            
            # 일반 파싱
            documents = parser.load_data(str(pdf_path))
            
            all_text = []
            for doc in documents:
                if doc.text and doc.text.strip():
                    all_text.append(doc.text)
                    
            combined_text = "\n".join(all_text)
            logger.info(f"  [LlamaParse] 추출 완료 → {len(combined_text):,}글자")
            return combined_text
        except Exception as e:
            raise ValueError(f"LlamaParse PDF 텍스트 추출 실패: {e} → {pdf_path}")

    def _extract_large_pdf(self, pdf_path: Path, parser: LlamaParse) -> str:
        import PyPDF2
        combined_markdown = []
        
        try:
            with open(pdf_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                total_pages = len(reader.pages)
                chunk_size = 50
                
                logger.info(f"  [LlamaParse] 총 {total_pages}페이지. {chunk_size}쪽씩 전송합니다.")
                
                for i in range(0, total_pages, chunk_size):
                    chunk_end = min(i + chunk_size, total_pages)
                    logger.info(f"    - 파싱 중: p.{i+1} ~ p.{chunk_end}")
                    
                    writer = PyPDF2.PdfWriter()
                    for j in range(i, chunk_end):
                        writer.add_page(reader.pages[j])
                        
                    temp_chunk_path = pdf_path.with_name(f"temp_chunk_{i}.pdf")
                    with open(temp_chunk_path, "wb") as chunk_out:
                        writer.write(chunk_out)
                        
                    # LlamaParse에 청크 전송
                    documents = parser.load_data(str(temp_chunk_path))
                    for doc in documents:
                        if doc.text and doc.text.strip():
                            combined_markdown.append(doc.text)
                            
                    # 임시 파일 삭제
                    if temp_chunk_path.exists():
                        temp_chunk_path.unlink()
                        
            final_text = "\n".join(combined_markdown)
            logger.info(f"  [LlamaParse] 대용량 추출 완료 → {len(final_text):,}글자")
            return final_text
            
        except Exception as e:
            raise ValueError(f"대용량 PDF 분할 추출 중 오류 발생: {e}")

    # ================================================================
    # Phase 3: 텍스트 파싱 및 노이즈 제거 (가공 없이 보존)
    # ================================================================
    def clean_and_parse_text(self) -> str:
        if not self.raw_text:
            raise ValueError("먼저 fetch_data()를 실행하세요.")

        # 1. 문서 앞뒤 쓰레기(페이지 번호 등) 최소한으로 컷
        cleaned_lines = self._remove_noise(self.raw_text)
        cleaned_markdown = "\n".join(cleaned_lines)
        
        # 2. Raw 마크다운 파일로 저장
        safe = re.sub(r'[^\w가-힣\-]', '_', self.source.title[:50])
        output_filename = f"{safe}.md"
        path = RAW_DIR / output_filename
        with open(path, "w", encoding="utf-8") as f:
            f.write(cleaned_markdown)
            
        logger.info(f"  [Parse] 파이썬 파싱 완료 및 Raw 마크다운 저장: {path.name} ({path.stat().st_size:,} bytes)")
        
        # main.py에서 step3_llm_filter.py(process_with_llm)에 넘길 metadata 조립용
        self.parsed_data = {
            "metadata": {
                "source_name": self.source.source_name,
                "source_url": self.source.url,
                "title": self.source.title,
                "original_language": self.source.language,
                "doi": self.source.extra.get("doi") if self.source.extra else None,
                "crawled_at": datetime.now(KST).isoformat(),
                "extra": self.source.extra or {},
            }
        }
        
        self.raw_text = cleaned_markdown
        return str(path)

    def _remove_noise(self, text: str) -> list:
        cleaned, noise_count = [], 0
        for line in text.splitlines():
            s = line.strip()
            if not s:
                continue
            is_noise = False
            for name, pattern in NOISE_PATTERNS.items():
                if name in ("copyright", "pdf_artifact"):
                    if pattern.search(s):
                        is_noise = True
                        break
                else:
                    if pattern.match(s):
                        is_noise = True
                        break
            if is_noise:
                noise_count += 1
            else:
                cleaned.append(s)
        logger.info(f"  [Noise] {noise_count}줄 제거 → {len(cleaned)}줄 유지")
        return cleaned
