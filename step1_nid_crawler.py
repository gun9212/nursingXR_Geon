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
import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, List

# --- 외부 라이브러리 ---
# pip install requests beautifulsoup4 lxml jsonschema PyMuPDF
import requests
from bs4 import BeautifulSoup

try:
    import fitz  # PyMuPDF
except ImportError:
    print("[오류] pip install PyMuPDF")
    raise

from jsonschema import validate, ValidationError
from discover import DiscoveredSource

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
PDF_DIR = PROJECT_ROOT / "pdf"
KST = timezone(timedelta(hours=9))

# ============================================================================
# Regex 패턴 (Crawling_Plan.md Phase 3.2.1)
# ============================================================================
HEADING_PATTERNS = {
    # === 한글 교과서 ===
    "topic": re.compile(r"^제?\s*\d+편\s+(.+)$|^Part\s+\d+[:\s]+(.+)$"),
    "section": re.compile(r"^제?\s*(\d+)절\s+(.+)$|^(\d+)절\s+(.+)$"),
    "chapter": re.compile(r"^(\d+)\.\s+([A-Z가-힣].+)$"),
    "item": re.compile(r"^([가-힣])\.\s+(.+)$"),
    "sub_item_numbered": re.compile(r"^(\d+)\)\s+(.+)$"),

    # === 한글 학술논문 (로마 숫자) ===
    "section_roman_ko": re.compile(r"^([ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]+)\.\s+(.+)$"),

    # === 영문 학술논문 ===
    "section_en": re.compile(
        r"^(\d+)\.\s+(Introduction|Background|Methods?|Materials?"
        r"|Results?|Discussion|Conclusions?|Limitations?"
        r"|Implications|Recommendations|References"
        r"|Literature\s+Review|Theoretical\s+Framework"
        r"|Data\s+Analysis|Findings|Study\s+Design"
        r"|Ethical\s+Considerations|Acknowledgments?)\s*$",
        re.IGNORECASE
    ),
    "sub_section_en": re.compile(r"^(\d+)\.(\d+)\s+(.+)$"),
    "item_alpha": re.compile(r"^([A-Z])\.\s+(.+)$"),
}

BULLET_PATTERN = re.compile(r"^[•·]\s*(.+)$")

# PDF 노이즈 패턴 (Phase 3.1.1 강화)
NOISE_PATTERNS = {
    "page_number": re.compile(
        r"^[-─—]\s*\d+\s*[-─—]$|"
        r"^p\.?\s*\d+$|"
        r"^page\s*\d+$|"
        r"^\d{1,4}\s*$",
        re.IGNORECASE
    ),
    "page_header": re.compile(
        r"^\d{2,4}\s*\d*\s*(장|부|편)[_\s].+$"
    ),
    "footer_repeated": re.compile(
        r"^(보건복지부|국민건강보험공단|요양보호사\s*양성\s*표준교재|중앙치매센터)\s*$"
    ),
    "copyright": re.compile(
        r"copyright|ⓒ|©|all\s+rights\s+reserved|공공누리",
        re.IGNORECASE
    ),
    "figure_ref": re.compile(
        r"^\[.+\]\s*$|^<.+>\s*$|^\(그림\s*\d*\)|^\(표\s*\d*\)"
    ),
    "pdf_artifact": re.compile(
        r"^[\x00-\x1f]+$|"
        r"^\s*[·•\-]{3,}\s*$|"
        r"^\s{20,}$"
    ),
}


# ============================================================================
# 데이터 클래스
# ============================================================================
@dataclass
class SubItemData:
    sub_title: str
    sub_description: str

@dataclass
class ItemData:
    item_title: str
    description: str
    sub_items: List[SubItemData] = field(default_factory=list)
    tags: list = field(default_factory=list)

@dataclass
class ChapterData:
    chapter_title: str
    chapter_number: int
    items: list = field(default_factory=list)

@dataclass
class SectionData:
    section_title: str
    section_number: int
    chapters: list = field(default_factory=list)

@dataclass
class DocumentData:
    topic: str
    keyword: str
    sections: list = field(default_factory=list)
    source_name: str = ""
    source_url: str = ""
    learning_objectives: list = field(default_factory=list)
    extracted_korean_guidelines: list = field(default_factory=list)


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
        self.schema = self._load_schema()
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        PDF_DIR.mkdir(parents=True, exist_ok=True)

        logger.info(f"  파이프라인 초기화: [{source.format.upper()}] {source.title[:60]}")

    def _load_schema(self) -> dict:
        if not SCHEMA_PATH.exists():
            raise FileNotFoundError(f"Schema 없음: {SCHEMA_PATH}")
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    # ================================================================
    # Phase 2: 데이터 수집 — PDF/HTML 자동 분기
    # ================================================================
    def fetch_data(self) -> str:
        url = self.source.url
        logger.info(f"  [Fetch] 다운로드: {url[:80]}")

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ko-KR,ko;q=0.9",
        }

        if self.source.format == "pdf":
            self.raw_text = self._fetch_pdf(url, headers)
        else:
            self.raw_text = self._fetch_html(url, headers)

        logger.info(f"  [Fetch] 완료: {len(self.raw_text):,}글자, "
                    f"{len(self.raw_text.splitlines()):,}라인")
        return self.raw_text

    def _fetch_pdf(self, url: str, headers: dict) -> str:
        headers["Accept"] = "application/pdf,*/*"
        try:
            resp = requests.get(url, headers=headers, timeout=30, stream=True)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise ConnectionError(f"PDF 다운로드 실패: {e} → {url}")

        # 파일명 결정
        safe_name = re.sub(r'[^\w\-.]', '_', self.source.title[:40]) + ".pdf"
        pdf_path = PDF_DIR / safe_name

        with open(pdf_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"  [PDF] 저장: {pdf_path.name} ({pdf_path.stat().st_size:,} bytes)")

        return self._extract_text_from_pdf(pdf_path)

    def _fetch_html(self, url: str, headers: dict) -> str:
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            resp.encoding = "utf-8"
        except requests.RequestException as e:
            raise ConnectionError(f"HTML 크롤링 실패: {e} → {url}")

        soup = BeautifulSoup(resp.text, "lxml")
        for tag in soup.find_all(["script", "style", "nav", "aside", "footer"]):
            tag.decompose()
        content = soup.find("div", class_="content") or soup.find("main") or soup.body
        return content.get_text(separator="\n", strip=True) if content else ""

    def _extract_text_from_pdf(self, pdf_path: Path) -> str:
        logger.info(f"  [PyMuPDF] 텍스트 추출: {pdf_path.name}")
        all_text = []
        try:
            doc = fitz.open(str(pdf_path))
            total_pages = len(doc)
            for page_num in range(total_pages):
                page_text = doc[page_num].get_text("text")
                if page_text and page_text.strip():
                    all_text.append(page_text)
            doc.close()
            logger.info(f"  [PyMuPDF] {total_pages}페이지 → {sum(len(t) for t in all_text):,}글자")
        except Exception as e:
            raise ValueError(f"PDF 텍스트 추출 실패: {e} → {pdf_path}")
        return "\n".join(all_text)

    # ================================================================
    # Phase 3: 텍스트 파싱 및 트리 구조화
    # ================================================================
    def clean_and_parse_text(self) -> dict:
        if not self.raw_text:
            raise ValueError("먼저 fetch_data()를 실행하세요.")

        lines = self._remove_noise(self.raw_text)
        document = self._build_tree_structure(lines)
        self.parsed_data = self._serialize_to_json(document)

        secs = self.parsed_data.get("sections", [])
        chs = sum(len(s.get("chapters", [])) for s in secs)
        its = sum(len(c.get("items", [])) for s in secs for c in s.get("chapters", []))
        sis = sum(len(i.get("sub_items", [])) for s in secs
                  for c in s.get("chapters", []) for i in c.get("items", []))
        logger.info(f"  [Parse] Sec:{len(secs)} Ch:{chs} It:{its} Sub:{sis}")
        return self.parsed_data

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
                continue
            cleaned.append(s)
        logger.info(f"  [Noise] {noise_count}줄 제거 → {len(cleaned)}줄 유지")
        return cleaned

    def _build_tree_structure(self, lines: list) -> DocumentData:
        doc = DocumentData(
            topic="", keyword=self.keyword,
            source_name=self.source.source_name,
            source_url=self.source.url,
        )

        current_section = None
        current_chapter = None
        current_item = None
        current_sub_title = None
        desc_buffer = []
        sub_desc_buffer = []
        learning_obj_mode = False

        for line in lines:
            # 학습목표
            if line == "학습목표":
                learning_obj_mode = True
                continue
            if learning_obj_mode:
                bullet = BULLET_PATTERN.match(line)
                if bullet:
                    doc.learning_objectives.append(bullet.group(1))
                    continue
                else:
                    learning_obj_mode = False

            # Topic
            topic_m = HEADING_PATTERNS["topic"].match(line)
            if topic_m and not doc.topic:
                doc.topic = line
                continue
            if not doc.topic and len(line) < 30 and not any(
                p.match(line) for p in HEADING_PATTERNS.values()
            ):
                doc.topic = line
                continue

            # Section (절)
            sec_m = HEADING_PATTERNS["section"].match(line)
            if sec_m:
                self._flush_sub(current_item, current_sub_title, sub_desc_buffer)
                self._flush_desc(current_item, desc_buffer)
                sec_num = sec_m.group(1) or sec_m.group(3)
                current_section = SectionData(section_title=line, section_number=int(sec_num))
                doc.sections.append(current_section)
                current_chapter = None
                current_item = None
                current_sub_title = None
                continue

            # Section — 한글 논문 로마 숫자 (Ⅰ. 서론, Ⅱ. 연구방법)
            roman_m = HEADING_PATTERNS["section_roman_ko"].match(line)
            if roman_m:
                self._flush_sub(current_item, current_sub_title, sub_desc_buffer)
                self._flush_desc(current_item, desc_buffer)
                roman_num = "ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ".index(roman_m.group(1)[0]) + 1
                current_section = SectionData(section_title=line, section_number=roman_num)
                doc.sections.append(current_section)
                current_chapter = None
                current_item = None
                current_sub_title = None
                continue

            # Section — 영문 논문 (1. Introduction, 2. Methods)
            en_sec_m = HEADING_PATTERNS["section_en"].match(line)
            if en_sec_m:
                self._flush_sub(current_item, current_sub_title, sub_desc_buffer)
                self._flush_desc(current_item, desc_buffer)
                sec_num = int(en_sec_m.group(1))
                current_section = SectionData(section_title=line, section_number=sec_num)
                doc.sections.append(current_section)
                current_chapter = None
                current_item = None
                current_sub_title = None
                continue

            # Chapter (장) — 한글 교과서 "1. 제목" 또는 영문 하위 섹션 "2.1 Title"
            sub_sec_en = HEADING_PATTERNS["sub_section_en"].match(line)
            chap_m = HEADING_PATTERNS["chapter"].match(line)
            if sub_sec_en:
                self._flush_sub(current_item, current_sub_title, sub_desc_buffer)
                self._flush_desc(current_item, desc_buffer)
                chap_num = int(sub_sec_en.group(2))
                if current_section is None:
                    current_section = SectionData(section_title="1절 기본", section_number=1)
                    doc.sections.append(current_section)
                current_chapter = ChapterData(chapter_title=line, chapter_number=chap_num)
                current_section.chapters.append(current_chapter)
                current_item = None
                current_sub_title = None
                continue
            elif chap_m:
                self._flush_sub(current_item, current_sub_title, sub_desc_buffer)
                self._flush_desc(current_item, desc_buffer)
                chap_num = chap_m.group(1)
                if current_section is None:
                    current_section = SectionData(section_title="1절 기본", section_number=1)
                    doc.sections.append(current_section)
                current_chapter = ChapterData(chapter_title=line, chapter_number=int(chap_num))
                current_section.chapters.append(current_chapter)
                current_item = None
                current_sub_title = None
                continue

            # Item (가. 나. 다.) 또는 (A. B. C.)
            item_m = HEADING_PATTERNS["item"].match(line)
            alpha_m = HEADING_PATTERNS["item_alpha"].match(line)
            matched_item = item_m or alpha_m
            if matched_item:
                self._flush_sub(current_item, current_sub_title, sub_desc_buffer)
                self._flush_desc(current_item, desc_buffer)
                if current_chapter is None:
                    if current_section is None:
                        current_section = SectionData(section_title="1절 기본", section_number=1)
                        doc.sections.append(current_section)
                    current_chapter = ChapterData(chapter_title="1. 기본", chapter_number=1)
                    current_section.chapters.append(current_chapter)
                current_item = ItemData(item_title=line, description="")
                current_chapter.items.append(current_item)
                current_sub_title = None
                continue

            # Sub-item (1) 2) 3))
            sub_m = HEADING_PATTERNS["sub_item_numbered"].match(line)
            if sub_m and current_item is not None:
                self._flush_sub(current_item, current_sub_title, sub_desc_buffer)
                self._flush_desc(current_item, desc_buffer)
                current_sub_title = line
                continue

            # 영문 논문 References 방어 (본문 파싱 조기 종료)
            if current_chapter and re.match(r"^(References|Bibliography)\s*$", line, re.IGNORECASE):
                logger.info("  → References 섹션 감지. 파싱을 조기 종료합니다.")
                break

            # 일반 텍스트 및 불릿 처리
            # Item이 아직 없는데 텍스트가 나오면 가상의 기본 Item을 생성 (영문 논문 본문 보존용)
            if not current_item and not line.startswith("학습목표"):
                if current_chapter is None:
                    if current_section is None:
                        current_section = SectionData(section_title="1절 기본", section_number=1)
                        doc.sections.append(current_section)
                    current_chapter = ChapterData(chapter_title="1. 기본", chapter_number=1)
                    current_section.chapters.append(current_chapter)
                current_item = ItemData(item_title="본문", description="")
                current_chapter.items.append(current_item)

            # 불릿 (• ...)
            bullet = BULLET_PATTERN.match(line)
            if bullet:
                bullet_text = bullet.group(1)
                colon_split = re.split(r"[：:]", bullet_text, maxsplit=1)
                if len(colon_split) == 2 and len(colon_split[0].strip()) < 20:
                    self._flush_sub(current_item, current_sub_title, sub_desc_buffer)
                    self._flush_desc(current_item, desc_buffer)
                    current_sub_title = colon_split[0].strip()
                    sub_desc_buffer.append(colon_split[1].strip())
                elif current_sub_title is not None:
                    sub_desc_buffer.append(bullet_text)
                else:
                    desc_buffer.append(bullet_text)
                continue

            # 일반 텍스트
            if current_sub_title is not None:
                sub_desc_buffer.append(line)
            elif current_item is not None:
                desc_buffer.append(line)

        self._flush_sub(current_item, current_sub_title, sub_desc_buffer)
        self._flush_desc(current_item, desc_buffer)
        return doc

    def _flush_desc(self, item, buffer):
        if item is not None and buffer:
            text = "\n".join(buffer)
            item.description = (item.description + "\n" + text).strip() if item.description else text
        buffer.clear()

    def _flush_sub(self, item, sub_title, buffer):
        if item is not None and sub_title is not None and buffer:
            item.sub_items.append(SubItemData(
                sub_title=sub_title,
                sub_description="\n".join(buffer),
            ))
        buffer.clear()

    def _serialize_to_json(self, doc: DocumentData) -> dict:
        result = {
            "topic": doc.topic or self.keyword,
            "keyword": doc.keyword,
            "learning_objectives": doc.learning_objectives,
            "extracted_korean_guidelines": doc.extracted_korean_guidelines,
            "sections": [],
            "source_metadata": {
                "source_name": self.source.source_name,
                "source_url": self.source.url,
                "original_language": self.source.language,
                "license_type": self.source.license_type,
                "crawled_at": datetime.now(KST).isoformat(),
                "extra": self.source.extra or {},
            },
        }
        for sec in doc.sections:
            sec_d = {"section_title": sec.section_title, "section_number": sec.section_number, "chapters": []}
            for ch in sec.chapters:
                ch_d = {"chapter_title": ch.chapter_title, "chapter_number": ch.chapter_number, "items": []}
                for it in ch.items:
                    desc = it.description
                    if not desc.strip() and it.sub_items:
                        desc = " / ".join(si.sub_title for si in it.sub_items)
                    it_d = {"item_title": it.item_title, "description": desc}
                    if it.sub_items:
                        it_d["sub_items"] = [
                            {"sub_title": si.sub_title, "sub_description": si.sub_description}
                            for si in it.sub_items
                        ]
                    if it.tags:
                        it_d["tags"] = it.tags
                    ch_d["items"].append(it_d)
                sec_d["chapters"].append(ch_d)
            result["sections"].append(sec_d)
        return result

    # ================================================================
    # Phase 4: JSON 검증 및 저장
    # ================================================================
    def validate_and_save_json(self, output_filename: str = "") -> Optional[Path]:
        if self.parsed_data is None:
            raise ValueError("먼저 clean_and_parse_text()를 실행하세요.")

        # 스키마 검증
        try:
            validate(instance=self.parsed_data, schema=self.schema)
            logger.info("  [Validate] ✅ JSON Schema 통과")
        except ValidationError as e:
            logger.warning(f"  [Validate] ❌ 검증 실패: {e.message[:100]}")
            return None

        # 저장
        if not output_filename:
            safe = re.sub(r'[^\w가-힣\-]', '_', self.source.title[:50])
            output_filename = f"{safe}.json"
        path = OUTPUT_DIR / output_filename
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.parsed_data, f, ensure_ascii=False, indent=4)
        logger.info(f"  [Save] ✅ {path.name} ({path.stat().st_size:,} bytes)")
        return path
