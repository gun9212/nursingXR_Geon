# 🧠 NID Auto Parser Pipeline (V2: Flat Extract & Expanded Sources)

이 프로젝트는 **키워드 기반으로 다양한 출처(Europe PMC, PubMed, 위키피디아, 중앙치매센터)의 문헌과 가이드북을 자동 탐색하여, 원문을 완벽히 보존한 채 인공지능이 간병 꿀팁(실무 행동 지침)과 핵심 요약을 발췌해 주는 데이터 파이프라인**입니다. 

V2 업데이트를 통해 기존의 무거운 계층형(Tree) 파싱 구조를 버리고, 벡터 데이터베이스 관리 및 AI 파인튜닝에 적합한 가볍고 명확한 **Flat JSON 구조**로 개편되었습니다.

---

## 🏗 아키텍처 및 프로세스 (How it Works)

자동 파이프라인은 아래 **4단계(Phase)**를 순차적으로 거치며 동작합니다.

### 1️⃣ Phase 1. Source Discovery (`step1_discovery.py`)
**"원하는 주제의 문서 링크를 알아내는 단계"**

*   **탐색 소스 (Expanded):**
    1. **Europe PMC REST API**: 전 세계 Open Access 논문 PDF 수집. `dementia` 등 키워드 및 `caregiver OR nursing` 필터링 적용.
    2. **PubMed E-utilities API**: NCBI 펍메드에서 추가적인 무료 원문(Free Full Text) 링크 및 PDF 확보.
    3. **Wikipedia API**: 파이썬 `wikipedia` 패키지를 사용해 한국어 위키백과 직접 검색.
    4. **NID (중앙치매센터)**: 한국 치매 가이드북 PDF 링크 크롤링.
*   **작동 방식:** 사용자가 CLI에서 `--keyword "치매"`라고 입력하면, 파트너 소스들을 순회하며 처리 대상인 문서(DiscoveredSource) 리스트를 구축합니다.

### 2️⃣ Phase 2. Document Fetch & Minimal Clean (`step2_crawler_parser.py`)
**"지식을 다운로드하고 원문을 훼손 없이 안전하게 가져오는 단계"**

*   **다운로드/추출 방식:**
    1. **PDF 논문/가이드북**: LlamaParse를 통해 PDF의 복잡한 레이아웃을 순수 마크다운 텍스트로 고품질 변환합니다.
    2. **HTML/위키백과**: 위키백과 패키지 및 bs4를 통해 순수 텍스트 본문만 즉시 크롤링합니다.
*   **노이즈 제거 (원문 100% 보존 위주):** 
    과거의 공격적인 제거 방식(References 전체 삭제 등) 대신, 원문 분석의 무결성을 지키기 위해 오직 **페이지 번호, 저작권 문구, 머리말/꼬리말 워터마크** 등 명백한 노이즈만 최소한으로 제거합니다.
*   **백업:** 다운로드한 텍스트는 보존을 위해 `output/` 디렉토리에 `.txt` 또는 `.html` 로 원본이 백업됩니다.

### 3️⃣ Phase 3. LLM Relevance & Tip Extraction (`step3_llm_filter.py`)
**"본문을 읽고, 유효한 문서를 걸러낸 뒤 실용적인 팁만 뽑아내는 단계"**

*   **사용 API:** **OpenAI API (`gpt-4o-mini` 모델)** + JSON Mode
*   **작동 방식:** 추출된 원문 테스트 전체(`full_text`)를 프롬프트와 함께 전달하여 다음 업무를 수행합니다.
    1. **적격성 평가 (is_relevant):** 기초 수치 통계나 역학 연구가 아닌, "실제로 간병 가족이나 보호자가 따라 할 수 있는 구체적 행동 팁이 있는가?"를 검증하여 PASS/FAIL을 결정합니다.
    2. **핵심 요약 (document_summary):** 문서가 무슨 내용을 다루는지 한국어로 2~3줄 요약합니다.
    3. **행동 지침 발췌 (extracted_tips):** 본문 내에서 가장 실용적인 팁 문구를 3~5개 찾아 한국어로 번역/추출합니다. 

### 4️⃣ Phase 4. Vector DB Namespace & Intelligent Caching (`step4_vector_db.py`)
**"지식을 키워드별로 격리 보관하고, 즉시 꺼내어 똑똑하게 대답하는 단계"**

*   **FAISS Namespace (병실 격리):**
    수집된 JSON 지식들을 `index_치매.bin`, `index_욕창.bin` 처럼 무조건 키워드별로 쪼개어(Namespace) 저장합니다. 이를 통해 치매 질문에 욕창 지식이 섞여 나오는 치명적인 노이즈를 0%로 차단합니다.
*   **지능형 캐싱 라우팅 (Cache Hit / Miss):**
    사용자가 질문했을 때 무작정 크롤링을 도는 것이 아니라, 먼저 해당 키워드의 Vector DB 방을 두드려봅니다.
    1. **Cache Hit**: 내부에 이미 답변할 지식이 충분하다면(`ntotal > 0`), 크롤러를 스킵하고 0.1초 만에 DB에서 최고 품질의 팁을 꺼내옵니다.
    2. **Cache Miss**: 내부에 지식이 부족하다면, 백그라운드에서 크롤링 모듈(`step1`~`step3`)을 가동시켜 실시간으로 새로운 지식을 학습하고 DB에 채워 넣습니다.
*   **LLM 필터링 락(Lock):** `is_actionable_tip == True` 인 순도 100%의 실무 요양 팁만 DB에 들어가도록 `upsert_json_data` 함수에 강력한 필터락이 걸려있습니다.

### 🚀 Phase 5. 하이브리드 검색 및 원문 보존 스키마 (Deployed)
**"원문을 100% 보존하면서 메모리 무결성(OOM 방지)과 핀포인트 키워드 매칭(하이브리드) 검색까지 완벽 지원하는 최종 고도화 단계"**

*   **원문 100% 보존 전략:** QA 억지 생성을 최소화하고, 노이즈가 제거된 `raw_text` 전체를 그대로 유지하여 RAG 프롬프트에 문맥 유실(Context Loss) 없이 전달합니다.
*   **하이브리드 검색 (Hybrid Search):** LLM이 문서를 분석할 때 `["치매", "알츠하이머"]` 등 핵심 질환명과 메타 태그(`search_keywords`)를 명시적으로 뽑아내어 메타데이터에 박아둡니다.
*   **결과:** 사용자가 특정 질환명을 검색할 때, 의미를 찾는 벡터 검색(Vector Search)에 **정확한 키워드가 포함된 문서를 우선순위(가산점)로 끌어올리는 완벽한 하이브리드 매칭**이 가능해집니다. 이를 통해 AI의 정보 환각(Hallucination) 현상이 근본적으로 차단됩니다.

---

## 🛠 설치 및 사용 방법

### 1. Requirements
*   Python 3.10+
*   맥 또는 리눅스 환경 권장

### 2. 가상 환경 설정 및 패키지 설치
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. 환경 변수 설정
최상단 디렉토리에 `.env` 파일을 만들고 OpenAI API 키와 LlamaParse API 키를 넣어야 합니다.
```env
OPENAI_API_KEY="sk-proj-본인의_OPENAI_API_키"
LLAMA_CLOUD_API_KEY="llx-본인의_LLAMAPARSE_API_키"
```

### 4. 파이프라인 실행
명령어 한 줄이면 알아서 탐색 ➔ 수집 ➔ 정제 ➔ LLM 추론 ➔ JSON/TXT 저장 기능을 모두 수행합니다.

```bash
# 기본 실행 (치매 관련 5개 문서 처리)
python main.py --keyword "치매" --max-sources 5

# 키워드 변경 및 처리 개수 지정
python main.py --keyword "고혈압" --max-sources 1
```

---

## 📂 핵심 출력물 예시 (Flat JSON)
`output/` 폴더에 생성되는 최종 JSON 파일의 구조는 다음과 같습니다.
```json
{
  "document_id": "DOC_001",
  "source_metadata": {
    "source_name": "Europe PMC",
    "source_url": "...",
    "title": "논문 제목"
  },
  "document_summary": "이 연구는 알츠하이머 환자 보호자의 부담을 조사하였습니다...",
  "chunks": [
    {
      "chunk_id": "uuid-...",
      "search_keywords": ["치매 보호자", "우울증", "부담"],
      "selection_reason_ko": "치매 환자 보호자가 겪는 우울증의 주요 원인과 이를 완화하기 위한 사회적 지원의 필요성이 서술되어 있습니다.",
      "raw_text": "원본 마크다운 텍스트 100% 보존 블록..."
    }
  ]
}
```

## 📂 파일 구조 설명

- **`main.py`**: CLI 진입점 및 전체 파이프라인 조립체.
- **`step1_discovery.py`**: PMC, PubMed, NID, Wikipedia 등 소스 헌팅 로직.
- **`step2_crawler_parser.py`**: 텍스트 다운로드, LlamaParse 변환, 백업(.txt/.html) 생성 및 노이즈 필터 모듈.
- **`step3_llm_filter.py`**: OpenAI JSON Mode를 통한 PASS/FAIL 평가 및 요약/팁 추출 엔진.
- **`step4_vector_db.py`**: FAISS 로컬 벡터 인덱싱, Namespace 분리, Cache Hit/Miss 오케스트레이션 엔진.
- **`schema.json`**: 출력될 데이터의 JSON 형태를 강제하는 구조체 (Flat 버전).
- **`db/faiss/`**: Namespace별로 생성된 FAISS 인덱스(`.bin`)와 메타데이터(`.json`)가 저장되는 핵심 지식 저장소.
- **`output/`**: 파이프라인 처리 후 최종 JSON 및 원문 텍스트(.txt/.html) 백업 파일이 생성되는 곳.
- **`pdf/`**: 다운로드 받은 PDF 원본 임시 보관소.
