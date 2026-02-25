# 🧠 NID Auto Parser Pipeline 

이 프로젝트는 **키워드 하나로 중앙치매센터 가이드북과 유럽 글로벌 논문(PMC)을 동시에 자동 탐색하여, 인공지능이 간병 꿀팁(실무 행동 지침)만 쪽집게처럼 뽑아 번역해 주는 데이터 파이프라인**입니다.

---

## 🏗 아키텍처 및 프로세스 (How it Works)

자동 파이프라인은 아래 **5단계(Phase)**를 순차적으로 거치며 동작합니다.

### 1️⃣ Source Discovery (`discover.py`)
- CLI에서 입력받은 키워드(예: "치매")를 **영문(dementia)**으로 자동 번역합니다.
- **Europe PMC API**: 오픈액세스(무료) 글로벌 기초/임상 논문 PDF 다운로드 URL을 수백 개 찾아옵니다. (검색어에 간병(caregiver) 관련어 강제 결합)
- **NID 웹 크롤링**: 국내 중앙치매센터 자료실을 뒤져 치매 관련 가이드북 한국어 PDF URL을 찾아옵니다.

### 2️⃣ Download & Extract (`step1_nid_crawler.py`)
- Python `requests`를 사용해 찾은 논문/가이드북을 로컬 `pdf/` 폴더에 동적으로 다운로드합니다.
- 강력한 텍스트 추출 라이브러리인 **PyMuPDF (`fitz`)**를 사용해 PDF 내부의 순수 텍스트(5~8만 글자)만 싹 긁어 모읍니다.

### 3️⃣ Regex Cleaning & Tree Parsing (`step1_nid_crawler.py`)
- 미리 정의된 **정규표현식(Regex)** 엔진이 페이지 번호, 저작권, 표/그림 번호 등 쓰레기 문자열들을 날려버립니다.
- ✨ **비용/효율 최적화**: 논문을 위에서부터 읽어 내려가다 "References(참고문헌)" 챕터에 도달하면 즉시 텍스트 수집을 종료하여 뒤쪽의 엄청난 용량을 버립니다.
- 남은 "순수 본문 고기"를 다시 훑으며 Section(절) ➔ Chapter(장) ➔ Item(단락) 트리 구조로 예쁘게 조립합니다.

### 4️⃣ LLM Content Evaluation & Translation (`llm_filter.py`)
- 깨끗해진 본문 통째를 OpenAI API (`gpt-4o-mini`)에 던집니다. (이때 프롬프트의 창의성은 껐습니다. `temperature=0.1`)
- **[A. 수문장 역할]**: "이 논문에 간병인을 위한 실질적 대처 방법(목욕법, 배회 방지법, 소통법)이 1줄이라도 있는가?" ➔ 딱딱한 통계 논문이어도 1줄이라도 있으면 PASS, 아예 없으면 FAIL!
- **[B. 발췌 및 통번역]**: 본문에서 그 행동 요령에 해당하는 원래 영어 문장을 뽑아내고(`original_english_text`), 한국 요양보호사가 바로 이해할 수 있도록 한국어 가이드라인(`translated_korean_guideline`)으로 번역합니다.

### 5️⃣ Validation & Export (`step1_nid_crawler.py` & `schema.json`)
- 생성된 데이터가 프로젝트의 뼈대인 `schema.json` 규격에 정확히 맞는지 `jsonschema`를 통해 최종 검사합니다.
- 합격한 결과물들만 `output/파일명.json` 이라는 완성된 하나의 지식 데이터로 저장합니다.

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
최상단 디렉토리에 `.env` 파일을 만들고 OpenAI API 키를 넣어야 합니다. (보안상 GitHub에 절대 올리지 마세요!)
```env
OPENAI_API_KEY="sk-proj-본인의_API_키를_여기에_붙여넣으세요"
```

### 4. 파이프라인 실행
명령어 한 줄이면 알아서 탐색 ➔ 다운로드 ➔ 정제 ➔ 추론 ➔ JSON 변환까지 자동으로 척척 수행합니다.

```bash
# 기본 실행 (치매 관련 5개 논문 처리)
python main.py --keyword "치매" --max-sources 5

# 키워드 변경
python main.py --keyword "고혈압" --max-sources 3
```

---

## 📂 파일 구조 설명

- **`main.py`**: CLI 진입점. (여기서 명령어 키워드를 받습니다.)
- **`discover.py`**: [Phase 1] PMC 및 NID에서 PDF 다운로드 링크 헌팅 엔진.
- **`step1_nid_crawler.py`**: [Phase 2, 3, 5] 다운로드, 구조화 파싱, 최종 JSON 검증 엔진을 모두 담당하는 파이프라인 뼈대.
- **`llm_filter.py`**: [Phase 4] OpenAI API를 이용한 본문 PASS/FAIL 분류 및 한영 발췌·요약 엔진.
- **`schema.json`**: 출력될 데이터의 JSON 형태를 강제하는 구조체 파일. (이 규격 통과 못하면 파일 안 만듦)
- **`output/`**: (gitignore됨) 성공적으로 파싱+번역된 최종 지식 JSON 파일들이 떨어지는 곳.
- **`pdf/`**: (gitignore됨) 임시로 다운로드 받은 PDF 원본 보관소.

---
**Note:** `.gitignore`에 의해 덩치가 큰 `pdf/` 파일들과 핵심 자산인 `output/` 결과 지식, 그리고 보안키가 든 `.env`는 깃허브에 절대 업로드되지 않습니다. 직접 실행해서 돌려보세요!
