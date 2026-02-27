"""
=============================================================================
Phase 2: Step 3 - LLM Based Chunking & QA Dataset Generation
=============================================================================
목적:
    output/raw/ 에 저장된 순수 마크다운 원본 텍스트를 읽어들여,
    GPT-4o (Structured Outputs)를 통해 `schema.json` 기반의 
    Flat Chunk(의미 단위 문단) 구조 및 가상의 QA 쌍 데이터를 추출한다.
    결과물은 output/processed/ 에 JSON 파일로 저장한다.
=============================================================================
"""

import os
import json
import logging
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv

try:
    from openai import OpenAI
except ImportError:
    print("[오류] pip install openai pydantic")
    raise

# ============================================================================
# 로깅 및 경로 설정
# ============================================================================
logger = logging.getLogger("Step3_LLM")
PROJECT_ROOT = Path(__file__).resolve().parent
PROCESSED_DIR = PROJECT_ROOT / "output" / "processed"

# ============================================================================
# Pydantic 모델 정의 (schema.json 완벽 매핑용)
# * OpenAI Structured Outputs 기능은 strict=True 구조를 요구하므로 Pydantic 활용
# ============================================================================
# QAPair removed for Phase 5

class Chunk(BaseModel):
    chunk_id: str = Field(description="이 문서 덩어리의 고유 식별자 ID")
    search_keywords: List[str] = Field(description="이 문서 블록을 하이브리드 검색으로 찾을 수 있도록 돕는 3~5개의 핵심 질환명 또는 고유 명사 키워드 (예: ['치매', '배회장애', '환경설정'])")
    raw_text: str = Field(description="잘라진 문서 블록의 파싱된 마크다운 원문 100% (절대 요약하거나 단어를 누락하지 마세요)")
    selection_reason_ko: str = Field(description="이 문서 블록 안에 요양 돌봄이나 실무 팁이 있다면, 어떤 이유로 이 문서를 선택했는지와 핵심 정보를 한국어로 3줄 요약 (벡터 임베딩용). 팁이 없다면 빈 문자열")

class NursingDataModel(BaseModel):
    document_id: str = Field(description="문서 전체에 부여되는 고유 식별 코드 (예: DOC_001)")
    document_summary: str = Field(description="이 문서 전체가 다루는 핵심 주제 3줄 요약")
    chunks: List[Chunk] = Field(description="의미 단위로 쪼개진 Flat Chunk 객체들의 배열")

# ============================================================================
# 핵심 처리 함수
# ============================================================================
def process_with_llm(raw_md_path: str, source_metadata: dict) -> Optional[Path]:
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("  [LLM Error] OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
        return None
        
    client = OpenAI(api_key=api_key)
    
    with open(raw_md_path, "r", encoding="utf-8") as f:
        content = f.read()

    logger.info(f"  [Step3] GPT-4o Semantic Chunking & QA 추출 시작: {Path(raw_md_path).name}")

    # ========================================================================
    # Pre-chunking (사전 분할) : 최대 8000 글자 단위로 물리적 분할
    # ========================================================================
    MAX_CHARS_PER_BLOCK = 8000
    blocks = [content[i:i+MAX_CHARS_PER_BLOCK] for i in range(0, len(content), MAX_CHARS_PER_BLOCK)]
    logger.info(f"  [Step3] 문서 크기: {len(content)}자. {len(blocks)}개 블록으로 분할(Pre-chunking)하여 LLM 처리")

    sys_prompt = """You are an elite Nursing & Caregiving Dataset Creator.
Your job is to read carefully and extract the core value of medical/nursing text while PRESERVING THE RAW TEXT 100%.

[STRICT INSTRUCTIONS]
1. DO NOT summarize or truncate the `raw_text`. You MUST copy the exact original text provided to you into `raw_text`.
2. Extract 3 to 5 core `search_keywords` (e.g., disease names, symptoms) for accurate keyword search.
3. Determine if the text has actionable nursing tips. If yes, write a 3-sentence Korean summary explaining why it is useful in `selection_reason_ko`. If it's pure statistics/unrelated, leave `selection_reason_ko` empty.
4. If the text is very long, break it into smaller semantic `chunks`, doing steps 1-3 for each chunk.
"""

    all_chunks_dumped = []
    doc_summary = "복합 문서 요약"
    doc_id = ""

    for idx, block_text in enumerate(blocks):
        try:
            logger.info(f"    - 블록 {idx+1}/{len(blocks)} LLM 처리 중...")
            completion = client.beta.chat.completions.parse(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": block_text}
                ],
                response_format=NursingDataModel,
                temperature=0.0
            )
            
            parsed_result = completion.choices[0].message.parsed
            if parsed_result:
                if not doc_id: doc_id = parsed_result.document_id
                if idx == 0: doc_summary = parsed_result.document_summary
                
                for chunk in parsed_result.chunks:
                    # 빈 이유(쓸모없는 데이터)는 제외 (필터링)
                    if chunk.selection_reason_ko.strip() != "":
                        all_chunks_dumped.append(chunk.model_dump())
                        
        except Exception as e:
            logger.error(f"    ⚠️ 블록 {idx+1} 처리 실패: {e}. (건너뜀)")
            continue

    if not all_chunks_dumped:
        logger.warning(f"  [Step3] ⚠️ 유효한 청크(Actionable Tip)가 없어 JSON을 생성하지 않습니다: {Path(raw_md_path).name}")
        return None

    # 스키마(schema.json) 포맷에 맞춰 메타데이터 병합
    final_data = {
        "document_id": doc_id,
        "source_metadata": source_metadata,
        "document_summary": doc_summary,
        "chunks": all_chunks_dumped
    }
    
    # 저장 전 Processed 폴더 확보
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    
    # 최종 JSON 저장
    safe_name = Path(raw_md_path).stem + ".json"
    out_path = PROCESSED_DIR / safe_name
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
        
    logger.info(f"  [Step3] 성공 — QA Chunk JSON 생성 및 병합 완료 : {out_path.name}")
    return out_path

if __name__ == "__main__":
    pass
