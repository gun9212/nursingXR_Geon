"""
=============================================================================
main.py — 키워드 기반 자동 탐색·크롤링·파싱 파이프라인 CLI
=============================================================================
사용법:
    python main.py --keyword "치매"
    python main.py --keyword "고혈압" --max-sources 10

동작 흐름:
    0. step4_vector_db.py → FAISS Vector DB 검색 (Cache Hit 시 즉시 반환)
    1. discover.py → 키워드로 소스 자동 탐색 (Cache Miss 시 가동)
    2. step2_crawler_parser.py → 소스 다운로드 → 파싱 → 마크다운 저장
    3. step3_llm_filter.py → GPT-4o로 Semantic Chunking & QA 추출 (JSON 저장)
    4. step4_vector_db.py → 결과 JSON을 FAISS DB에 실시간 Upsert

필수 패키지:
    pip install requests beautifulsoup4 lxml jsonschema PyMuPDF
=============================================================================
"""

import argparse
import logging

from step1_discovery import discover
from step2_crawler_parser import CrawlPipeline
from step3_llm_filter import process_with_llm
from step4_vector_db import VectorDBManager

# ============================================================================
# 로깅 설정
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Main")


def run_pipeline(keyword: str, max_sources: int = 5):
    """키워드 → 탐색 → 크롤링 → 파싱 → JSON 저장"""

    print()
    print("=" * 60)
    print(f" 🔍 지능형 RAG 파이프라인 가동")
    print(f"    키워드(Namespace): {keyword}")
    print(f"    소스당 최대 탐색: {max_sources}개")
    print("=" * 60)
    print()

    # ========================================================================
    # Step 0: FAISS Vector DB 우선 검색 (Cache Hit 판별)
    # ========================================================================
    print(" [Step 0] Vector DB 기존 지식 검색 중...")
    db_manager = VectorDBManager(keyword=keyword)
    
    # 임의의 대표 쿼리로 해당 Namespace에 유효한 QA 데이터가 있는지 확인
    test_query = f"{keyword}에 대해 요양보호사나 가족이 알아야 할 핵심 대처 방법"
    search_results = db_manager.search(test_query, top_k=3)
    
    if len(search_results) >= 2:
        print("\n ✅ [Cache Hit] Vector DB에 이미 충분한 지식이 학습되어 있습니다!")
        print(f"    - 현재 '{keyword}' 인덱스 크기: {db_manager.get_document_count()} 청크")
        print("\n 💡 [검색된 핵심 팁 미리보기]")
        for i, res in enumerate(search_results, 1):
            meta = res['meta']
            score = res['score']
            if meta.get("type") == "qa":
                print(f"    {i}. (유사도 {score:.3f}) Q: {meta.get('question')}")
                print(f"       A: {meta.get('answer')[:80]}...")
            else:
                print(f"    {i}. (유사도 {score:.3f}) 본문: {meta.get('content')[:100]}...")
                
        print("\n 🎯 추가 크롤링 없이 즉시 응답을 반환하고 종료합니다.\n")
        return

    print("\n ⚠️ [Cache Miss] DB에 연관 지식이 부족합니다. 백그라운드 크롤링을 가동합니다.")

    # ========================================================================
    # Step 1: Source Discovery (Cache Miss 상태)
    # ========================================================================
    sources = discover(keyword, max_results=max_sources)

    if not sources:
        print("\n ⚠ 발견된 소스가 없습니다.")
        return

    # Phase 2-4: 각 소스에 대해 파이프라인 실행
    success_count = 0
    error_count = 0
    saved_files = []

    for i, source in enumerate(sources, 1):
        print()
        print(f"─── [{i}/{len(sources)}] {source.title[:60]} ───")
        logger.info(f"[{i}/{len(sources)}] 파이프라인 시작: {source.source_name}")

        pipeline = CrawlPipeline(source=source, keyword=keyword)

        try:
            # Fetch
            pipeline.fetch_data()

            # Parse & Save Raw
            raw_markdown_path = pipeline.clean_and_parse_text()
            
            if raw_markdown_path:
                # Step 3: LLM Filter & QA Extraction
                processed_json_path = process_with_llm(
                    raw_md_path=raw_markdown_path, 
                    source_metadata=pipeline.parsed_data["metadata"]
                )
                
                if processed_json_path:
                    success_count += 1
                    saved_files.append(processed_json_path)
                else:
                    error_count += 1
                    logger.warning(f"  ❌ LLM 처리 실패 — 스킵")

        except (ConnectionError, ValueError) as e:
            error_count += 1
            logger.error(f"  ❌ 시스템 에러: {e}")

    # ========================================================================
    # Step 4: Vector DB Upsert (새로 생산된 JSON 학습)
    # ========================================================================
    new_vectors = 0
    if saved_files:
        print()
        print(" [Step 4] 새로 수집된 데이터를 Vector DB에 학습(Upsert)합니다...")
        new_vectors = db_manager.upsert_json_data(saved_files)

    # 최종 결과
    print()
    print("=" * 60)
    print(f" 📊 파이프라인 완료")
    print(f"    키워드(Namespace): {keyword}")
    print(f"    탐색: {len(sources)}개 소스 대상")
    print(f"    ✅ 성공 파싱/생성 JSON: {success_count}개")
    print(f"    ⚠️ 파싱/네트워크 오류: {error_count}개")
    print(f"    🧠 인덱스 신규 추가 벡터: {new_vectors}개 (총 {db_manager.get_document_count()}개 누적)")
    print("=" * 60)

    if saved_files:
        print()
        print(" 📁 최종 저장된 결과물 (output/processed/):")
        for f in saved_files:
            print(f"    → {f.name}")
    print()


# ============================================================================
# CLI 진입점
# ============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="키워드 기반 요양보호 데이터 자동 크롤링·파싱 파이프라인"
    )
    parser.add_argument(
        "--keyword", "-k",
        type=str,
        required=True,
        help="검색 키워드 (예: 치매, 고혈압, 당뇨)"
    )
    parser.add_argument(
        "--max-sources", "-m",
        type=int,
        default=5,
        help="소스당 최대 결과 수 (기본: 5)"
    )

    args = parser.parse_args()
    run_pipeline(keyword=args.keyword, max_sources=args.max_sources)
