"""
=============================================================================
main.py — 키워드 기반 자동 탐색·크롤링·파싱 파이프라인 CLI
=============================================================================
사용법:
    python main.py --keyword "치매"
    python main.py --keyword "고혈압" --max-sources 10

동작 흐름:
    1. discover.py → 키워드로 소스 자동 탐색 (Europe PMC + NID)
    2. step1_nid_crawler.py → 각 소스를 다운로드 → 파싱 → JSON 저장
    3. output/ 디렉토리에 결과 JSON 파일 저장

필수 패키지:
    pip install requests beautifulsoup4 lxml jsonschema PyMuPDF
=============================================================================
"""

import argparse
import logging
import sys
from pathlib import Path

from discover import discover
from step1_nid_crawler import CrawlPipeline

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
    print(f" 🔍 키워드 기반 자동 파이프라인")
    print(f"    키워드: {keyword}")
    print(f"    소스당 최대: {max_sources}개")
    print("=" * 60)
    print()

    # Phase 1: Source Discovery
    sources = discover(keyword, max_results=max_sources)

    if not sources:
        print("\n ⚠ 발견된 소스가 없습니다.")
        return

    # Phase 2-4: 각 소스에 대해 파이프라인 실행
    success_count = 0
    fail_count = 0
    saved_files = []

    for i, source in enumerate(sources, 1):
        print()
        print(f"─── [{i}/{len(sources)}] {source.title[:60]} ───")
        logger.info(f"[{i}/{len(sources)}] 파이프라인 시작: {source.source_name}")

        pipeline = CrawlPipeline(source=source, keyword=keyword)

        try:
            # Fetch
            pipeline.fetch_data()

            # Parse
            parsed_data = pipeline.clean_and_parse_text()

            # LLM Filtering
            logger.info("  [Filter] LLM 적합성 필터링 평가 중...")
            from llm_filter import check_relevance
            eval_result = check_relevance(pipeline.raw_text)
            
            if not eval_result.get("is_relevant", True):
                fail_count += 1
                reason = eval_result.get("reason", "적합하지 않은 문서")
                logger.warning(f"  [Filter] ❌ 무효 문서: {reason}")
                continue
                
            logger.info(f"  [Filter] ✅ 유효 문서 (점수: {eval_result.get('confidence_score')})")
            parsed_data["llm_evaluation"] = eval_result

            # Validate & Save
            output_path = pipeline.validate_and_save_json()
            if output_path:
                success_count += 1
                saved_files.append(output_path)
            else:
                fail_count += 1
                logger.warning(f"  스키마 검증 실패 (빈 섹션 등) — 스킵")

        except (ConnectionError, ValueError) as e:
            fail_count += 1
            logger.error(f"  ❌ 실패: {e}")

    # 최종 결과
    print()
    print("=" * 60)
    print(f" 📊 파이프라인 완료")
    print(f"    키워드: {keyword}")
    print(f"    탐색: {len(sources)}개 소스")
    print(f"    성공: {success_count}개 ✅")
    print(f"    실패: {fail_count}개 ❌")
    print("=" * 60)

    if saved_files:
        print()
        print(" 📁 저장된 파일:")
        for f in saved_files:
            print(f"    → {f}")
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
