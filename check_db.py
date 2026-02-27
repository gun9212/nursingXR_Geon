import os
import json
from pathlib import Path
import faiss

def inspect_faiss_db(keyword: str):
    print("=" * 60)
    print(f" 📂 FAISS DB 내부 데이터 강제 조회 (키워드: {keyword})")
    print("=" * 60)
    
    from urllib.parse import quote
    
    # 경로 설정
    base_dir = Path("db/faiss")
    safe_keyword = quote(keyword.replace(" ", "_"))
    index_path = base_dir / "indexes" / f"index_{safe_keyword}.bin"
    meta_path = base_dir / "metadata" / f"meta_{safe_keyword}.json"
    
    if not index_path.exists() or not meta_path.exists():
        print(f"❌ '{keyword}' 키워드에 해당하는 DB 파일을 찾을 수 없습니다.")
        return
        
    # 1. FAISS .bin 파일 로드 (벡터 개수 확인)
    index = faiss.read_index(str(index_path))
    total_vectors = index.ntotal
    print(f"\n[1] Vector DB (.bin 파일 상태)")
    print(f"   ▶ 로드된 인덱스 경로: {index_path.name}")
    print(f"   ▶ 저장된 총 벡터(청크/QA) 개수: {total_vectors}개")
    print(f"   ▶ 임베딩 차원 수: {index.d}차원 (text-embedding-3-small)\n")
    
    # 2. JSON 파일 로드 (실제 텍스트 확인)
    with open(meta_path, "r", encoding="utf-8") as f:
        meta_data = json.load(f)
        
    print(f"[2] 매핑된 실제 텍스트 데이터 (.json 파일 상태)")
    print(f"   ▶ 로드된 메타데이터 경로: {meta_path.name}")
    print(f"   ▶ 메타데이터 항목 수: {len(meta_data)}개 (벡터 수와 일치해야 함)")
    
    # 저장된 데이터 샘플 3개만 미리보기
    print(f"\n[👀 저장된 데이터 전체 목록 ({len(meta_data)}개)]")
    count = 0
    for vec_id, data in meta_data.items():
        print(f"✅ 벡터 ID: {vec_id}")
        print(f"   - 타입: {data.get('type')}")
        print(f"   - 출처: {data.get('source_name')} ({data.get('document_id')})")
        
        if data.get("type") == "qa":
            print(f"   - [Q] 질문: {data.get('question')}")
            print(f"   - [A] 답변: {data.get('answer')}")
        else:
            print(f"   - [본문내용]: {data.get('content')[:100]}...")
        count += 1
        
    print("-" * 50)
    print("\n전체 내용이 궁금하시다면 아래 경로의 JSON 파일을 텍스트 에디터로 직접 여시면 됩니다.")
    print(f" 👉 {meta_path.resolve()}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", type=str, required=True, help="조회할 키워드 (예: 욕창)")
    args = parser.parse_args()
    
    inspect_faiss_db(args.keyword)
