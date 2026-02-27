"""
=============================================================================
Phase 3: Step 4 - FAISS Vector DB Management & Namespace
=============================================================================
목적:
    output/processed/ 에 저장된 QA JSON 데이터를 파싱하여
    OpenAI text-embedding-3-small 모델로 임베딩한 후,
    FAISS 인덱스(.bin 파일)와 메타데이터 매핑(.json 파일)을 업데이트한다.

핵심 기능:
    - Namespace 분리: 키워드(질환명 등)별로 인덱스(`index_{keyword}.bin`)와 
      메타데이터(`meta_{keyword}.json`)를 분리 구축하여 검색 정확도를 높인다.
    - 검색 기능: 질문(Query)을 벡터화하여 가장 유사도가 높은 청크/QA를 반환.
=============================================================================
"""

import os
import json
import logging
import uuid
from pathlib import Path
from typing import List, Dict, Any

from dotenv import load_dotenv

try:
    import faiss
    import numpy as np
    from openai import OpenAI
except ImportError:
    print("[오류] pip install faiss-cpu numpy openai")
    raise

# ============================================================================
# 로깅 및 경로 설정
# ============================================================================
logger = logging.getLogger("Step4_FAISS")
PROJECT_ROOT = Path(__file__).resolve().parent
DB_DIR = PROJECT_ROOT / "db" / "faiss"
INDEX_DIR = DB_DIR / "indexes"
META_DIR = DB_DIR / "metadata"

INDEX_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

class VectorDBManager:
    def __init__(self, keyword: str):
        """키워드(Namespace)별로 DB 인스턴스 초기화"""
        load_dotenv()
        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
            
        self.client = OpenAI(api_key=self.api_key)
        self.keyword = keyword
        self.embedding_model = "text-embedding-3-small"
        self.dim = 1536 # text-embedding-3-small의 차원 수
        
        # Namespace File Paths
        from urllib.parse import quote
        safe_keyword = quote(keyword.replace(" ", "_"))
        self.index_path = INDEX_DIR / f"index_{safe_keyword}.bin"
        self.meta_path = META_DIR / f"meta_{safe_keyword}.json"
        
        # Load or Initialize FAISS & Meta
        self.index, self.metadata_store = self._load_or_init_db()

    def _load_or_init_db(self):
        """기존 DB 로드 또는 새로 생성"""
        # 1. 메타데이터 로드
        metadata_store = {}
        if self.meta_path.exists():
            try:
                with open(self.meta_path, "r", encoding="utf-8") as f:
                    metadata_store = json.load(f)
            except Exception as e:
                logger.warning(f"  [FAISS] 메타데이터 로드 실패, 새로 생성합니다: {e}")
                
        # 2. FAISS 인덱스 로드
        if self.index_path.exists():
            try:
                index = faiss.read_index(str(self.index_path))
                logger.info(f"  [FAISS] 기존 인덱스 로드 완료 (Namespace: {self.keyword}, 크기: {index.ntotal})")
                return index, metadata_store
            except Exception as e:
                logger.warning(f"  [FAISS] 인덱스 로드 실패, 새로 생성합니다: {e}")
                
        # 신규 생성
        logger.info(f"  [FAISS] 신규 인덱스 생성 (Namespace: {self.keyword})")
        # L2 거리 기반 플랫 인덱스 생성 (가장 정확한 완전탐색)
        index = faiss.IndexFlatL2(self.dim)
        
        # IndexIDMap을 사용하여 커스텀 정수 ID를 메타데이터 딕셔너리와 매핑할 수 있게 함
        index_id_map = faiss.IndexIDMap(index)
        return index_id_map, metadata_store

    def _save_db(self):
        """DB 저장 동기화"""
        faiss.write_index(self.index, str(self.index_path))
        with open(self.meta_path, "w", encoding="utf-8") as f:
            json.dump(self.metadata_store, f, ensure_ascii=False, indent=2)

    def _get_embedding(self, text: str) -> List[float]:
        """OpenAI Embedding 호출"""
        try:
            resp = self.client.embeddings.create(
                input=[text],
                model=self.embedding_model
            )
            return resp.data[0].embedding
        except Exception as e:
            logger.error(f"  [FAISS] 임베딩 생성 오류: {e}")
            return []

    def get_document_count(self) -> int:
        """현재 인덱스에 저장된 총 청크 개수 반환"""
        return self.index.ntotal if self.index else 0

    def get_document_ids(self) -> set:
        """현재 저장된 document_id 리스트 반환 (중복 처리 방지용)"""
        doc_ids = set()
        for meta in self.metadata_store.values():
            if "document_id" in meta:
                doc_ids.add(meta["document_id"])
        return doc_ids

    def upsert_json_data(self, json_files: List[Path]) -> int:
        """
        output/processed 의 JSON 파일들을 읽어들여
        QA Pair와 Chunk 본문을 텍스트 형태로 벡터화 후 FAISS에 삽입.
        """
        if not json_files:
            return 0
            
        existing_doc_ids = self.get_document_ids()
        vectors_to_add = []
        ids_to_add = []
        meta_to_add = {}
        
        # 현재 ID 기준점 (충돌 방지용 임의의 고유 숫자 생성 혹은 카운터 방식)
        # 삭제가 번번하지 않으므로 순차적 ID 부여를 위해 hash값 사용
        
        total_vectors = 0

        for jpath in json_files:
            try:
                with open(jpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    
                doc_id = data.get("document_id")
                if not doc_id or doc_id in existing_doc_ids:
                    logger.info(f"  [FAISS] 스킵: 이미 존재하는 문서입니다 ({jpath.name})")
                    continue
                    
                chunks = data.get("chunks", [])
                source_meta = data.get("source_metadata", {})
                
                for chunk in chunks:
                    chunk_id = chunk.get("chunk_id", str(uuid.uuid4()))
                    raw_text = chunk.get("raw_text", "")
                    search_keywords = chunk.get("search_keywords", [])
                    selection_reason = chunk.get("selection_reason_ko", "")
                    
                    # 빈 파싱 오류 덩어리 필터링
                    if not raw_text or not selection_reason:
                        continue
                        
                    # 1. 텍스트 임베딩: "키워드 + 선택 이유/요약" 만으로 깨끗하게 임베딩 (검색 노이즈 방지)
                    text_to_embed = f"핵심 키워드: {', '.join(search_keywords)}\n선택 이유 및 요약: {selection_reason}"
                    vec = self._get_embedding(text_to_embed)
                    
                    if vec:
                        # 64비트 정수형 ID 생성
                        numeric_id = abs(hash(chunk_id + "_origin")) % (10 ** 15)
                        vectors_to_add.append(vec)
                        ids_to_add.append(numeric_id)
                        
                        # 2. 메타데이터: 원문 텍스트 전체(100%) 보존 저장
                        meta_to_add[str(numeric_id)] = {
                            "type": "chunk",
                            "document_id": doc_id,
                            "source_name": source_meta.get("source_name", "Unknown"),
                            "source_url": source_meta.get("source_url", ""),
                            "search_keywords": search_keywords,
                            "selection_reason_ko": selection_reason,
                            "content": raw_text # 파싱된 텍스트 전체 보존
                        }
                        total_vectors += 1
                                
            except Exception as e:
                logger.error(f"  [FAISS] 파일 파싱/임베딩 오류 ({jpath.name}): {e}")

        # Batch Insert
        if vectors_to_add:
            # numpy float32 배열로 변환해야 FAISS에 입력 가능
            np_vectors = np.array(vectors_to_add).astype('float32')
            np_ids = np.array(ids_to_add).astype('int64')
            
            self.index.add_with_ids(np_vectors, np_ids)
            self.metadata_store.update(meta_to_add)
            
            self._save_db()
            logger.info(f"  [FAISS] {total_vectors}개의 새 벡터를 네임스페이스 '{self.keyword}' 에 저장 완료!")
        else:
            logger.info(f"  [FAISS] 새로 추가할 벡터가 매핑되지 않았습니다.")

        return total_vectors

    def search(self, query: str, top_k: int = 4) -> List[Dict[str, Any]]:
        """사용자 쿼리와 가장 유사한 상위 K개의 문서를 반환"""
        if self.get_document_count() == 0:
            return []

        vec = self._get_embedding(query)
        if not vec:
            return []
            
        np_query = np.array([vec]).astype('float32')
        distances, indices = self.index.search(np_query, top_k)
        
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            if idx == -1: # 매칭 결과 없음
                continue
                
            str_idx = str(idx)
            if str_idx in self.metadata_store:
                meta = self.metadata_store[str_idx]
                # 거리(Distances)가 작을수록 유사도가 높음 (L2 거리 기준).
                results.append({
                    "score": float(dist), 
                    "meta": meta
                })
        
        return results
