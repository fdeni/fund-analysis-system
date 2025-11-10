"""
Vector store service using pgvector (PostgreSQL extension)
"""
import json
import numpy as np
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from langchain_community.embeddings import HuggingFaceEmbeddings
from app.core.config import settings
from app.db.session import SessionLocal

class VectorStore:
    """Vector store using pgvector and HuggingFace embeddings"""
    
    def __init__(self, db: Session = None):
        self.db = db or SessionLocal()
        self.embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
        self._ensure_extension()
    
    def _ensure_extension(self):
        """Enable pgvector extension and create table if needed"""
        try:
            self.db.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            dimension = 384  # sesuai HuggingFace MiniLM-L6-v2

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS document_embeddings (
                id SERIAL PRIMARY KEY,
                document_id INTEGER,
                fund_id INTEGER,
                content TEXT NOT NULL,
                embedding vector({dimension}),
                metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS document_embeddings_embedding_idx 
            ON document_embeddings USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100);
            """
            self.db.execute(text(create_table_sql))
            self.db.commit()
        except Exception as e:
            print(f"Error ensuring pgvector extension: {e}")
            self.db.rollback()
    
    async def _get_embedding(self, text: str) -> np.ndarray:
        """Generate embedding using HuggingFace"""
        vec = self.embeddings.embed_query(text)
        return np.array(vec, dtype=np.float32)
    
    async def add_document(self, content: str, metadata: Dict[str, Any]):
        """Add a document to the vector store"""
        try:
            embedding = await self._get_embedding(content)
            embedding_str = ','.join(map(str, embedding.tolist()))  # pgvector cast friendly

            insert_sql = text("""
                INSERT INTO document_embeddings (document_id, fund_id, content, embedding, metadata)
                VALUES (:document_id, :fund_id, :content, :embedding::vector, :metadata::jsonb)
            """)
            self.db.execute(insert_sql, {
                "document_id": metadata.get("document_id"),
                "fund_id": metadata.get("fund_id"),
                "content": content,
                "embedding": embedding_str,
                "metadata": json.dumps(metadata)
            })
            self.db.commit()
        except Exception as e:
            print(f"Error adding document: {e}")
            self.db.rollback()
            raise
    
    async def similarity_search(
        self, 
        query: str, 
        k: int = 5, 
        filter_metadata: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Search for similar documents"""
        try:
            query_embedding = await self._get_embedding(query)
            embedding_pg = f"ARRAY[{','.join(map(str, query_embedding.tolist()))}]::vector"

            where_clause = ""
            params = {"k": k}

            if filter_metadata:
                conditions = []
                for key, value in filter_metadata.items():
                    if key in ["document_id", "fund_id"]:
                        param_name = f"param_{key}"
                        conditions.append(f"{key} = :{param_name}")
                        params[param_name] = value
                if conditions:
                    where_clause = "WHERE " + " AND ".join(conditions)

            # embedding inserted langsung sebagai literal
            search_sql = text(f"""
                SELECT 
                    id,
                    document_id,
                    fund_id,
                    content,
                    metadata,
                    1 - (embedding <=> {embedding_pg}) AS similarity_score
                FROM document_embeddings
                {where_clause}
                ORDER BY embedding <=> {embedding_pg}
                LIMIT :k
            """)

            result = self.db.execute(search_sql, params)
            rows = result.fetchall()

            return [
                {
                    "id": row.id,
                    "document_id": row.document_id,
                    "fund_id": row.fund_id,
                    "content": row.content,
                    "metadata": row.metadata,
                    "score": float(row.similarity_score)
                }
                for row in rows
            ]
        except Exception as e:
            print(f"Error in similarity search: {e}")
            return []

    def clear(self, fund_id: Optional[int] = None):
        """Clear vector store"""
        try:
            if fund_id:
                self.db.execute(text("DELETE FROM document_embeddings WHERE fund_id = :fund_id"), {"fund_id": fund_id})
            else:
                self.db.execute(text("DELETE FROM document_embeddings"))
            self.db.commit()
        except Exception as e:
            print(f"Error clearing vector store: {e}")
            self.db.rollback()
