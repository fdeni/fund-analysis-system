"""
RAG Engine Service
Handles:
- Text chunking
- Embedding generation
- FAISS/PGVector-based similarity search
- Context retrieval for LLM
- Prompt engineering for accurate responses
"""

from typing import List, Dict, Any, Optional
import math
import asyncio
from app.services.vector_store import VectorStore
from app.core.config import settings

# Constants for chunking
CHUNK_SIZE = 500  # characters per chunk
CHUNK_OVERLAP = 50  # overlap characters between chunks


class RAGEngine:
    """Retrieval-Augmented Generation Engine"""

    def __init__(self, vector_store: VectorStore = None):
        self.vector_store = vector_store or VectorStore()

    def chunk_text(self, text: str) -> List[str]:
        """
        Split text into overlapping chunks for embedding
        """
        chunks = []
        start = 0
        while start < len(text):
            end = start + CHUNK_SIZE
            chunk = text[start:end]
            chunks.append(chunk)
            start += CHUNK_SIZE - CHUNK_OVERLAP
        return chunks

    async def add_document(self, document_id: int, fund_id: int, content: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Chunk the document, generate embeddings, and store in vector store
        """
        metadata = metadata or {}
        metadata["document_id"] = document_id
        metadata["fund_id"] = fund_id

        chunks = self.chunk_text(content)
        tasks = []
        for chunk in chunks:
            tasks.append(self.vector_store.add_document(chunk, metadata))
        await asyncio.gather(*tasks)

    async def retrieve_context(self, query: str, fund_id: Optional[int] = None, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Retrieve top-k similar chunks from vector store
        """
        filter_metadata = {"fund_id": fund_id} if fund_id else None
        results = await self.vector_store.similarity_search(query, k=top_k, filter_metadata=filter_metadata)
        return results

    def build_prompt(self, query: str, context_chunks: List[Dict[str, Any]]) -> str:
        """
        Build a prompt for the LLM using retrieved context
        """
        context_text = "\n\n".join([f"Context {i+1}:\n{chunk['content']}" for i, chunk in enumerate(context_chunks)])
        prompt = (
            "You are an expert financial assistant.\n"
            "Use the context below to answer the user's question accurately.\n"
            "If the answer is not contained in the context, respond with 'I don't know'.\n\n"
            f"{context_text}\n\n"
            f"Question: {query}\n"
            "Answer:"
        )
        return prompt

    async def answer_query(self, query: str, fund_id: Optional[int] = None, top_k: int = 5) -> Dict[str, Any]:
        """
        Main method: retrieve context + generate LLM response
        """
        context_chunks = await self.retrieve_context(query, fund_id=fund_id, top_k=top_k)
        prompt = self.build_prompt(query, context_chunks)

        # Use Ollama LLM to get answer
        from langchain_community.llms import Ollama
        llm = Ollama(model=settings.OLLAMA_MODEL, base_url=settings.OLLAMA_BASE_URL)
        response = await llm.agenerate([prompt])

        return {
            "query": query,
            "answer": response.generations[0][0].text,
            "context": context_chunks
        }

    async def add_documents_bulk(self, documents: List[Dict[str, Any]]):
        """
        Add multiple documents to the RAG engine at once
        Each document should be a dict: {"document_id": int, "fund_id": int, "content": str, "metadata": dict}
        """
        tasks = []
        for doc in documents:
            tasks.append(self.add_document(
                document_id=doc["document_id"],
                fund_id=doc["fund_id"],
                content=doc["content"],
                metadata=doc.get("metadata")
            ))
        await asyncio.gather(*tasks)
