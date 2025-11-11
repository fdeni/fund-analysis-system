import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.rag_engine import RAGEngine, CHUNK_SIZE, CHUNK_OVERLAP


@pytest.fixture
def mock_vector_store():
    store = AsyncMock()
    store.add_document = AsyncMock()
    store.similarity_search = AsyncMock(return_value=[
        {"content": "Fund performance summary", "metadata": {"fund_id": 1}, "score": 0.9},
        {"content": "IRR details", "metadata": {"fund_id": 1}, "score": 0.85},
    ])
    return store


@pytest.fixture
def rag_engine(mock_vector_store):
    return RAGEngine(vector_store=mock_vector_store)

def test_chunk_text_basic(rag_engine):
    text = "A" * (CHUNK_SIZE + 100)
    chunks = rag_engine.chunk_text(text)
    assert isinstance(chunks, list)
    assert len(chunks) >= 2
    assert all(isinstance(c, str) for c in chunks)
    assert chunks[0].startswith("A")
    assert chunks[-1].endswith("A")


def test_chunk_text_short(rag_engine):
    text = "Hello World"
    chunks = rag_engine.chunk_text(text)
    assert chunks == ["Hello World"]

@pytest.mark.asyncio
async def test_add_document_calls_vector_store(rag_engine, mock_vector_store):
    await rag_engine.add_document(1, 2, "Sample text for fund performance.")
    assert mock_vector_store.add_document.call_count >= 1

    # Verify metadata attached
    args, kwargs = mock_vector_store.add_document.call_args
    assert "fund_id" in args[1]
    assert args[1]["document_id"] == 1

@pytest.mark.asyncio
async def test_retrieve_context_with_fund_id(rag_engine, mock_vector_store):
    results = await rag_engine.retrieve_context("performance", fund_id=1, top_k=3)
    assert isinstance(results, list)
    assert results[0]["content"].startswith("Fund")
    mock_vector_store.similarity_search.assert_awaited_with(
        "performance", k=3, filter_metadata={"fund_id": 1}
    )


@pytest.mark.asyncio
async def test_retrieve_context_without_fund_id(rag_engine, mock_vector_store):
    await rag_engine.retrieve_context("performance")
    mock_vector_store.similarity_search.assert_awaited_with(
        "performance", k=5, filter_metadata=None
    )

def test_build_prompt_includes_context(rag_engine):
    context = [
        {"content": "Fund A achieved 20% IRR"},
        {"content": "Fund B performed well"}
    ]
    query = "What is the IRR of Fund A?"
    prompt = rag_engine.build_prompt(query, context)

    assert "Context 1:" in prompt
    assert "Question:" in prompt
    assert "Answer:" in prompt
    assert "20% IRR" in prompt
    assert query in prompt

@pytest.mark.asyncio
async def test_add_documents_bulk(rag_engine, mock_vector_store):
    documents = [
        {"document_id": 1, "fund_id": 1, "content": "Fund A data"},
        {"document_id": 2, "fund_id": 2, "content": "Fund B data"}
    ]

    await rag_engine.add_documents_bulk(documents)

    # Verify both add_document called
    assert mock_vector_store.add_document.await_count >= 2
    
@pytest.mark.asyncio
async def test_add_document_with_metadata(monkeypatch):
    mock_vector_store = MagicMock()
    mock_vector_store.add_document = AsyncMock(return_value=None)

    engine = RAGEngine(vector_store=mock_vector_store)

    engine.chunk_text = MagicMock(return_value=["chunk1", "chunk2"])

    await engine.add_document(
        document_id=10,
        fund_id=5,
        content="Example document text.",
        metadata={"custom": "meta"}
    )

    expected_metadata = {"custom": "meta", "document_id": 10, "fund_id": 5}
    mock_vector_store.add_document.assert_any_call("chunk1", expected_metadata)
    mock_vector_store.add_document.assert_any_call("chunk2", expected_metadata)

    assert mock_vector_store.add_document.await_count == 2
