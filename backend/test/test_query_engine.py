import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.query_engine import QueryEngine


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def query_engine(mock_db):
    # Patch VectorStore, MetricsCalculator, and Ollama init
    with patch("app.services.query_engine.VectorStore") as MockVectorStore, \
         patch("app.services.query_engine.MetricsCalculator") as MockMetrics, \
         patch("app.services.query_engine.Ollama") as MockOllama:
        engine = QueryEngine(mock_db)
        engine.vector_store = MockVectorStore()
        engine.metrics_calculator = MockMetrics(mock_db)
        engine.llm = MagicMock()
        return engine


@pytest.mark.asyncio
async def test_classify_intent_calculation(query_engine):
    result = await query_engine._classify_intent("Calculate DPI of fund A")
    assert result == "calculation"

@pytest.mark.asyncio
async def test_classify_intent_retrieval(query_engine):
    result = await query_engine._classify_intent("List all funds")
    assert result == "retrieval"

@pytest.mark.asyncio
async def test_classify_intent_general(query_engine):
    result = await query_engine._classify_intent("Tell me something interesting")
    assert result == "general"


@pytest.mark.asyncio
async def test_generate_response_with_metrics(query_engine):
    """Should build prompt correctly and handle normal response"""
    query_engine.llm.invoke.return_value = MagicMock(content="This is the AI answer.")
    result = await query_engine._generate_response(
        query="What is DPI?",
        context=[{"content": "DPI means Distribution to Paid-In"}],
        metrics={"dpi": 1.2, "irr": 0.15},
        conversation_history=[{"role": "user", "content": "hello"}]
    )
    assert "AI answer" in result
    query_engine.llm.invoke.assert_called_once()


@pytest.mark.asyncio
async def test_generate_response_with_exception(query_engine):
    """Should return fallback message if LLM fails"""
    query_engine.llm.invoke.side_effect = Exception("Connection error")
    result = await query_engine._generate_response(
        query="What is IRR?",
        context=[{"content": "IRR is internal rate of return"}],
        metrics=None,
        conversation_history=[]
    )
    assert "error generating a response" in result.lower()


@pytest.mark.asyncio
async def test_process_query_with_metrics(query_engine):
    """Should handle calculation intent and include metrics"""
    query_engine._classify_intent = AsyncMock(return_value="calculation")
    query_engine.vector_store.similarity_search = AsyncMock(
        return_value=[{"content": "fund info", "score": 0.9}]
    )
    query_engine.metrics_calculator.calculate_all_metrics.return_value = {"dpi": 1.2}
    query_engine._generate_response = AsyncMock(return_value="Final answer")

    result = await query_engine.process_query("Calculate DPI", fund_id=123)

    assert result["answer"] == "Final answer"
    assert "sources" in result
    assert "metrics" in result
    query_engine.vector_store.similarity_search.assert_called_once()
    query_engine.metrics_calculator.calculate_all_metrics.assert_called_once_with(123)


@pytest.mark.asyncio
async def test_process_query_no_metrics(query_engine):
    """Should handle non-calculation intent correctly"""
    query_engine._classify_intent = AsyncMock(return_value="definition")
    query_engine.vector_store.similarity_search = AsyncMock(
        return_value=[{"content": "doc", "score": 0.5}]
    )
    query_engine._generate_response = AsyncMock(return_value="Answer about definition")

    result = await query_engine.process_query("What does IRR mean?", fund_id=None)

    assert "definition" not in result["answer"].lower() or "Answer" in result["answer"]
    assert isinstance(result["sources"], list)
    assert result["metrics"] is None


def test_initialize_llm_default(mock_db):
    """Should initialize Ollama with default values"""
    with patch("app.services.query_engine.Ollama") as MockOllama:
        engine = QueryEngine(mock_db)
        MockOllama.assert_called_once()
        assert hasattr(engine, "llm")
