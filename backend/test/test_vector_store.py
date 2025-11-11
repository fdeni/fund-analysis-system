import pytest
import numpy as np
from unittest.mock import MagicMock, patch, AsyncMock
from app.services.vector_store import VectorStore


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.execute = MagicMock()
    db.commit = MagicMock()
    db.rollback = MagicMock()
    return db


@pytest.fixture
def store(mock_db):
    with patch("app.services.vector_store.HuggingFaceEmbeddings") as MockEmbeddings:
        mock_embed = MagicMock()
        mock_embed.embed_query.return_value = [0.1, 0.2, 0.3]
        MockEmbeddings.return_value = mock_embed
        store = VectorStore(db=mock_db)
        store.embeddings = mock_embed
        return store


@pytest.mark.asyncio
async def test_get_embedding_returns_numpy_array(store):
    text = "sample text"
    result = await store._get_embedding(text)

    assert isinstance(result, np.ndarray)
    assert result.dtype == np.float32
    store.embeddings.embed_query.assert_called_once_with(text)


@pytest.mark.asyncio
async def test_add_document_failure_rolls_back(store, mock_db):
    with patch.object(store, "_get_embedding", new=AsyncMock(side_effect=Exception("Embedding failed"))):
        with pytest.raises(Exception):
            await store.add_document("content", {"document_id": 1, "fund_id": 1})

    mock_db.rollback.assert_called_once()


@pytest.mark.asyncio
async def test_similarity_search_handles_exception(store, mock_db):
    mock_db.execute.side_effect = Exception("DB error")

    with patch.object(store, "_get_embedding", new=AsyncMock(return_value=np.array([0.1, 0.2, 0.3]))):
        results = await store.similarity_search("query")

    assert results == []
    
@pytest.mark.asyncio
async def test_ensure_extension_error_handling(mock_db):
    # Simulate error when executing SQL
    mock_db.execute.side_effect = Exception("DB error")
    vs = VectorStore(db=mock_db)
    # It should catch and rollback
    mock_db.rollback.assert_called()
    

@pytest.mark.asyncio
async def test_add_document_exception(mock_db):
    vs = VectorStore(db=mock_db)
    # Mock embedding generation
    vs._get_embedding = MagicMock(return_value=np.array([0.1, 0.2, 0.3]))
    # Force DB execute to raise exception
    mock_db.execute.side_effect = Exception("Insert error")
    with pytest.raises(Exception):
        await vs.add_document("content", {"document_id": 1, "fund_id": 2})
    mock_db.rollback.assert_called()
    
@pytest.mark.asyncio
async def test_similarity_search_error(mock_db):
    vs = VectorStore(db=mock_db)
    vs._get_embedding = MagicMock(return_value=np.array([0.1, 0.2, 0.3]))
    mock_db.execute.side_effect = Exception("Query failed")
    result = await vs.similarity_search("query")
    assert result == []

def test_clear_with_error(mock_db):
    vs = VectorStore(db=mock_db)
    mock_db.execute.side_effect = Exception("Delete error")
    vs.clear(fund_id=123)
    mock_db.rollback.assert_called()

def test_ensure_extension_exception(mock_db):
    mock_db.execute.side_effect = Exception("pgvector failed")
    vs = VectorStore(db=mock_db)
    # Ensure rollback called when error happens
    mock_db.rollback.assert_called()

@pytest.mark.asyncio
async def test_add_document_raises_exception(mock_db):
    vs = VectorStore(db=mock_db)
    vs._get_embedding = MagicMock(return_value=np.array([0.1, 0.2, 0.3]))
    mock_db.execute.side_effect = Exception("insert fail")

    with pytest.raises(Exception):
        await vs.add_document("content", {"document_id": 1, "fund_id": 1})
    mock_db.rollback.assert_called()


@pytest.mark.asyncio
async def test_similarity_search_exception(mock_db):
    vs = VectorStore(db=mock_db)
    vs._get_embedding = MagicMock(return_value=np.array([0.1, 0.2, 0.3]))
    mock_db.execute.side_effect = Exception("query error")

    result = await vs.similarity_search("test query")
    assert result == []  # must return empty list


def test_clear_exception(mock_db):
    vs = VectorStore(db=mock_db)
    mock_db.execute.side_effect = Exception("delete failed")

    vs.clear(fund_id=99)
    mock_db.rollback.assert_called()