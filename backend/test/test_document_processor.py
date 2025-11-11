import pytest
from unittest.mock import MagicMock
from datetime import datetime
from app.services.document_processor import (
    parse_fund_info, parse_date, parse_amount,
    parse_capital_calls, parse_distributions, parse_adjustments,
    DocumentProcessor
)

@pytest.fixture
def mock_db():
    """Mock SQLAlchemy Session"""
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = [1]
    db.execute.return_value.fetchall.return_value = []
    return db

@pytest.fixture
def mock_embedding_func():
    """Mock embedding generator"""
    return lambda text: [0.1, 0.2, 0.3]

# ---------- Unit Tests for Utility Functions ----------

def test_parse_fund_info_basic():
    text = """
    Fund Name: Alpha Growth Fund
    GP: Horizon Partners
    Vintage Year: 2020
    """
    result = parse_fund_info(text)
    assert result["name"] == "Alpha Growth Fund"
    assert result["gp_name"] == "Horizon Partners"
    assert result["vintage_year"] == 2020

def test_parse_fund_info_missing_fields():
    text = "No info here"
    result = parse_fund_info(text)
    assert result["name"] == "Unknown Fund"
    assert result["gp_name"] == "Unknown GP"
    assert result["vintage_year"] is None

def test_parse_date_formats():
    assert parse_date("2024-03-21") == datetime(2024, 3, 21)
    assert parse_date("03/21/2024") == datetime(2024, 3, 21)
    assert parse_date("March 21, 2024") == datetime(2024, 3, 21)
    assert parse_date("invalid") is None

def test_parse_amount():
    assert parse_amount("$1,234,567.89") == 1234567.89
    assert parse_amount("-$500") == -500.0
    assert parse_amount("abc") is None

# ---------- Parser Tests for Sections ----------

def test_parse_capital_calls():
    text = """
    Capital Calls
    Date Call Number Amount Description
    2024-01-01 Call 1 $100,000 Initial investment
    2024-03-01 Call 2 $200,000 Follow-up investment
    Distributions
    """
    results = parse_capital_calls(text)
    assert len(results) == 2
    assert results[0]["call_type"] == "Call 1"
    assert results[1]["amount"] == 200000.0

def test_parse_distributions():
    text = """
    Distributions
    Date Type Amount Recallable Description
    2024-04-01 Dividend $50,000 Yes Profit distribution
    2024-05-01 Return $30,000 No Partial return
    """
    results = parse_distributions(text)
    assert len(results) == 2
    assert results[0]["is_recallable"] is True
    assert results[1]["is_recallable"] is False

def test_parse_adjustments():
    text = """
    Adjustments
    Date Type Amount Description
    2024-06-01 Correction -$1,000 Typo fix
    2024-07-01 Fee $2,000 Management fee
    """
    results = parse_adjustments(text)
    assert len(results) == 2
    assert results[0]["amount"] == -1000.0
    assert results[1]["adjustment_type"] == "Fee"

# ---------- Integration Test for DocumentProcessor ----------

@pytest.mark.asyncio
async def test_process_document_success(tmp_path, mock_db, mock_embedding_func):
    # Buat PDF dummy (simulasi file yang bisa dibaca pdfplumber)
    pdf_path = tmp_path / "dummy.pdf"
    pdf_path.write_text("""
    Fund Name: Alpha Fund
    GP: Test Partners
    Vintage Year: 2022

    Capital Calls
    Date Call Number Amount Description
    2024-01-01 Call 1 $100,000 Initial

    Distributions
    Date Type Amount Recallable Description
    2024-02-01 Dividend $50,000 Yes Return

    Adjustments
    Date Type Amount Description
    2024-03-01 Fee $1,000 Service
    """)

    processor = DocumentProcessor(mock_db, mock_embedding_func)

    # Patch pdfplumber.open agar tidak benar-benar baca file PDF
    import pdfplumber
    pdfplumber.open = MagicMock(return_value=MagicMock(
        __enter__=lambda self: self,
        __exit__=lambda *a: None,
        pages=[MagicMock(extract_text=lambda: pdf_path.read_text())]
    ))

    result = await processor.process_document(str(pdf_path), 1, 1)

    assert result["status"] == "success"
    assert result["fund_id"] == 1
    assert result["parsed"]["capital_calls"] == 1
    assert result["parsed"]["distributions"] == 1
    assert result["parsed"]["adjustments"] == 1

    mock_db.execute.assert_called()  # ensure DB interaction happens
    mock_db.commit.assert_called_once()

@pytest.mark.asyncio
async def test_process_document_failure(mock_db, mock_embedding_func):
    processor = DocumentProcessor(mock_db, mock_embedding_func)

    # PDF kosong (simulasi error)
    import pdfplumber
    pdfplumber.open = MagicMock(return_value=MagicMock(
        __enter__=lambda self: self,
        __exit__=lambda *a: None,
        pages=[MagicMock(extract_text=lambda: "")]
    ))

    result = await processor.process_document("dummy.pdf", 1, 1)

    assert result["status"] == "failed"
    mock_db.rollback.assert_called_once()

