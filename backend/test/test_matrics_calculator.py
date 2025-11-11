import pytest
from unittest.mock import MagicMock
from decimal import Decimal
import numpy as np
from types import SimpleNamespace
from app.services.metrics_calculator import MetricsCalculator


@pytest.fixture
def mock_db():
    """Mock SQLAlchemy Session"""
    return MagicMock()


@pytest.fixture
def calculator(mock_db):
    """Instance MetricsCalculator dengan mock db"""
    return MetricsCalculator(db=mock_db)


def test_calculate_pic(calculator, mock_db):
    mock_db.query().filter().scalar.side_effect = [Decimal("100000"), Decimal("20000")]  # total_calls, total_adjustments
    result = calculator.calculate_pic(1)
    assert result == Decimal("80000")


def test_calculate_total_distributions(calculator, mock_db):
    mock_db.query().filter().scalar.return_value = Decimal("50000")
    result = calculator.calculate_total_distributions(1)
    assert result == Decimal("50000")


def test_calculate_dpi(calculator, mock_db):
    calculator.calculate_pic = MagicMock(return_value=Decimal("100000"))
    calculator.calculate_total_distributions = MagicMock(return_value=Decimal("50000"))

    result = calculator.calculate_dpi(1)
    assert result == 0.5


def test_calculate_irr_basic(calculator, mock_db, monkeypatch):
    """Test IRR calculation with fake cashflows"""
    fake_flows = [
        {"amount": -100000, "date": "2023-01-01"},
        {"amount": 60000, "date": "2024-01-01"},
        {"amount": 60000, "date": "2025-01-01"},
    ]
    calculator._get_cash_flows = MagicMock(return_value=fake_flows)

    irr = calculator.calculate_irr(1)
    assert irr is not None
    assert isinstance(irr, float)
    assert irr > 0  # should be positive IRR


def test_calculate_nav(calculator, mock_db):
    mock_db.query().filter().scalar.return_value = Decimal("120000")
    result = calculator.calculate_nav(1)
    assert result == 120000.0


def test_calculate_rvpi(calculator):
    calculator.calculate_nav = MagicMock(return_value=50000)
    calculator.calculate_pic = MagicMock(return_value=Decimal("100000"))
    result = calculator.calculate_rvpi(1)
    assert result == 0.5


def test_calculate_tvpi(calculator):
    calculator.calculate_total_distributions = MagicMock(return_value=Decimal("50000"))
    calculator.calculate_nav = MagicMock(return_value=50000)
    calculator.calculate_pic = MagicMock(return_value=Decimal("100000"))
    result = calculator.calculate_tvpi(1)
    assert result == 1.0


def test_calculate_all_metrics(calculator):
    calculator.calculate_pic = MagicMock(return_value=Decimal("100000"))
    calculator.calculate_total_distributions = MagicMock(return_value=Decimal("50000"))
    calculator.calculate_dpi = MagicMock(return_value=0.5)
    calculator.calculate_irr = MagicMock(return_value=15.25)
    calculator.calculate_nav = MagicMock(return_value=50000)
    calculator.calculate_rvpi = MagicMock(return_value=0.5)
    calculator.calculate_tvpi = MagicMock(return_value=1.0)

    result = calculator.calculate_all_metrics(1)
    assert set(result.keys()) == {
        "pic", "total_distributions", "dpi", "irr", "tvpi", "rvpi", "nav"
    }
    assert result["dpi"] == 0.5
    assert result["irr"] == 15.25

def test_get_cash_flows(calculator, mock_db):
    """Test _get_cash_flows() returns correctly sorted list"""
    # Mock capital calls
    mock_calls = [
        SimpleNamespace(call_date="2023-01-01", amount=Decimal("100000")),
        SimpleNamespace(call_date="2023-03-01", amount=Decimal("50000")),
    ]
    # Mock distributions
    mock_dists = [
        SimpleNamespace(distribution_date="2023-02-01", amount=Decimal("30000")),
        SimpleNamespace(distribution_date="2023-04-01", amount=Decimal("40000")),
    ]

    mock_db.query().filter().order_by().all.side_effect = [mock_calls, mock_dists]

    result = calculator._get_cash_flows(1)

    assert len(result) == 4
    assert result[0]["amount"] == -100000.0  # capital call → negatif
    assert result[-1]["amount"] == 40000.0   # distribution → positif

def test_calculate_irr_invalid(calculator):
    """Test IRR returns None for invalid or empty cashflows"""
    calculator._get_cash_flows = MagicMock(return_value=[{"amount": -1000}])  # cuma 1 cashflow
    result = calculator.calculate_irr(1)
    assert result is None

    # Case invalid IRR (np.irr returns NaN)
    calculator._get_cash_flows = MagicMock(return_value=[{"amount": -1000}, {"amount": 1000}])
    import numpy_financial as npf
    npf.irr = MagicMock(return_value=np.nan)
    result = calculator.calculate_irr(1)
    assert result is None


def test_get_calculation_breakdown_dpi(calculator, mock_db):
    """Test DPI breakdown returns detailed structure"""
    calculator.calculate_pic = MagicMock(return_value=Decimal("100000"))
    calculator.calculate_total_distributions = MagicMock(return_value=Decimal("50000"))
    calculator.calculate_dpi = MagicMock(return_value=0.5)

    # Mock queries untuk capital_calls, distributions, adjustments
    fake_call = SimpleNamespace(call_date="2023-01-01", amount=Decimal("100000"), description="Initial call")
    fake_dist = SimpleNamespace(distribution_date="2023-06-01", amount=Decimal("50000"), is_recallable=False, description="Payout")
    fake_adj = SimpleNamespace(adjustment_date="2023-03-01", amount=Decimal("0"), adjustment_type="NAV_ADJUSTMENT", description="None")

    mock_db.query().filter().order_by().all.side_effect = [
        [fake_call], [fake_dist], [fake_adj]
    ]

    result = calculator.get_calculation_breakdown(1, "dpi")
    assert "formula" in result
    assert result["metric"] == "DPI"
    assert "transactions" in result


def test_get_calculation_breakdown_irr(calculator):
    """Test IRR breakdown includes cash flow summary"""
    calculator._get_cash_flows = MagicMock(return_value=[
        {"amount": -1000, "date": "2023-01-01"},
        {"amount": 1100, "date": "2024-01-01"},
    ])
    calculator.calculate_irr = MagicMock(return_value=10.0)

    result = calculator.get_calculation_breakdown(1, "irr")
    assert result["metric"] == "IRR"
    assert "cash_flow_summary" in result
    assert result["result"] == 10.0


def test_get_calculation_breakdown_pic(calculator, mock_db):
    """Test PIC breakdown"""
    fake_call = SimpleNamespace(call_date="2023-01-01", amount=Decimal("100000"), description="Call 1")
    fake_adj = SimpleNamespace(adjustment_date="2023-02-01", amount=Decimal("10000"), adjustment_type="NAV_ADJUSTMENT", description="Adj 1")

    mock_db.query().filter().order_by().all.side_effect = [[fake_call], [fake_adj]]
    calculator.calculate_pic = MagicMock(return_value=Decimal("90000"))

    result = calculator.get_calculation_breakdown(1, "pic")
    assert result["metric"] == "PIC"
    assert "transactions" in result
    assert result["result"] == 90000.0


def test_get_calculation_breakdown_unknown(calculator):
    """Test unknown metric returns error dict"""
    result = calculator.get_calculation_breakdown(1, "unknown")
    assert "error" in result

def test_calculate_nav_exception(calculator, mock_db):
    """Should return None when DB query fails"""
    mock_db.query.side_effect = Exception("DB Error")
    result = calculator.calculate_nav(1)
    assert result is None


def test_calculate_pic_zero_calls(calculator, mock_db):
    """Should return 0 if no capital calls (division by zero safe)"""
    mock_query = mock_db.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_filter.scalar.side_effect = [Decimal(0), Decimal(0)]  # total_calls, total_adjustments
    
    result = calculator.calculate_pic(1)
    assert result == Decimal(0)


def test_calculate_dpi_division_by_zero(calculator, mock_db):
    """Should handle zero capital calls gracefully"""
    calculator.calculate_pic = MagicMock(return_value=Decimal("0"))
    calculator.calculate_total_distributions = MagicMock(return_value=Decimal("100000"))
    result = calculator.calculate_dpi(1)
    assert result == 0 or result == 0.0


def test_calculate_irr_raises_exception(calculator):
    """Should return None if np.irr raises exception"""
    calculator._get_cash_flows = MagicMock(return_value=[{"amount": -1000}, {"amount": 1000}])
    import numpy_financial as npf
    npf.irr = MagicMock(side_effect=Exception("Invalid IRR"))
    result = calculator.calculate_irr(1)
    assert result is None


def test_get_calculation_breakdown_unknown_metric(calculator):
    """Should return error dict for unknown metric"""
    result = calculator.get_calculation_breakdown(1, "random_metric")
    assert "error" in result
    assert "Unknown metric" in result["error"]