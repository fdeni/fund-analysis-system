import pytest
from datetime import date
from unittest.mock import MagicMock
from app.services.table_parser import TableParser


@pytest.fixture
def parser():
    return TableParser()

def test_parse_page_tables_returns_non_empty(parser):
    mock_page = MagicMock()
    mock_page.extract_tables.return_value = [
        [["A", "B"], ["1", "2"]],
        [],
        [["X", "Y"]]
    ]

    result = parser.parse_page_tables(mock_page)

    assert len(result) == 2
    assert all(isinstance(t, list) for t in result)
    mock_page.extract_tables.assert_called_once()


def test_clean_table_removes_empty_rows_and_strips(parser):
    table = [
        ["  A ", " B  "],
        [None, None],
        ["", "   "],
        ["  Value  ", "  123 "]
    ]
    result = parser.clean_table(table)

    assert result == [["A", "B"], ["Value", "123"]]
    assert all(all(isinstance(cell, str) for cell in row) for row in result)


def test_clean_table_handles_non_str_cells(parser):
    table = [["A", 123, None], ["", ""]]
    result = parser.clean_table(table)
    assert result == [["A", 123, None]]


def test_validate_table_converts_numbers(parser):
    table = [["100", "2,500", "not_a_number"]]
    result = parser.validate_table(table)

    # Converted numeric strings become floats
    assert result[0][0] == 100.0
    assert result[0][1] == 2500.0
    # non-numeric string remains unchanged
    assert result[0][2] == "not_a_number"


def test_validate_table_converts_dates(parser):
    table = [["01/01/2024", "15/08/2023", "random"]]
    result = parser.validate_table(table)

    assert result[0][0] == date(2024, 1, 1)
    assert result[0][1] == date(2023, 8, 15)
    assert result[0][2] == "random"


def test_validate_table_mixed_data(parser):
    table = [["100", "01/01/2024", "ABC"]]
    result = parser.validate_table(table)

    assert isinstance(result[0][0], float)
    assert isinstance(result[0][1], date)
    assert result[0][2] == "ABC"


@pytest.mark.parametrize("text,expected", [
    ([["Capital Call", "Amount"]], "capital_call"),
    ([["Distribution Summary"]], "distribution"),
    ([["Adjustment Data"]], "adjustment"),
])
def test_classify_table(parser, text, expected):
    assert parser.classify_table(text) == expected
