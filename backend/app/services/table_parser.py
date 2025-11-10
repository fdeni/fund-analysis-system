from typing import List
from datetime import datetime

class TableParser:
    """Parse, clean, classify tables from PDF pages"""

    def parse_page_tables(self, page) -> List[List[List[str]]]:
        """
        Extract tables from a pdfplumber page
        """
        tables = page.extract_tables()
        return [t for t in tables if t]

    def clean_table(self, table: List[List[str]]) -> List[List[str]]:
        """
        Clean table:
        - Strip strings
        - Remove empty rows
        """
        cleaned = []
        for row in table:
            cleaned_row = [cell.strip() if isinstance(cell, str) else cell for cell in row]
            if any(cleaned_row):
                cleaned.append(cleaned_row)
        return cleaned

    def validate_table(self, table: List[List[str]]) -> List[List[str]]:
        """
        Validate table:
        - Convert numeric strings to float
        - Parse dates if any (example: DD/MM/YYYY)
        """
        for row in table:
            for i, cell in enumerate(row):
                if isinstance(cell, str):
                    # Convert numbers
                    try:
                        row[i] = float(cell.replace(",", ""))
                        continue
                    except:
                        pass
                    # Convert dates
                    try:
                        row[i] = datetime.strptime(cell, "%d/%m/%Y").date()
                    except:
                        pass
        return table

    def classify_table(self, table: List[List[str]]) -> str:
        """
        Classify table type based on keywords
        """
        flat_text = " ".join(str(cell) for row in table for cell in row).lower()
        if "capital call" in flat_text:
            return "capital_call"
        elif "distribution" in flat_text:
            return "distribution"
        else:
            return "adjustment"
