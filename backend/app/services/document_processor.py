import re
from typing import Dict, List, Optional
from datetime import datetime
import pdfplumber
from sqlalchemy.orm import Session
from sqlalchemy import text as sql_text
import logging

logger = logging.getLogger(__name__)

# Parsers with Error Handling
def parse_fund_info(text: str) -> Dict:
    """Parse fund information with fallback values"""
    fund_info = {
        'name': 'Unknown Fund',
        'gp_name': 'Unknown GP',
        'vintage_year': None
    }
    
    try:
        name_match = re.search(r"Fund Name:\s*(.+)", text, re.IGNORECASE)
        if name_match:
            fund_info['name'] = name_match.group(1).strip()
    except Exception as e:
        logger.warning(f"Failed to parse fund name: {e}")
    
    try:
        gp_match = re.search(r"GP:\s*(.+)", text, re.IGNORECASE)
        if gp_match:
            fund_info['gp_name'] = gp_match.group(1).strip()
    except Exception as e:
        logger.warning(f"Failed to parse GP: {e}")
    
    try:
        year_match = re.search(r"Vintage Year:\s*(\d{4})", text, re.IGNORECASE)
        if year_match:
            fund_info['vintage_year'] = int(year_match.group(1))
    except Exception as e:
        logger.warning(f"Failed to parse vintage year: {e}")
    
    return fund_info

def parse_date(date_str: str) -> Optional[datetime]:
    """Try multiple date formats"""
    date_formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%Y/%m/%d"
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date: {date_str}")
    return None

def parse_amount(amount_str: str) -> Optional[float]:
    """Extract numeric value from amount string"""
    try:
        # Remove currency symbols, commas, spaces
        cleaned = re.sub(r"[^\d.-]", "", amount_str)
        return float(cleaned) if cleaned else None
    except Exception as e:
        logger.warning(f"Could not parse amount: {amount_str} - {e}")
        return None

def parse_table_generic(text: str, section_name: str) -> List[List[str]]:
    """Generic table parser that handles both formatted and inline tables"""
    rows = []
    
    #Find section and extract content until next section
    section_pattern = rf"{section_name}\s+(.*?)(?=\n[A-Z][a-z]+\s+[A-Z]|Performance Summary|Fund Strategy|Key Definitions|\Z)"
    match = re.search(section_pattern, text, re.DOTALL | re.IGNORECASE)
    
    if not match:
        logger.warning(f"Could not find section: {section_name}")
        return []
    
    content = match.group(1).strip()
    logger.debug(f"Section content for {section_name}:\n{content[:200]}...")
    
    # Split by date patterns (YYYY-MM-DD format)
    # This handles inline tables where all data is in one line
    date_pattern = r'(\d{4}-\d{2}-\d{2})'
    
    # Split content by dates to get individual rows
    parts = re.split(date_pattern, content)
    
    # Reconstruct rows: parts are [before_date, date1, after_date1, date2, after_date2, ...]
    i = 1  # Start from first date
    while i < len(parts):
        if i + 1 < len(parts):
            date_str = parts[i]
            row_data = parts[i + 1].strip()
            
            # Extract columns from row_data
            # Remove extra spaces and split
            row_data = re.sub(r'\s+', ' ', row_data)
            
            # Create row starting with date
            row = [date_str]
            
            # Add remaining data
            remaining = row_data.split(' ', 3)  # Split into max 4 parts
            row.extend(remaining)
            
            rows.append(row)
            logger.debug(f"Parsed row: {row}")
        
        i += 2
    
    return rows

def parse_capital_calls(text: str) -> List[Dict]:
    """Parse capital calls - handles inline format"""
    # Look for Capital Calls section
    pattern = r'Capital Calls\s+Date Call Number Amount Description\s+(.*?)(?=Distributions|Adjustments|Performance Summary|\Z)'
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    
    if not match:
        logger.warning("Could not find Capital Calls section")
        return []
    
    content = match.group(1).strip()
    logger.info(f"Capital Calls content: {content[:300]}")
    
    result = []
    
    # Pattern: YYYY-MM-DD Call X $X,XXX,XXX Description text
    call_pattern = r'(\d{4}-\d{2}-\d{2})\s+(Call\s+\d+)\s+\$?([\d,]+)\s+(.+?)(?=\d{4}-\d{2}-\d{2}|\Z)'
    
    matches = re.finditer(call_pattern, content, re.DOTALL)
    
    for match in matches:
        try:
            call_date = parse_date(match.group(1))
            call_type = match.group(2).strip()
            amount = parse_amount(match.group(3))
            description = match.group(4).strip()
            
            if call_date and amount:
                result.append({
                    "call_date": call_date,
                    "call_type": call_type,
                    "amount": amount,
                    "description": description
                })
                logger.info(f"Parsed capital call: date={call_date.date()}, type={call_type}, amount={amount}")
        except Exception as e:
            logger.warning(f"Failed to parse capital call: {e}")
    
    return result

def parse_distributions(text: str) -> List[Dict]:
    """Parse distributions - handles inline format"""
    # Look for Distributions section
    pattern = r'Distributions\s+Date Type Amount Recallable Description\s+(.*?)(?=Adjustments|Performance Summary|\Z)'
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    
    if not match:
        logger.warning("Could not find Distributions section")
        return []
    
    content = match.group(1).strip()
    logger.info(f"Distributions content: {content[:300]}")
    
    result = []
    
    # Pattern: YYYY-MM-DD Type $X,XXX,XXX Yes/No Description
    dist_pattern = r'(\d{4}-\d{2}-\d{2})\s+([\w\s]+?)\s+\$?([\d,]+)\s+(Yes|No)\s+(.+?)(?=\d{4}-\d{2}-\d{2}|\Z)'
    
    matches = re.finditer(dist_pattern, content, re.DOTALL | re.IGNORECASE)
    
    for match in matches:
        try:
            dist_date = parse_date(match.group(1))
            dist_type = match.group(2).strip()
            amount = parse_amount(match.group(3))
            is_recallable = match.group(4).strip().lower() == 'yes'
            description = match.group(5).strip()
            
            if dist_date and amount:
                result.append({
                    "distribution_date": dist_date,
                    "distribution_type": dist_type,
                    "amount": amount,
                    "is_recallable": is_recallable,
                    "description": description
                })
                logger.info(f"Parsed distribution: date={dist_date.date()}, type={dist_type}, amount={amount}")
        except Exception as e:
            logger.warning(f"Failed to parse distribution: {e}")
    
    return result

def parse_adjustments(text: str) -> List[Dict]:
    """Parse adjustments - handles inline format"""
    # Look for Adjustments section
    pattern = r'Adjustments\s+Date Type Amount Description\s+(.*?)(?=Performance Summary|Fund Strategy|\Z)'
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    
    if not match:
        logger.warning("Could not find Adjustments section")
        return []
    
    content = match.group(1).strip()
    logger.info(f"Adjustments content: {content[:300]}")
    
    result = []
    
    # Pattern: YYYY-MM-DD Type $X,XXX,XXX or -$X,XXX,XXX Description
    adj_pattern = r'(\d{4}-\d{2}-\d{2})\s+([\w\s]+?)\s+(-?\$?[\d,]+)\s+(.+?)(?=\d{4}-\d{2}-\d{2}|\Z)'
    
    matches = re.finditer(adj_pattern, content, re.DOTALL | re.IGNORECASE)
    
    for match in matches:
        try:
            adj_date = parse_date(match.group(1))
            adj_type = match.group(2).strip()
            amount = parse_amount(match.group(3))
            description = match.group(4).strip()
            
            if adj_date and amount is not None:  # amount can be negative
                result.append({
                    "adjustment_date": adj_date,
                    "adjustment_type": adj_type,
                    "amount": amount,
                    "description": description
                })
                logger.info(f"Parsed adjustment: date={adj_date.date()}, type={adj_type}, amount={amount}")
        except Exception as e:
            logger.warning(f"Failed to parse adjustment: {e}")
    
    return result

# Text Chunking
def chunk_text(text: str, chunk_size: int = 500) -> List[str]:
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunks.append(" ".join(words[i:i + chunk_size]))
    return chunks

# Document Processor
class DocumentProcessor:
    def __init__(self, db: Session, embedding_func):
        self.db = db
        self.embedding_func = embedding_func

    async def process_document(self, file_path: str, document_id: int, fund_id: int):
        try:
            # Extract text
            with pdfplumber.open(file_path) as pdf:
                pdf_text = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())

            if not pdf_text.strip():
                raise ValueError("PDF contains no extractable text")

            logger.info(f"Extracted {len(pdf_text)} characters from PDF")
            logger.debug(f"First 1000 chars:\n{pdf_text[:1000]}")

            # Chunk text & save embeddings
            chunks = chunk_text(pdf_text)
            logger.info(f"Created {len(chunks)} text chunks")
            
            for chunk in chunks:
                embedding = self.embedding_func(chunk)
                self.db.execute(
                    sql_text("""
                        INSERT INTO document_embeddings (document_id, content, embedding, created_at)
                        VALUES (:doc_id, :content, :embedding, :created_at)
                    """),
                    {
                        "doc_id": document_id, 
                        "content": chunk,
                        "embedding": embedding, 
                        "created_at": datetime.now()
                    }
                )

            # Parse and save fund info
            fund_info = parse_fund_info(pdf_text)
            logger.info(f"Parsed fund info: {fund_info}")
            
            fund_check = self.db.execute(
                sql_text("SELECT id FROM funds WHERE id=:fund_id"),
                {"fund_id": fund_id}
            ).fetchone()
            
            if fund_check:
                self.db.execute(
                    sql_text("""
                        UPDATE funds
                        SET name=:name, gp_name=:gp_name, vintage_year=:vintage_year
                        WHERE id=:fund_id
                    """),
                    {
                        "name": fund_info['name'],
                        "gp_name": fund_info['gp_name'],
                        "vintage_year": fund_info['vintage_year'],
                        "fund_id": fund_id
                    }
                )
            else:
                result = self.db.execute(
                    sql_text("""
                        INSERT INTO funds (name, gp_name, vintage_year, fund_type, created_at)
                        VALUES (:name, :gp_name, :vintage_year, 'Private Equity', :created_at)
                        RETURNING id
                    """),
                    {
                        "name": fund_info['name'],
                        "gp_name": fund_info['gp_name'],
                        "vintage_year": fund_info['vintage_year'],
                        "created_at": datetime.now()
                    }
                )
                fund_id = result.fetchone()[0]
                logger.info(f"Created new fund with ID: {fund_id}")
                
                self.db.execute(
                    sql_text("UPDATE documents SET fund_id=:fund_id WHERE id=:doc_id"),
                    {"fund_id": fund_id, "doc_id": document_id}
                )

            # Parse and insert capital calls
            capital_calls = parse_capital_calls(pdf_text)
            logger.info(f"Parsed {len(capital_calls)} capital calls")
            
            for call in capital_calls:
                self.db.execute(
                    sql_text("""
                        INSERT INTO capital_calls (fund_id, call_date, call_type, amount, description, created_at)
                        VALUES (:fund_id, :call_date, :call_type, :amount, :description, :created_at)
                    """),
                    {
                        "fund_id": fund_id,
                        "call_date": call['call_date'],
                        "call_type": call['call_type'],
                        "amount": call['amount'],
                        "description": call['description'],
                        "created_at": datetime.now()
                    }
                )

            # Parse and insert distributions
            distributions = parse_distributions(pdf_text)
            logger.info(f"Parsed {len(distributions)} distributions")
            
            for dist in distributions:
                self.db.execute(
                    sql_text("""
                        INSERT INTO distributions (fund_id, distribution_date, distribution_type, amount, is_recallable, description, created_at)
                        VALUES (:fund_id, :distribution_date, :distribution_type, :amount, :is_recallable, :description, :created_at)
                    """),
                    {
                        "fund_id": fund_id,
                        "distribution_date": dist['distribution_date'],
                        "distribution_type": dist['distribution_type'],
                        "amount": dist['amount'],
                        "is_recallable": dist['is_recallable'],
                        "description": dist['description'],
                        "created_at": datetime.now()
                    }
                )

            # Parse and insert adjustments
            adjustments = parse_adjustments(pdf_text)
            logger.info(f"Parsed {len(adjustments)} adjustments")
            
            for adj in adjustments:
                self.db.execute(
                    sql_text("""
                        INSERT INTO adjustments (fund_id, adjustment_date, adjustment_type, amount, description, created_at)
                        VALUES (:fund_id, :adjustment_date, :adjustment_type, :amount, :description, :created_at)
                    """),
                    {
                        "fund_id": fund_id,
                        "adjustment_date": adj['adjustment_date'],
                        "adjustment_type": adj['adjustment_type'],
                        "amount": adj['amount'],
                        "description": adj['description'],
                        "created_at": datetime.now()
                    }
                )

            self.db.commit()
            
            summary = {
                "status": "success",
                "document_id": document_id,
                "fund_id": fund_id,
                "parsed": {
                    "capital_calls": len(capital_calls),
                    "distributions": len(distributions),
                    "adjustments": len(adjustments)
                }
            }
            logger.info(f"Processing complete: {summary}")
            return summary

        except Exception as e:
            self.db.rollback()
            logger.error(f"Document processing failed: {e}", exc_info=True)
            return {"status": "failed", "document_id": document_id, "error": str(e)}