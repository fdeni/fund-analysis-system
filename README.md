# Fund Performance Analysis System

## Overview

Build an **AI-powered fund performance analysis system** that enables Limited Partners (LPs) to:
1. Upload fund performance PDF documents
2. Automatically parse and extract structured data (tables → SQL, text → Vector DB)
3. Ask natural language questions about fund metrics (DPI, IRR, etc.)
4. Get accurate answers powered by RAG (Retrieval Augmented Generation) and SQL calculations

---

## Business Context

As an LP, you receive quarterly fund performance reports in PDF format. These documents contain:
- **Capital Call tables**: When and how much capital was called
- **Distribution tables**: When and how much was distributed back to LPs
- **Adjustment tables**: Rebalancing entries (recallable distributions, capital call adjustments)
- **Text explanations**: Definitions, investment strategies, market commentary
---

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Frontend (Next.js)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Upload     │  │     Chat     │  │   Dashboard  │     │
│  │     Page     │  │  Interface   │  │     Page     │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────────┬────────────────────────────────────┘
                         │ REST API
┌────────────────────────▼────────────────────────────────────┐
│                    Backend (FastAPI)                        │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Document Processor                     │   │
│  │  ┌──────────────┐         ┌──────────────┐        │   │
│  │  │   Docling    │────────▶│  Table       │        │   │
│  │  │   Parser     │         │  Extractor   │        │   │
│  │  └──────────────┘         └──────┬───────┘        │   │
│  │                                   │                 │   │
│  │  ┌──────────────┐         ┌──────▼───────┐        │   │
│  │  │   Text       │────────▶│  Embedding   │        │   │
│  │  │   Chunker    │         │  Generator   │        │   │
│  │  └──────────────┘         └──────────────┘        │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Query Engine (RAG)                     │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │   │
│  │  │   Intent     │─▶│   Vector     │─▶│   LLM    │ │   │
│  │  │  Classifier  │  │   Search     │  │ Response │ │   │
│  │  └──────────────┘  └──────────────┘  └──────────┘ │   │
│  │                                                      │   │
│  │  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │  Metrics     │─▶│     SQL      │               │   │
│  │  │ Calculator   │  │   Queries    │               │   │
│  │  └──────────────┘  └──────────────┘               │   │
│  └─────────────────────────────────────────────────────┘   │
└────────────────────────┬────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
┌───────▼────────┐ ┌────▼─────┐ ┌────────▼────────┐
│   PostgreSQL   │ │  FAISS   │ │     Redis       │
│  (Structured)  │ │ (Vectors)│ │  (Task Queue)   │
└────────────────┘ └──────────┘ └─────────────────┘
```

---

## Data Model

### PostgreSQL Schema

#### `funds` table
```sql
CREATE TABLE funds (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gp_name VARCHAR(255),
    fund_type VARCHAR(100),
    vintage_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `capital_calls` table
```sql
CREATE TABLE capital_calls (
    id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES funds(id),
    call_date DATE NOT NULL,
    call_type VARCHAR(100),
    amount DECIMAL(15, 2) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `distributions` table
```sql
CREATE TABLE distributions (
    id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES funds(id),
    distribution_date DATE NOT NULL,
    distribution_type VARCHAR(100),
    is_recallable BOOLEAN DEFAULT FALSE,
    amount DECIMAL(15, 2) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `adjustments` table
```sql
CREATE TABLE adjustments (
    id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES funds(id),
    adjustment_date DATE NOT NULL,
    adjustment_type VARCHAR(100),
    category VARCHAR(100),
    amount DECIMAL(15, 2) NOT NULL,
    is_contribution_adjustment BOOLEAN DEFAULT FALSE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### `documents` table
```sql
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    fund_id INTEGER REFERENCES funds(id),
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(500),
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    parsing_status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT
);
```

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Node.js 18+ (for local frontend development)
- Python 3.11+ (for local backend development)
- Ollama (For local LLM)

### Quick Start

1. **Clone the repository**
```bash
git clone <your-repo-url>
cd fund-analysis-system
```

2. **Set up environment variables**
```bash
# Copy example env file
cp .env.example .env

# Edit .env and add your API keys
# LLM_PROVIDER=ollama
# OLLAMA_BASE_URL=http://host.docker.internal:11434
# OLLAMA_MODEL=llama3.2
# DATABASE_URL=postgresql://user:password@localhost:5432/funddb
```

3. **Start with Docker Compose**
```bash
docker-compose up -d
```

4. **Access the application**
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs

5. **Upload sample document**
- Navigate to http://localhost:3000/upload
- Upload the provided PDF: `files/ILPA based Capital Accounting and Performance Metrics_ PIC, Net PIC, DPI, IRR  .pdf`
- Wait for parsing to complete

6. **Start asking questions**
- Go to http://localhost:3000/chat
- Try: "What is DPI?"
- Try: "Calculate the current DPI for this fund"

---

## Project Structure

```
fund-analysis-system/
├── backend/
│   ├── app/
│   │   ├── api/
│   │   │   ├── endpoints/
│   │   │   │   ├── documents.py
│   │   │   │   ├── funds.py
│   │   │   │   ├── chat.py
│   │   │   │   └── metrics.py
│   │   │   └── deps.py
│   │   ├── core/
│   │   │   ├── config.py
│   │   │   └── security.py
│   │   ├── db/
│   │   │   ├── base.py
│   │   │   ├── session.py
│   │   │   └── init_db.py
│   │   ├── models/
│   │   │   ├── fund.py
│   │   │   ├── transaction.py
│   │   │   └── document.py
│   │   ├── schemas/
│   │   │   ├── fund.py
│   │   │   ├── transaction.py
│   │   │   ├── document.py
│   │   │   └── chat.py
│   │   ├── services/
│   │   │   ├── document_processor.py
│   │   │   ├── table_parser.py
│   │   │   ├── vector_store.py
│   │   │   ├── query_engine.py
│   │   │   └── metrics_calculator.py
│   │   └── main.py
│   ├── test/
│   │   ├── test_document_processor.py
│   │   ├── test_matrics_calculator.py
│   │   └── test_query_engine.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── alembic/
│       └── versions/
├── frontend/
│   ├── app/
│   │   ├── layout.tsx
│   │   ├── page.tsx
│   │   ├── upload/
│   │   │   └── page.tsx
│   │   ├── chat/
│   │   │   └── page.tsx
│   │   └── funds/
│   │       ├── page.tsx
│   │       └── [id]/
│   │           └── page.tsx
│   ├── components/
│   │   ├── ui/
│   │   │   ├── button.tsx
│   │   │   ├── card.tsx
│   │   │   ├── input.tsx
│   │   │   └── ...
│   │   ├── FileUpload.tsx
│   │   ├── ChatInterface.tsx
│   │   ├── FundMetrics.tsx
│   │   └── TransactionTable.tsx
│   ├── lib/
│   │   ├── api.ts
│   │   └── utils.ts
│   ├── package.json
│   ├── tsconfig.json
│   ├── next.config.js
│   ├── tailwind.config.ts
│   └── Dockerfile
├── docker-compose.yml
├── .env.example
├── README.md
└── docs/
    ├── API.md
    ├── ARCHITECTURE.md
    └── CALCULATIONS.md
```

---

## API Endpoints

### Documents
```
POST   /api/documents/upload
GET    /api/documents/{doc_id}/status
GET    /api/documents/{doc_id}
DELETE /api/documents/{doc_id}
```

### Funds
```
GET    /api/funds
POST   /api/funds
GET    /api/funds/{fund_id}
GET    /api/funds/{fund_id}/transactions
GET    /api/funds/{fund_id}/metrics
```

### Chat
```
POST   /api/chat/query
GET    /api/chat/conversations/{conv_id}
POST   /api/chat/conversations
```

See [API.md](docs/API.md) for detailed documentation.

---

## Fund Metrics Formulas

### Paid-In Capital (PIC)
```
PIC = Total Capital Calls - Adjustments
```

### DPI (Distribution to Paid-In)
```
DPI = Cumulative Distributions / PIC
```

### IRR (Internal Rate of Return)
```
IRR = Rate where NPV of all cash flows = 0
Uses numpy-financial.irr() function
```

See [CALCULATIONS.md](docs/CALCULATIONS.md) for detailed formulas.

---

## Testing

### Run Backend Tests
```bash
cd backend
pytest tests/ -v --cov=app
```

### Run Frontend Tests
```bash
cd frontend
npm test
```

### Test Document Upload
```bash
curl -X POST "http://localhost:8000/api/documents/upload" \
  -F "file=@files/sample_fund_report.pdf"
```

### Test Chat Query
```bash
curl -X POST "http://localhost:8000/api/chat/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is the current DPI?",
    "fund_id": 1
  }'
```

---

## Features Implemented : 
1. Upload PDF fund performance reports
2. Auto-parse capital call, distribution, adjustment tables
3. Store structured data in PostgreSQL
4. Store text in Vector DB (FAISS / pgvector)
5. RAG-based question answering via LLMs
6. Accurate DPI / IRR / PIC calculations
7. Dockerized multi-service setup

---

---

## Known Limitations
1. Parsing accuracy may drop for PDFs with complex table layouts
2. IRR calculations assume accurate date ordering of cash flows
3. Ollama local inference may be slower on lower-end machines
4. RAG responses depend on embedding quality

---

---

## Future Improvements
1. Benchmarking against ILPA standards
2. Advanced analytics dashboard with fund comparisons
3. Time-series visualization for cash flow trends
4. Cloud deployment (Render, AWS, or GCP)
5. User authentication and multi-LP support
6. More robust parsing for scanned PDFs (OCR integration)
7. Unit test coverage

---

## Screenshots
1. Upload Page	

![alt text](image.png)

2. Chat Interface	

![alt text](image-2.png)

3. Fund Dashboard

![alt text](image-1.png)

---

## Sample Questions to Test

### Definitions
- "What does DPI mean?"
- "Explain Paid-In Capital"
- "What is a recallable distribution?"

### Calculations
- "What is the current DPI?"
- "Calculate the IRR for this fund"
- "Has the fund returned all capital to LPs?"

### Data Retrieval
- "Show me all capital calls in 2024"
- "What was the largest distribution?"
- "List all adjustments"

### Complex Queries
- "How is the fund performing compared to industry benchmarks?"
- "What percentage of distributions were recallable?"
- "Explain the trend in capital calls over time"

---

---

## Tech Stack

### Backend
- **Framework**: FastAPI (Python 3.11+)
- **Document Parser**: Docling
- **Vector DB**: pgvector (PostgreSQL extension)
- **SQL DB**: PostgreSQL 15+
- **ORM**: SQLAlchemy
- **LLM Framework**: LangChain
- **LLM**: Ollama 3.2
- **Embeddings**: sentence-transformers/all-MiniLM-L6-v2
- **Task Queue**: Celery + Redis

### Frontend
- **Framework**: Next.js 14 (App Router)
- **UI Library**: shadcn/ui + Tailwind CSS
- **State**: Zustand or React Context
- **Data Fetching**: TanStack Query
- **Charts**: Recharts
- **File Upload**: react-dropzone

### Infrastructure
- **Development**: Docker + Docker Compose

---

## Setup Ollama for This Project

### Ollama (Recommended for Development)

**Completely free, runs locally on your machine**

1. **Install Ollama**
```bash
# Mac
brew install ollama

# Linux
curl -fsSL https://ollama.com/install.sh | sh

# Windows
# Download from https://ollama.com/download
```

2. **Download a model**
```bash
# Llama 3.2 (3B - fast, good for development)
ollama pull llama3.2

# Or Llama 3.1 (8B - better quality)
ollama pull llama3.1

# Or Mistral (7B - good balance)
ollama pull mistral
```

3. **Update your .env**
```bash
# Use Ollama instead of OpenAI
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://host.docker.internal:11434 # to connect that backend running in docker
OLLAMA_MODEL=llama3.2
```

4. **Modify your code to use Ollama**
```python
# In backend/app/services/query_engine.py
from langchain_community.llms import Ollama

llm = Ollama(
    base_url="http://host.docker.internal:11434",
    model="llama3.2"
)
```

**Pros**: Free, private, no API limits, works offline
**Cons**: Requires decent hardware (8GB+ RAM), slower than cloud APIs

---

## Sample Data

### Provided Sample Files

Located in `files/` directory:

1. **`ILPA based Capital Accounting and Performance Metrics_ PIC, Net PIC, DPI, IRR.pdf`**
   - Reference document explaining fund metrics
   - Contains definitions of PIC, DPI, IRR, TVPI
   - Use this to test text extraction and RAG

### Sample Data You Should Create

For comprehensive testing, you should create **mock fund performance reports** with:

#### Example Capital Call Table
```
Date       | Call Number | Amount      | Description
-----------|-------------|-------------|------------------
2023-01-15 | Call 1      | $5,000,000  | Initial Capital
2023-06-20 | Call 2      | $3,000,000  | Follow-on
2024-03-10 | Call 3      | $2,000,000  | Bridge Round
```

#### Example Distribution Table
```
Date       | Type        | Amount      | Recallable | Description
-----------|-------------|-------------|------------|------------------
2023-12-15 | Return      | $1,500,000  | No         | Exit: Company A
2024-06-20 | Income      | $500,000    | No         | Dividend
2024-09-10 | Return      | $2,000,000  | Yes        | Partial Exit: Company B
```

#### Example Adjustment Table
```
Date       | Type                | Amount    | Description
-----------|---------------------|-----------|------------------
2024-01-15 | Recallable Dist     | -$500,000 | Recalled distribution
2024-03-20 | Capital Call Adj    | $100,000  | Fee adjustment
```

### Expected Test Results

For the sample data above:
- **Total Capital Called**: $10,000,000
- **Total Distributions**: $4,000,000
- **Net PIC**: $10,100,000 (after adjustments)
- **DPI**: 0.40 (4M / 10M)
- **IRR**: ~8-12% (depends on exact dates)

### Creating Test PDFs

#### Use Provided Script (Recommended)

We've included a Python script to generate sample PDFs:

```bash
cd files/
pip install reportlab
python create_sample_pdf.py
```

This creates `Sample_Fund_Performance_Report.pdf` with:
- Capital calls table (4 entries)
- Distributions table (4 entries)
- Adjustments table (3 entries)
- Performance summary with definitions

---

## Reference Materials

- **Docling**: https://github.com/DS4SD/docling
- **LangChain RAG**: https://python.langchain.com/docs/use_cases/question_answering/
- **FAISS**: https://faiss.ai/
- **ILPA Guidelines**: https://ilpa.org/
- **PE Metrics**: https://www.investopedia.com/terms/d/dpi.asp

---
