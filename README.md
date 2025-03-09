# High-Value Link Scraper with API

A web scraper that identifies high-value links on webpages, focusing on extracting relevant contacts and specific files (like "ACFR," "Budget," or related terms). The scraper uses a rule-based system and an optional LLM-powered classifier to prioritize relevant links. The project includes a REST API built with FastAPI to access the scraped data.

## Features

- **Intelligent Link Scraping**: Extracts and prioritizes links based on relevance to specified keywords
- **Machine Learning Classification**: Optional OpenAI-powered link classification for improved relevance scoring
- **MongoDB Integration**: Stores scraped links with metadata in MongoDB Atlas
- **REST API**: Provides endpoints for triggering scrapes and accessing the data
- **Scalability Considerations**: Designed with performance and scale in mind

## Architecture

The project is organized into several modular components:

1. **Link Scraper (`scraper.py`)**: Core web scraping functionality
2. **LLM Classifier (`llm_classifier.py`)**: OpenAI-powered link classifier
3. **Database Layer (`database.py`)**: MongoDB storage interface with fallback to in-memory storage
4. **API Layer (`api.py`)**: FastAPI-based REST API
5. **Main Integration (`main.py`)**: CLI tool that ties everything together

## Setup

### Prerequisites

- Python 3.8 or higher
- MongoDB Atlas account (or a local MongoDB instance)
- OpenAI API key (optional, for enhanced link classification)

### Installation

1. Clone the repository
```bash
git clone https://github.com/your-username/high-value-link-scraper.git
cd high-value-link-scraper
```

2. Create a virtual environment and activate it
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python -m venv venv
source venv/bin/activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Configure environment variables
```bash
# Copy the example .env file and update with your credentials
cp .env.example .env
```

5. Edit the `.env` file with your MongoDB URI and OpenAI API key

## Usage

### Command Line Interface

Run the scraper from the command line:

```bash
python main.py https://example.gov --keywords "ACFR,Budget,Finance" --max-depth 2
```

Options:
- `--no-llm`: Disable LLM classifier
- `--keywords`: Comma-separated list of keywords (default: "ACFR,Budget,Finance,Contact,Director,Annual,Report")
- `--max-depth`: Maximum depth for crawling (default: 2)
- `--min-score`: Minimum relevance score threshold (default: 0.5)
- `--max-links`: Maximum links to process per page (default: 100)

### API

Start the API server:

```bash
uvicorn api:app --reload
```

Or use:

```bash
python api.py
```

Access the API documentation at: http://localhost:8000/docs

#### API Endpoints

- `POST /scrape`: Start a scraping task
- `GET /tasks/{task_id}`: Get the status of a scraping task
- `GET /links`: Get links from the database with filtering options
- `GET /links/count`: Count links that match specified filters
- `GET /domains`: Get list of unique domains in the database

## Link Classification

The scraper prioritizes links using two methods:

1. **Rule-based Classification**: Basic scoring using keyword matching and heuristics
2. **LLM-based Classification**: Optional OpenAI-powered classification for improved accuracy

The rule-based approach looks for:
- Keywords in link text and context
- File extensions indicating documents (.pdf, .doc, etc.)
- Contact page indicators
- URL patterns suggesting high-value content

The LLM classifier analyzes:
- Deeper semantic meaning of link text
- Surrounding context and its relation to the target content
- Likelihood of links leading to financial documents or contact information

## Scaling Considerations

This project is designed with scalability in mind:

- MongoDB for efficient data storage and retrieval
- Connection pooling for database access
- Query optimization with indexes
- Batching for API calls to reduce rate limiting impacts
- Background task processing for long-running scrapes
- In-memory fallbacks for edge cases

## License

[MIT License](LICENSE)

## Contact

Your Name - your.email@example.com