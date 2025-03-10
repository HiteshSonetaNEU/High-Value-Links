from fastapi import FastAPI, Query, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, AnyHttpUrl, Field
from typing import List, Dict, Optional, Any
import uvicorn
import os
import logging
from datetime import datetime
from database import LinkDatabase
from main import ScraperManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="High-Value Link Scraper API",
    description="API for accessing and managing scraped high-value links",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Define Pydantic models for request/response validation
class LinkResponse(BaseModel):
    url: str
    text: str
    context: Optional[str] = None
    relevance_score: float
    source_url: str
    domain: Optional[str] = None
    path: Optional[str] = None
    query: Optional[str] = None
    timestamp: datetime
    llm_reason: Optional[str] = None

class ScrapeRequest(BaseModel):
    url: AnyHttpUrl
    keywords: Optional[List[str]] = None
    max_depth: Optional[int] = Field(default=2, ge=0, le=5)
    use_llm: Optional[bool] = True
    min_score_threshold: Optional[float] = Field(default=0.5, ge=0.0, le=1.0)

class ScrapingResponse(BaseModel):
    task_id: str
    status: str
    message: str

# Singleton pattern for database connection
def get_db():
    db = LinkDatabase()
    try:
        yield db
    finally:
        db.close()

# Background task for scraping
active_tasks = {}

def scrape_in_background(task_id: str, url: str, keywords: Optional[List[str]] = None, 
                         max_depth: int = 2, use_llm: bool = True, 
                         min_score_threshold: float = 0.5):
    try:
        logger.info(f"Starting background scraping task {task_id} for URL: {url}")
        logger.info(f"Task parameters - max_depth: {max_depth}, use_llm: {use_llm}, keywords: {keywords}")
        
        # Initialize the scraper manager with explicit parameters
        manager = ScraperManager(
            use_llm=use_llm,
            keywords=keywords,
            max_depth=max_depth,
            min_score_threshold=min_score_threshold
        )
        
        # Update task status and begin processing
        active_tasks[task_id] = {
            "status": "running", 
            "url": url, 
            "start_time": datetime.now(),
            "use_llm": use_llm
        }
        
        # Process the URL recursively
        link_count = manager.process_url_recursively(url)
        
        # Update task with completion details
        active_tasks[task_id] = {
            "status": "completed", 
            "url": url,
            "link_count": link_count,
            "start_time": active_tasks[task_id]["start_time"],
            "end_time": datetime.now(),
            "use_llm": use_llm
        }
        
        logger.info(f"Completed background scraping task {task_id}. Found {link_count} high-value links.")
        
    except Exception as e:
        logger.error(f"Error in background scraping task {task_id}: {str(e)}")
        logger.exception("Exception details:")
        
        active_tasks[task_id] = {
            "status": "failed",
            "url": url,
            "error": str(e),
            "start_time": active_tasks[task_id]["start_time"],
            "end_time": datetime.now(),
            "use_llm": use_llm
        }
    finally:
        if 'manager' in locals() and manager:
            manager.close()

# API Routes
@app.get("/")
async def root():
    return {"message": "Welcome to the High-Value Link Scraper API"}

@app.post("/scrape", response_model=ScrapingResponse)
async def start_scrape(
    request: ScrapeRequest,
    background_tasks: BackgroundTasks
):
    """Start a scraping task in the background"""
    task_id = f"task_{datetime.now().strftime('%Y%m%d%H%M%S')}_{id(request)}"
    
    background_tasks.add_task(
        scrape_in_background,
        task_id=task_id,
        url=str(request.url),
        keywords=request.keywords,
        max_depth=request.max_depth,
        use_llm=request.use_llm,
        min_score_threshold=request.min_score_threshold
    )
    
    return {
        "task_id": task_id,
        "status": "started",
        "message": f"Scraping task started for URL: {request.url}"
    }

@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    """Get the status of a scraping task"""
    if task_id not in active_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = active_tasks[task_id]
    
    response = {
        "task_id": task_id,
        "status": task["status"],
        "url": task["url"],
    }
    
    if "link_count" in task:
        response["link_count"] = task["link_count"]
    
    if "error" in task:
        response["error"] = task["error"]
    
    if "start_time" in task:
        response["start_time"] = task["start_time"]
    
    if "end_time" in task:
        response["end_time"] = task["end_time"]
        duration = (task["end_time"] - task["start_time"]).total_seconds()
        response["duration_seconds"] = duration
    
    return response

@app.get("/links", response_model=List[LinkResponse])
async def get_links(
    domain: Optional[str] = None,
    min_score: Optional[float] = None,
    source_url: Optional[str] = None,
    keyword: Optional[str] = None,
    sort: str = "relevance_score",
    order: str = "desc",
    limit: int = Query(default=100, le=500),
    skip: int = Query(default=0, ge=0),
    db: LinkDatabase = Depends(get_db)
):
    """Get links from the database with optional filtering"""
    # Build filter parameters
    filter_params = {}
    if domain:
        filter_params["domain"] = domain
    if min_score:
        filter_params["relevance_score"] = {"$gte": float(min_score)}
    if source_url:
        filter_params["source_url"] = source_url
    if keyword:
        # Search for keyword in text or context
        keyword_lower = keyword.lower()
        filter_params["$or"] = [
            {"text": {"$regex": keyword_lower, "$options": "i"}},
            {"context": {"$regex": keyword_lower, "$options": "i"}}
        ]
    
    # Determine sort direction
    direction = -1 if order.lower() == "desc" else 1
    
    # Get links from database
    links = db.get_links(
        filter_params=filter_params,
        sort_by=[(sort, direction)],
        limit=limit,
        skip=skip
    )
    
    return links

@app.get("/links/count")
async def count_links(
    domain: Optional[str] = None,
    min_score: Optional[float] = None,
    source_url: Optional[str] = None,
    keyword: Optional[str] = None,
    db: LinkDatabase = Depends(get_db)
):
    """Count links in the database with optional filtering"""
    # Build filter parameters
    filter_params = {}
    if domain:
        filter_params["domain"] = domain
    if min_score:
        filter_params["relevance_score"] = {"$gte": float(min_score)}
    if source_url:
        filter_params["source_url"] = source_url
    if keyword:
        # Search for keyword in text or context
        keyword_lower = keyword.lower()
        filter_params["$or"] = [
            {"text": {"$regex": keyword_lower, "$options": "i"}},
            {"context": {"$regex": keyword_lower, "$options": "i"}}
        ]
    
    # Get count from database
    count = db.get_link_count(filter_params=filter_params)
    
    return {"count": count}

@app.get("/domains")
async def get_domains(
    min_score: Optional[float] = None,
    db: LinkDatabase = Depends(get_db)
):
    """Get list of unique domains in the database"""
    try:
        filter_params = {}
        if min_score:
            filter_params["relevance_score"] = {"$gte": float(min_score)}
        
        # Get all links first
        links = db.get_links(filter_params=filter_params, limit=99999)
        
        # Process domains in memory
        domain_counts = {}
        for link in links:
            domain = link.get("domain")
            if domain:
                domain_counts[domain] = domain_counts.get(domain, 0) + 1
        
        # Format response
        domains = [{"domain": domain, "count": count} 
                  for domain, count in domain_counts.items()]
        domains.sort(key=lambda x: x["count"], reverse=True)
        
        return domains
        
    except Exception as e:
        logger.error(f"Error getting domains: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving domains: {str(e)}"
        )

# Run the application
if __name__ == "__main__":
    # Get port from environment variable or use default
    port = int(os.environ.get("PORT", 8000))
    
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)