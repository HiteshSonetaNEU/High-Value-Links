import os
import time
import logging
import argparse
from typing import List, Dict, Optional
from datetime import datetime
from urllib.parse import urlparse
import traceback

from scraper import LinkScraper
from llm_classifier import LLMClassifier
from database import LinkDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class ScraperManager:
    """
    Manager class that integrates the scraper, classifier, and database components.
    """
    
    def __init__(self, 
                 use_llm: bool = True, 
                 keywords: Optional[List[str]] = None,
                 max_depth: int = 2,
                 min_score_threshold: float = 0.5,
                 max_links_per_page: int = 100):
        """
        Initialize the scraper manager.
        
        Args:
            use_llm: Whether to use the LLM classifier
            keywords: List of keywords to prioritize
            max_depth: Maximum depth for crawling
            min_score_threshold: Minimum relevance score threshold for saving links
            max_links_per_page: Maximum number of links to process per page
        """
        self.use_llm = use_llm
        self.keywords = keywords or ["ACFR", "Budget", "Finance", "Contact", "Director", "Annual", "Report"]
        self.max_depth = max_depth
        self.min_score_threshold = min_score_threshold
        self.max_links_per_page = max_links_per_page
        
        # Initialize components
        self.scraper = LinkScraper(keywords=self.keywords, max_depth=self.max_depth)
        self.classifier = LLMClassifier() if use_llm else None
        self.db = LinkDatabase()
        
        logger.info(f"Initialized ScraperManager with keywords: {self.keywords}")
        logger.info(f"Using LLM classifier: {self.use_llm}")
    
    def process_url(self, url: str, depth: int = 0) -> List[Dict]:
        """
        Process a single URL to extract, classify, and store high-value links.
        
        Args:
            url: The URL to process
            depth: Current depth in the crawling process
            
        Returns:
            List of high-value links
        """
        logger.info(f"Processing URL: {url} (depth: {depth})")
        
        try:
            # Step 1: Scrape links from the URL
            links = self.scraper.scrape(url, depth)
            
            # Limit the number of links to process
            if len(links) > self.max_links_per_page:
                logger.info(f"Limiting from {len(links)} to {self.max_links_per_page} links")
                links = links[:self.max_links_per_page]
            
            # Step 2: Use LLM to improve relevance scoring if enabled
            if self.use_llm and self.classifier and links:
                links = self.classifier.classify_links(links, self.keywords)
            
            # Step 3: Filter links by relevance score threshold
            high_value_links = [link for link in links if link.get('relevance_score', 0) >= self.min_score_threshold]
            
            # Step 4: Add timestamp and parsed components
            timestamp = datetime.now()
            for link in high_value_links:
                link['timestamp'] = timestamp
                parsed_url = urlparse(link['url'])
                link['domain'] = parsed_url.netloc
                link['path'] = parsed_url.path
                link['query'] = parsed_url.query
            
            # Step 5: Store links in database
            if high_value_links:
                saved_count = self.db.save_links(high_value_links, url)
                logger.info(f"Saved {saved_count} high-value links to database")
            
            return high_value_links
            
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def process_url_recursively(self, start_url: str) -> int:
        """
        Process a URL and its high-value links recursively.
        
        Args:
            start_url: The URL to start processing from
            
        Returns:
            Total number of high-value links found
        """
        logger.info(f"Starting recursive processing from URL: {start_url}")
        start_time = time.time()
        
        processed_urls = set()
        total_high_value_links = 0
        
        def _recursive_process(current_url: str, current_depth: int):
            nonlocal total_high_value_links
            
            if current_depth > self.max_depth or current_url in processed_urls:
                return
            
            processed_urls.add(current_url)
            high_value_links = self.process_url(current_url, current_depth)
            total_high_value_links += len(high_value_links)
            
            # Follow high-value links that exceed a higher threshold
            follow_threshold = max(0.7, self.min_score_threshold + 0.1)  # Adjust threshold for following
            links_to_follow = [link for link in high_value_links if link.get('relevance_score', 0) >= follow_threshold]
            
            # Limit the number of links to follow at each level
            max_follow = 5
            if len(links_to_follow) > max_follow:
                links_to_follow = links_to_follow[:max_follow]
            
            for link in links_to_follow:
                _recursive_process(link['url'], current_depth + 1)
        
        _recursive_process(start_url, 0)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Recursive processing completed in {elapsed_time:.2f} seconds")
        logger.info(f"Processed {len(processed_urls)} URLs, found {total_high_value_links} high-value links")
        
        return total_high_value_links
    
    def close(self):
        """Close all resources"""
        if self.db:
            self.db.close()


def main():
    parser = argparse.ArgumentParser(description='High-Value Link Scraper')
    parser.add_argument('url', help='The URL to start scraping from')
    parser.add_argument('--no-llm', action='store_true', help='Disable LLM classifier')
    parser.add_argument('--keywords', type=str, help='Comma-separated list of keywords to prioritize')
    parser.add_argument('--max-depth', type=int, default=2, help='Maximum depth for crawling')
    parser.add_argument('--min-score', type=float, default=0.5, help='Minimum relevance score threshold')
    parser.add_argument('--max-links', type=int, default=100, help='Maximum links to process per page')
    
    args = parser.parse_args()
    
    # Parse keywords if provided
    keywords = None
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(',') if k.strip()]
    
    # Create and run the scraper manager
    manager = ScraperManager(
        use_llm=not args.no_llm,
        keywords=keywords,
        max_depth=args.max_depth,
        min_score_threshold=args.min_score,
        max_links_per_page=args.max_links
    )
    
    try:
        total_links = manager.process_url_recursively(args.url)
        print(f"\nScraping completed! Found {total_links} high-value links.")
        print("Check the database for the results.")
    except KeyboardInterrupt:
        print("\nScraping interrupted by user.")
    finally:
        manager.close()


if __name__ == "__main__":
    main()