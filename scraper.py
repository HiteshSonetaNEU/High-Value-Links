import requests
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from typing import Dict, List, Optional, Set, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LinkScraper:
    """
    A web scraper that identifies and prioritizes high-value links on a webpage.
    """
    
    def __init__(self, 
                 keywords: Optional[List[str]] = None, 
                 respect_robots_txt: bool = True,
                 max_depth: int = 2):
        """
        Initialize the link scraper with configurable options.
        
        Args:
            keywords: List of keywords to prioritize in link evaluation
            respect_robots_txt: Whether to respect robots.txt files
            max_depth: Maximum depth for crawling links
        """
        self.keywords = keywords or ["ACFR", "Budget", "Finance", "Contact", "Director", "Annual", "Report"]
        self.respect_robots_txt = respect_robots_txt
        self.max_depth = max_depth
        self.visited_urls: Set[str] = set()
        
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """
        Extract all links from the page and collect metadata.
        
        Args:
            soup: BeautifulSoup object of the page
            base_url: Base URL to resolve relative links
            
        Returns:
            List of dictionaries containing link information
        """
        links = []
        
        # Find all <a> tags with href attribute
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '').strip()
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue
                
            # Resolve relative links
            absolute_url = urljoin(base_url, href)
            
            # Extract text and surrounding context
            link_text = a_tag.get_text().strip()
            
            # Get parent's text for context
            parent_text = ""
            parent = a_tag.parent
            if parent:
                parent_text = parent.get_text().strip()
            
            links.append({
                'url': absolute_url,
                'text': link_text,
                'context': parent_text,
                'href': href,
            })
            
        return links
    
    def _calculate_relevance_score(self, link_info: Dict) -> float:
        """
        Calculate a relevance score for a link based on keywords and other factors.
        
        Args:
            link_info: Dictionary containing link information
            
        Returns:
            Relevance score as a float (0.0 to 1.0)
        """
        score = 0.0
        text = f"{link_info['text']} {link_info['context']}".lower()
        url = link_info['url'].lower()
        
        # Check for keywords in link text, context, and URL
        for keyword in self.keywords:
            keyword_lower = keyword.lower()
            if keyword_lower in text:
                score += 0.2
            if keyword_lower in url:
                score += 0.15
        
        # Check for file extensions that might indicate documents
        if re.search(r'\.(pdf|doc|docx|xls|xlsx|csv)$', url):
            score += 0.25
        
        # Prioritize contact pages
        if 'contact' in url or 'contact' in text:
            score += 0.2
            
        # Normalize score to 0.0-1.0 range
        return min(max(score, 0.0), 1.0)
    
    def scrape(self, url: str, depth: int = 0) -> List[Dict]:
        """
        Scrape a URL for high-value links.
        
        Args:
            url: The URL to scrape
            depth: Current depth in the crawling process
            
        Returns:
            List of dictionaries containing scraped links with their relevance scores
        """
        if depth > self.max_depth or url in self.visited_urls:
            return []
            
        self.visited_urls.add(url)
        logger.info(f"Scraping URL: {url} (depth: {depth})")
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Extract links from page
            all_links = self._extract_links(soup, url)
            
            # Calculate relevance score for each link
            for link in all_links:
                link['relevance_score'] = self._calculate_relevance_score(link)
            
            # Sort links by relevance score (descending)
            sorted_links = sorted(all_links, key=lambda x: x['relevance_score'], reverse=True)
            
            return sorted_links
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error scraping URL {url}: {e}")
            return []
    
    def scrape_recursively(self, start_url: str) -> List[Dict]:
        """
        Recursively scrape a URL and its high-value links up to max_depth.
        
        Args:
            start_url: The URL to start scraping from
            
        Returns:
            List of dictionaries containing scraped links with their relevance scores
        """
        all_results = []
        
        def _recursive_scrape(current_url: str, current_depth: int):
            if current_depth > self.max_depth or current_url in self.visited_urls:
                return
                
            links = self.scrape(current_url, current_depth)
            all_results.extend(links)
            
            # Follow high-value links (relevance score > 0.5)
            high_value_links = [link for link in links if link['relevance_score'] > 0.5]
            for link in high_value_links[:5]:  # Limit to top 5 high-value links
                if link['url'] not in self.visited_urls:
                    _recursive_scrape(link['url'], current_depth + 1)
        
        _recursive_scrape(start_url, 0)
        return all_results

# Example usage
if __name__ == "__main__":
    scraper = LinkScraper(keywords=["ACFR", "Budget", "Finance"])
    results = scraper.scrape("https://www.example.gov")
    
    print(f"Found {len(results)} links")
    print("Top 5 high-value links:")
    for link in sorted(results, key=lambda x: x['relevance_score'], reverse=True)[:5]:
        print(f"URL: {link['url']}")
        print(f"Text: {link['text']}")
        print(f"Relevance Score: {link['relevance_score']:.2f}")
        print("---")