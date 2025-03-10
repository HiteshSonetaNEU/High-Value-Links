import requests
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import time
import random
from typing import Dict, List, Optional, Set, Tuple
import http.client
import urllib3
import ssl

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
        """
        self.keywords = keywords or ["ACFR", "Budget", "Finance", "Contact", "Director", "Annual", "Report"]
        self.respect_robots_txt = respect_robots_txt
        self.max_depth = max_depth
        self.visited_urls: Set[str] = set()
        
        # List of user agents to rotate through
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/122.0.0.0"
        ]

    def _get_page_content(self, url: str) -> Optional[str]:
        """
        Get the HTML content from a URL using multiple methods.
        
        Args:
            url: URL to fetch
            
        Returns:
            HTML content as string or None if all methods fail
        """
        # Method 1: Standard requests with custom headers
        html_content = self._try_requests_with_headers(url)
        if html_content:
            return html_content
            
        # Method 2: Low-level urllib3 request
        html_content = self._try_urllib3(url)
        if html_content:
            return html_content
            
        # Method 3: http.client connection (more basic, may bypass some blocks)
        html_content = self._try_http_client(url)
        if html_content:
            return html_content
                
        logger.error(f"All methods failed to fetch {url}")
        return None
        
    def _try_requests_with_headers(self, url: str) -> Optional[str]:
        """Try fetching with requests library using various headers"""
        try:
            user_agent = random.choice(self.user_agents)
            logger.info(f"Fetching URL with requests: {url}")
            
            headers = {
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "max-age=0",
                "Referer": "https://www.google.com/",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "cross-site",
                "Sec-Fetch-User": "?1"
            }
            
            response = requests.get(
                url, 
                timeout=15,
                headers=headers,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                logger.info(f"Successfully fetched with requests: {url}")
                return response.text
                
            logger.warning(f"Failed to fetch with requests: {url}, status: {response.status_code}")
            return None
                
        except requests.RequestException as e:
            logger.warning(f"Request exception for {url}: {e}")
            return None
            
    def _try_urllib3(self, url: str) -> Optional[str]:
        """Try fetching with urllib3 directly"""
        try:
            user_agent = random.choice(self.user_agents)
            logger.info(f"Fetching URL with urllib3: {url}")
            
            # Create a pool manager with SSL verification disabled
            http = urllib3.PoolManager(
                timeout=15.0,
                retries=urllib3.Retry(3, redirect=5),
                cert_reqs='CERT_NONE'
            )
            
            headers = {
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            
            response = http.request("GET", url, headers=headers)
            
            if response.status == 200:
                logger.info(f"Successfully fetched with urllib3: {url}")
                return response.data.decode('utf-8', errors='replace')
                
            logger.warning(f"Failed to fetch with urllib3: {url}, status: {response.status}")
            return None
                
        except Exception as e:
            logger.warning(f"Urllib3 exception for {url}: {e}")
            return None
            
    def _try_http_client(self, url: str) -> Optional[str]:
        """Try fetching with http.client (most basic method)"""
        try:
            parsed_url = urlparse(url)
            protocol = parsed_url.scheme
            hostname = parsed_url.netloc
            path = parsed_url.path
            if not path:
                path = "/"
            if parsed_url.query:
                path += "?" + parsed_url.query
                
            logger.info(f"Fetching URL with http.client: {url}")
            
            user_agent = random.choice(self.user_agents)
            
            if protocol == 'https':
                # Create an unverified HTTPS context
                context = ssl._create_unverified_context()
                conn = http.client.HTTPSConnection(hostname, timeout=15, context=context)
            else:
                conn = http.client.HTTPConnection(hostname, timeout=15)
                
            headers = {
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5"
            }
            
            conn.request("GET", path, headers=headers)
            response = conn.getresponse()
            
            if response.status == 200:
                logger.info(f"Successfully fetched with http.client: {url}")
                return response.read().decode('utf-8', errors='replace')
                
            logger.warning(f"Failed to fetch with http.client: {url}, status: {response.status}")
            return None
                
        except Exception as e:
            logger.warning(f"http.client exception for {url}: {e}")
            return None

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
        
        # Get page content using multiple methods
        html_content = self._get_page_content(url)
        if not html_content:
            logger.error(f"Failed to get content from {url}")
            return []
            
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Extract links from page
            all_links = self._extract_links(soup, url)
            
            # Calculate relevance score for each link
            for link in all_links:
                link['relevance_score'] = self._calculate_relevance_score(link)
            
            # Sort links by relevance score (descending)
            sorted_links = sorted(all_links, key=lambda x: x['relevance_score'], reverse=True)
            
            return sorted_links
            
        except Exception as e:
            logger.error(f"Error processing content from {url}: {e}")
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