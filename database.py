import os
import logging
from typing import Dict, List, Optional, Union
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, PyMongoError, ServerSelectionTimeoutError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LinkDatabase:
    """
    A database interface for storing and retrieving scraped links.
    """
    
    def __init__(self, 
                connection_string: Optional[str] = None, 
                db_name: str = "link_scraper", 
                collection_name: str = "links"):
        """
        Initialize the link database.
        """
        # Try to get connection string, with explicit MongoDB Atlas check
        self.connection_string = connection_string or os.getenv("MONGODB_URI")
        logger.info("Initializing database connection...")
        
        if not self.connection_string:
            logger.error("No MongoDB connection string provided")
            self._setup_in_memory()
            return
            
        # Log connection attempt (with sanitized string)
        sanitized_uri = self.connection_string.split('@')[-1] if '@' in self.connection_string else 'localhost'
        logger.info(f"Attempting to connect to MongoDB at: {sanitized_uri}")
        
        if self.connection_string.startswith('mongodb+srv://'):
            try:
                # Configure client with explicit timeouts and settings for Atlas
                self.client = MongoClient(
                    self.connection_string,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=5000,
                    retryWrites=True,
                    retryReads=True
                )
                
                # Force a connection to verify it works
                self.client.admin.command('ping')
                logger.info("Successfully connected to MongoDB Atlas")
                
                # Set up database and collection
                self.db = self.client[db_name]
                self.collection = self.db[collection_name]
                
                # Test collection access
                self.collection.find_one()
                logger.info(f"Successfully accessed collection: {collection_name}")
                
                # Create indexes
                self._create_indexes()
                
            except Exception as e:
                logger.error(f"Failed to connect to MongoDB Atlas: {str(e)}")
                logger.error("Connection error details:", exc_info=True)
                self._setup_in_memory()
        else:
            logger.warning("Not using MongoDB Atlas connection string")
            self._setup_in_memory()
    
    def _setup_in_memory(self):
        """Set up in-memory storage when MongoDB connection fails"""
        self.client = None
        self.db = None
        self.collection = None
        self._in_memory_links = []
        logger.warning("Falling back to in-memory storage")
    
    def _create_indexes(self):
        """Create database indexes for optimized querying"""
        if self.collection is not None:
            self.collection.create_index("url", unique=True)
            self.collection.create_index("relevance_score")
            self.collection.create_index("source_url")
            self.collection.create_index("timestamp")
    
    def save_links(self, links: List[Dict], source_url: str) -> int:
        """
        Save links to the database.
        
        Args:
            links: List of link dictionaries to save
            source_url: URL of the page these links were scraped from
            
        Returns:
            Number of links successfully saved
        """
        if not links:
            return 0
            
        # Add source URL to each link
        for link in links:
            link["source_url"] = source_url
            
        if self.collection is not None:
            try:
                # Create proper UpdateOne operations for bulk write
                from pymongo import UpdateOne
                operations = [
                    UpdateOne(
                        {"url": link["url"]},
                        {"$set": link},
                        upsert=True
                    ) for link in links
                ]
                
                # Perform bulk write
                if operations:
                    result = self.collection.bulk_write(operations)
                    return result.upserted_count + result.modified_count
                return 0
                
            except PyMongoError as e:
                logger.error(f"Error saving links to MongoDB: {e}")
                # Fall back to in-memory storage
                self._in_memory_links.extend(links)
                return len(links)
        else:
            # In-memory storage
            self._in_memory_links.extend(links)
            return len(links)
    
    def get_links(self, 
                 filter_params: Optional[Dict] = None, 
                 sort_by: Optional[List[tuple]] = None,
                 limit: int = 100,
                 skip: int = 0) -> List[Dict]:
        """
        Retrieve links from the database with filtering, sorting, and pagination.
        
        Args:
            filter_params: Dictionary of filter parameters
            sort_by: List of (field, direction) tuples for sorting
            limit: Maximum number of links to return
            skip: Number of links to skip (for pagination)
            
        Returns:
            List of link dictionaries
        """
        filter_params = filter_params or {}
        sort_by = sort_by or [("relevance_score", -1)]  # Default sort by relevance desc
        
        if self.collection is not None:
            try:
                # Convert sort tuples to MongoDB format
                mongo_sort = [(field, direction) for field, direction in sort_by]
                
                # Execute query with pagination
                cursor = self.collection.find(
                    filter=filter_params
                ).sort(mongo_sort).skip(skip).limit(limit)
                
                # Convert to list and return
                return list(cursor)
                
            except PyMongoError as e:
                logger.error(f"Error retrieving links from MongoDB: {e}")
                # Fall back to in-memory filtering
                return self._filter_in_memory_links(filter_params, sort_by, limit, skip)
        else:
            # In-memory filtering
            return self._filter_in_memory_links(filter_params, sort_by, limit, skip)
    
    def _filter_in_memory_links(self, 
                              filter_params: Dict, 
                              sort_by: List[tuple],
                              limit: int,
                              skip: int) -> List[Dict]:
        """
        Filter and sort in-memory links when database is unavailable.
        
        Args:
            filter_params: Dictionary of filter parameters
            sort_by: List of (field, direction) tuples for sorting
            limit: Maximum number of links to return
            skip: Number of links to skip
            
        Returns:
            Filtered and sorted list of link dictionaries
        """
        # Simple filtering
        filtered_links = self._in_memory_links
        for key, value in filter_params.items():
            if isinstance(value, dict):
                # Handle special operators like $gt, $lt, etc.
                for op, op_value in value.items():
                    if op == "$gt":
                        filtered_links = [link for link in filtered_links if key in link and link[key] > op_value]
                    elif op == "$gte":
                        filtered_links = [link for link in filtered_links if key in link and link[key] >= op_value]
                    elif op == "$lt":
                        filtered_links = [link for link in filtered_links if key in link and link[key] < op_value]
                    elif op == "$lte":
                        filtered_links = [link for link in filtered_links if key in link and link[key] <= op_value]
            else:
                # Exact match
                filtered_links = [link for link in filtered_links if key in link and link[key] == value]
        
        # Sorting (handle multiple sort fields)
        for field, direction in reversed(sort_by):  # Reverse to apply first sort field last
            filtered_links.sort(
                key=lambda x: x.get(field, 0) if field in x else 0,
                reverse=(direction == -1)
            )
        
        # Apply pagination
        return filtered_links[skip:skip + limit]
    
    def get_link_count(self, filter_params: Optional[Dict] = None) -> int:
        """
        Get count of links matching the filter.
        
        Args:
            filter_params: Dictionary of filter parameters
            
        Returns:
            Count of matching links
        """
        filter_params = filter_params or {}
        
        if self.collection is not None:
            try:
                return self.collection.count_documents(filter_params)
            except PyMongoError as e:
                logger.error(f"Error counting links in MongoDB: {e}")
                # Fall back to in-memory counting
                return len(self._filter_in_memory_links(filter_params, [], 999999, 0))
        else:
            # In-memory counting
            return len(self._filter_in_memory_links(filter_params, [], 999999, 0))
    
    def delete_links(self, filter_params: Dict) -> int:
        """
        Delete links matching the filter.
        
        Args:
            filter_params: Dictionary of filter parameters for deletion
            
        Returns:
            Number of links deleted
        """
        if not filter_params:
            # Prevent accidental deletion of all links
            return 0
            
        if self.collection is not None:
            try:
                result = self.collection.delete_many(filter_params)
                return result.deleted_count
            except PyMongoError as e:
                logger.error(f"Error deleting links from MongoDB: {e}")
                return 0
        else:
            # In-memory deletion
            original_count = len(self._in_memory_links)
            self._in_memory_links = self._filter_out_in_memory_links(filter_params)
            return original_count - len(self._in_memory_links)
    
    def _filter_out_in_memory_links(self, filter_params: Dict) -> List[Dict]:
        """
        Filter out links that match the filter (inverse of filtering in).
        
        Args:
            filter_params: Dictionary of filter parameters
            
        Returns:
            List of links that don't match the filter
        """
        result = self._in_memory_links.copy()
        
        for key, value in filter_params.items():
            if isinstance(value, dict):
                # Handle special operators
                for op, op_value in value.items():
                    if op == "$gt":
                        result = [link for link in result if key not in link or link[key] <= op_value]
                    elif op == "$gte":
                        result = [link for link in result if key not in link or link[key] < op_value]
                    elif op == "$lt":
                        result = [link for link in result if key not in link or link[key] >= op_value]
                    elif op == "$lte":
                        result = [link for link in result if key not in link or link[key] > op_value]
            else:
                # Exact match exclusion
                result = [link for link in result if key not in link or link[key] != value]
                
        return result
    
    def close(self):
        """Close the database connection"""
        if self.client:
            self.client.close()
            logger.info("Closed MongoDB connection")


# Example usage
if __name__ == "__main__":
    # Example links
    sample_links = [
        {
            'url': 'https://example.gov/budget-2023.pdf',
            'text': 'FY 2023 Budget',
            'context': 'Download our financial documents',
            'relevance_score': 0.9
        },
        {
            'url': 'https://example.gov/contact',
            'text': 'Contact Us',
            'context': 'Get in touch with our staff',
            'relevance_score': 0.8
        }
    ]
    
    db = LinkDatabase()
    source_url = "https://example.gov/home"
    
    # Save links
    saved_count = db.save_links(sample_links, source_url)
    print(f"Saved {saved_count} links")
    
    # Retrieve links
    high_value_links = db.get_links(
        filter_params={"relevance_score": {"$gte": 0.7}},
        sort_by=[("relevance_score", -1)],
        limit=10
    )
    
    print(f"Retrieved {len(high_value_links)} high-value links")
    for link in high_value_links:
        print(f"URL: {link['url']}, Score: {link['relevance_score']}")
    
    # Close connection
    db.close()