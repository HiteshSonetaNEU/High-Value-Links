import os
import logging
import openai
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LLMClassifier:
    """
    A classifier that uses OpenAI's API to evaluate link relevance.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the LLM classifier with an API key.
        
        Args:
            api_key: OpenAI API key (will fall back to OPENAI_API_KEY env var if not provided)
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            logger.warning("No OpenAI API key provided. LLM classifier will be disabled.")
        else:
            openai.api_key = self.api_key
    
    def classify_links(self, links: List[Dict], keywords: List[str]) -> List[Dict]:
        """
        Classify links using OpenAI's API to determine their relevance.
        
        Args:
            links: List of link dictionaries to classify
            keywords: List of keywords to prioritize
            
        Returns:
            List of links with updated relevance scores
        """
        if not self.api_key:
            logger.warning("No OpenAI API key provided. Skipping LLM classification.")
            return links
            
        logger.info(f"Starting LLM classification for {len(links)} links...")
            
        # Increased batch size to reduce API calls (process up to 30 links at a time)
        batch_size = 30
        link_batches = [links[i:i+batch_size] for i in range(0, len(links), batch_size)]
        classified_links = []
        
        for batch_idx, batch in enumerate(link_batches):
            try:
                logger.info(f"Processing batch {batch_idx+1}/{len(link_batches)} with LLM")
                
                # Create prompt for OpenAI
                prompt = self._create_classification_prompt(batch, keywords)
                
                # Call OpenAI API
                logger.info("Calling OpenAI API...")
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are an AI that evaluates the relevance of links based on specific criteria."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.2,
                    max_tokens=2000  # Increased max tokens to accommodate larger batch size
                )
                logger.info("Received response from OpenAI API")
                
                # Process response
                result = response.choices[0].message.content.strip()
                updated_batch = self._parse_classification_response(batch, result)
                classified_links.extend(updated_batch)
                
                # Log a sample of the results
                if updated_batch:
                    sample = updated_batch[0]
                    logger.info(f"Sample LLM classification: URL: {sample['url']}, " 
                               f"Score: {sample.get('relevance_score', 'N/A')}, "
                               f"Reason: {sample.get('llm_reason', 'N/A')}")
                
            except Exception as e:
                logger.error(f"Error classifying links with OpenAI: {e}")
                logger.exception("Exception details:")
                # If API call fails, return the original batch
                classified_links.extend(batch)
                
        logger.info(f"LLM classification completed for {len(links)} links")
        return classified_links
    
    def _create_classification_prompt(self, links: List[Dict], keywords: List[str]) -> str:
        """
        Create a prompt for link classification.
        
        Args:
            links: List of links to classify
            keywords: List of keywords to prioritize
            
        Returns:
            String prompt for the OpenAI API
        """
        keywords_str = ", ".join(keywords)
        prompt = f"""Evaluate the following links based on their relevance to these keywords: {keywords_str}.
Focus on identifying links that might lead to important documents like budgets, financial reports (ACFR), 
or contact information for financial or administrative staff.

For each link, provide a relevance score between 0.0 and 1.0, where:
- 1.0 = Extremely relevant (direct link to target content)
- 0.7-0.9 = Highly relevant (likely leads to target content with 1-2 clicks)
- 0.4-0.6 = Moderately relevant (might lead to target content)
- 0.0-0.3 = Low relevance (unlikely to lead to target content)

Links to evaluate:
"""

        for i, link in enumerate(links, 1):
            prompt += f"\nLink {i}:\nURL: {link['url']}\nText: {link['text']}\nContext: {link['context']}\n"
            
        prompt += "\nRespond in this format for each link (replace X with the link number):\nLink X: [score] - [brief reason for score]"
        
        return prompt
    
    def _parse_classification_response(self, original_links: List[Dict], response: str) -> List[Dict]:
        """
        Parse the OpenAI response and assign relevance scores.
        
        Args:
            original_links: Original list of link dictionaries
            response: OpenAI API response text
            
        Returns:
            Updated list of links with LLM-based relevance scores
        """
        updated_links = original_links.copy()
        
        # Parse the response line by line
        lines = response.strip().split('\n')
        
        for line in lines:
            # Look for lines with the format "Link X: [score] - [reason]"
            if line.startswith('Link ') and ':' in line:
                try:
                    # Extract link number
                    link_part = line.split(':')[0].strip()
                    link_num = int(link_part.replace('Link ', '')) - 1
                    
                    # Extract score
                    score_part = line.split(':')[1].strip()
                    if ' - ' in score_part:
                        score_str = score_part.split(' - ')[0].strip()
                    else:
                        score_str = score_part.strip()
                        
                    # Convert score to float
                    score = float(score_str)
                    
                    # Update the corresponding link if valid index and score
                    if 0 <= link_num < len(updated_links) and 0.0 <= score <= 1.0:
                        # Assign LLM score directly (no combination with rule-based score)
                        updated_links[link_num]['relevance_score'] = score
                        
                        # Add LLM reason if available
                        if ' - ' in score_part:
                            reason = score_part.split(' - ')[1].strip()
                            updated_links[link_num]['llm_reason'] = reason
                
                except (ValueError, IndexError) as e:
                    logger.error(f"Error parsing LLM response line '{line}': {e}")
        
        return updated_links


# Example usage
if __name__ == "__main__":
    # Example links
    sample_links = [
        {
            'url': 'https://example.gov/budget-2023.pdf',
            'text': 'FY 2023 Budget',
            'context': 'Download our financial documents'
        },
        {
            'url': 'https://example.gov/contact',
            'text': 'Contact Us',
            'context': 'Get in touch with our staff'
        }
    ]
    
    keywords = ["ACFR", "Budget", "Finance", "Contact"]
    
    classifier = LLMClassifier()
    if classifier.api_key:
        classified_links = classifier.classify_links(sample_links, keywords)
        
        for link in classified_links:
            print(f"URL: {link['url']}")
            print(f"Text: {link['text']}")
            print(f"Relevance Score: {link.get('relevance_score', 'N/A')}")
            if 'llm_reason' in link:
                print(f"Reason: {link['llm_reason']}")
            print("---")
    else:
        print("API key not found. Cannot run the example.")