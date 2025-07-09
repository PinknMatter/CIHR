import os
import json
import pandas as pd
from serpapi import GoogleSearch
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("patent_search_log.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PatentSearcher:
    def __init__(self, api_key=None):
        """
        Initialize the PatentSearcher with a SerpAPI key.
        
        Args:
            api_key (str): SerpAPI key. If None, will try to get from environment variable.
        """
        self.api_key = api_key or os.environ.get("SERPAPI_KEY")
        if not self.api_key:
            raise ValueError("SerpAPI key is required. Set it as an environment variable 'SERPAPI_KEY' or pass it directly.")
        
        logger.info("PatentSearcher initialized")
    
    def search_patents(self, 
                      inventor=None, 
                      keywords=None, 
                      after_date=None, 
                      before_date=None, 
                      num_results=10,
                      save_results=True,
                      output_file=None):
        """
        Search Google Patents using SerpAPI.
        
        Args:
            inventor (str): Name of the inventor to search for
            keywords (list): List of keywords to include in the search
            after_date (str): Search for patents after this date (format: YYYY-MM-DD)
            before_date (str): Search for patents before this date (format: YYYY-MM-DD)
            num_results (int): Number of results to return
            save_results (bool): Whether to save results to a file
            output_file (str): Path to save results (if None, will generate one)
            
        Returns:
            dict: Search results and metadata
        """
        # Build the search query
        query = []
        
        if inventor:
            query.append(f"inventor:\"{inventor}\"")
        
        if keywords:
            query.append(" ".join(keywords))
        
        date_range = []
        if after_date:
            date_range.append(f"after:{after_date}")
        if before_date:
            date_range.append(f"before:{before_date}")
        
        if date_range:
            query.append(" ".join(date_range))
        
        search_query = " ".join(query)
        
        # Prepare search parameters
        params = {
            "engine": "google_patents",
            "q": search_query,
            "api_key": self.api_key,
            "num": num_results
        }
        
        logger.info(f"Searching with query: {search_query}")
        
        # Execute search
        search = GoogleSearch(params)
        results = search.get_dict()
        
        # Log the number of results
        total_results = results.get("search_information", {}).get("total_results", 0)
        logger.info(f"Found {total_results} results for query: {search_query}")
        
        # Save results if requested
        if save_results:
            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                inventor_slug = inventor.replace(" ", "_") if inventor else "no_inventor"
                output_file = f"patent_search_{inventor_slug}_{timestamp}.json"
            
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Results saved to {output_file}")
            
            # Also save a CSV with key information
            if "organic_results" in results:
                patents_data = []
                for patent in results.get("organic_results", []):
                    patent_data = {
                        "title": patent.get("title"),
                        "link": patent.get("link"),
                        "publication_info": patent.get("publication_info", {}).get("summary"),
                        "snippet": patent.get("snippet")
                    }
                    patents_data.append(patent_data)
                
                if patents_data:
                    csv_file = output_file.replace(".json", ".csv")
                    pd.DataFrame(patents_data).to_csv(csv_file, index=False)
                    logger.info(f"Patent summary saved to {csv_file}")
        
        return {
            "query": search_query,
            "total_results": total_results,
            "results": results.get("organic_results", []),
            "search_information": results.get("search_information", {})
        }
    
    def batch_search_inventors(self, inventors, keywords=None, after_date=None, before_date=None):
        """
        Search patents for multiple inventors and compile results.
        
        Args:
            inventors (list): List of inventor names
            keywords (list): List of keywords to include in the search
            after_date (str): Search for patents after this date (format: YYYY-MM-DD)
            before_date (str): Search for patents before this date (format: YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: Summary of results for all inventors
        """
        summary_data = []
        
        for inventor in inventors:
            logger.info(f"Searching patents for inventor: {inventor}")
            
            try:
                result = self.search_patents(
                    inventor=inventor,
                    keywords=keywords,
                    after_date=after_date,
                    before_date=before_date
                )
                
                summary_data.append({
                    "inventor": inventor,
                    "keywords": keywords,
                    "after_date": after_date,
                    "before_date": before_date,
                    "total_results": result["total_results"],
                    "query": result["query"]
                })
            except Exception as e:
                logger.error(f"Error searching for {inventor}: {str(e)}")
                summary_data.append({
                    "inventor": inventor,
                    "keywords": keywords,
                    "after_date": after_date,
                    "before_date": before_date,
                    "total_results": "ERROR",
                    "query": "ERROR",
                    "error": str(e)
                })
        
        # Create summary DataFrame
        summary_df = pd.DataFrame(summary_data)
        
        # Save summary
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_file = f"patent_search_summary_{timestamp}.csv"
        summary_df.to_csv(summary_file, index=False)
        logger.info(f"Search summary saved to {summary_file}")
        
        return summary_df


# Example usage
if __name__ == "__main__":
    # You can set your API key here or as an environment variable
    # api_key = "your_serpapi_key_here"
    
    try:
        searcher = PatentSearcher()
        
        # Example 1: Search for a single inventor
        results = searcher.search_patents(
            inventor="John Smith",
            keywords=["artificial intelligence", "machine learning"],
            after_date="2020-01-01"
        )
        print(f"Found {results['total_results']} patents")
        
        # Example 2: Batch search for multiple inventors
        inventors = ["John Smith", "Jane Doe", "Albert Einstein"]
        summary = searcher.batch_search_inventors(
            inventors=inventors,
            keywords=["artificial intelligence"],
            after_date="2019-01-01"
        )
        print(summary)
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
