import os
import sys
import json
import argparse
from dotenv import load_dotenv
from serpapi import GoogleSearch

# Load environment variables from .env file
load_dotenv()

def count_patents(inventor=None, keywords=None, after_date=None, before_date=None, api_key=None, debug=False):
    """
    Count patents for a specific inventor with optional keywords and date filters.
    
    Args:
        inventor (str): Name of the inventor to search for
        keywords (list): List of keywords to include in the search
        after_date (str): Search for patents after this date (format: YYYY-MM-DD)
        before_date (str): Search for patents before this date (format: YYYY-MM-DD)
        api_key (str): SerpAPI key
        debug (bool): Whether to print debug information
        
    Returns:
        int: Total number of results found
    """
    # Get API key
    api_key = api_key or os.environ.get("SERPAPI_KEY")
    if not api_key:
        raise ValueError("SerpAPI key is required")
    
    # Remove quotes if they were included in the API key
    api_key = api_key.strip('"').strip("'")
    
    # Build the search query for keywords only
    keyword_query = ""
    if keywords and len(keywords) > 0:
        # Format keywords as (word1);(word2) as per SerpAPI documentation
        keyword_query = ";".join([f"({k})" for k in keywords])
    
    # Format dates for the 'after' and 'before' parameters using priority:YYYYMMDD format
    after_param = None
    if after_date:
        # Remove hyphens from the date and format as priority:YYYYMMDD
        date_formatted = after_date.replace("-", "")
        after_param = f"priority:{date_formatted}"
    
    before_param = None
    if before_date:
        # Remove hyphens from the date and format as priority:YYYYMMDD
        date_formatted = before_date.replace("-", "")
        before_param = f"priority:{date_formatted}"
    
    # Prepare search parameters according to SerpAPI documentation
    params = {
        "engine": "google_patents",
        "api_key": api_key,
        "num": 10  # Request a few results to ensure we get proper count
    }
    
    # Add query parameters separately
    if keyword_query:
        params["q"] = keyword_query
    
    # Add inventor as a separate parameter
    if inventor:
        params["inventor"] = inventor
    
    # Add date filters as separate parameters
    if after_param:
        params["after"] = after_param
    
    if before_param:
        params["before"] = before_param
    
    if debug:
        print(f"\nDebug: Parameters being sent to SerpAPI:")
        print(f"Debug: Full params: {params}")
    
    # Execute search
    try:
        search = GoogleSearch(params)
        results = search.get_dict()
        
        if debug:
            print(f"\nDebug: Response structure: {list(results.keys())}")
            if "search_information" in results:
                print(f"Debug: Search info: {results['search_information']}")
            if "error" in results:
                print(f"Debug: Error: {results['error']}")
        
        # Get total results count
        total_results = results.get("search_information", {}).get("total_results", 0)
        
        # If we got organic_results but total_results is 0, something might be wrong
        if total_results == 0 and "organic_results" in results and len(results["organic_results"]) > 0:
            total_results = len(results["organic_results"])
            if debug:
                print(f"Debug: Found {total_results} results in organic_results despite total_results being 0")
        
        return total_results
    
    except Exception as e:
        print(f"Error during API call: {str(e)}")
        if debug:
            import traceback
            traceback.print_exc()
        return 0

def main():
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description='Count patents for specific inventors and keywords')
    parser.add_argument('--inventor', '-i', type=str, help='Inventor name to search for')
    parser.add_argument('--keywords', '-k', type=str, help='Keywords to search for, comma-separated')
    parser.add_argument('--after', '-a', type=str, help='Search for patents after this date (YYYY-MM-DD)')
    parser.add_argument('--before', '-b', type=str, help='Search for patents before this date (YYYY-MM-DD)')
    parser.add_argument('--debug', '-d', action='store_true', help='Enable debug output')
    parser.add_argument('--test', '-t', action='store_true', help='Run a test search with known results')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Get API key from environment variable
    api_key = os.environ.get("SERPAPI_KEY")
    if not api_key:
        print("Error: No API key found. Please set the SERPAPI_KEY environment variable or add it to .env file")
        sys.exit(1)
    
    # Run test search if requested
    if args.test:
        print("\nRunning test search with specified inventor...")
        test_count = count_patents(
            inventor="Purang Abolmaesumi",
            keywords=["Needle Guidance"],
            after_date="2011-10-01",
            api_key=api_key,
            debug=args.debug
        )
        print(f"\nTest search results: {test_count} patents found for Purang Abolmaesumi on 'Needle Guidance' since 2011-10-01")
        if test_count == 0:
            print("\nWarning: Test search returned 0 results. This could be because:")
            print("1. The inventor may not have patents matching these criteria")
            print("2. There might be an issue with the API key or connection")
            print("3. The search query format might need adjustment")
            print("\nTry searching directly on Google Patents to verify: https://patents.google.com/")
        return
    
    # Process keywords if provided
    keywords = None
    if args.keywords:
        keywords = [k.strip() for k in args.keywords.split(',')]
    
    # Ensure we have an inventor name unless we're in test mode
    if not args.inventor and not args.test:
        print("Error: Inventor name is required unless using --test mode")
        sys.exit(1)
    
    try:
        # Get the count
        count = count_patents(
            inventor=args.inventor,
            keywords=keywords,
            after_date=args.after,
            before_date=args.before,
            api_key=api_key,
            debug=args.debug
        )
        
        # Print search parameters and result
        print(f"\nSearch parameters:")
        print(f"- Inventor: {args.inventor}")
        if keywords:
            print(f"- Keywords: {keywords}")
        if args.after:
            print(f"- After date: {args.after}")
        if args.before:
            print(f"- Before date: {args.before}")
        
        # Just print the number
        print(f"\nTotal patents found: {count}")
        
        if count == 0:
            print("\nNo patents found. Try:")
            print("1. Checking the spelling of the inventor name")
            print("2. Using fewer or different keywords")
            print("3. Widening the date range")
            print("4. Running with --debug flag to see the API response")
            print("5. Try the --test flag to verify API connectivity")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
