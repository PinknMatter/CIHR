import os
from dotenv import load_dotenv
from patent_search import PatentSearcher

# Load environment variables from .env file (if it exists)
load_dotenv()

def main():
    # Get API key from environment variable or set it directly here
    api_key = os.environ.get("SERPAPI_KEY")
    
    if not api_key:
        api_key = input("Please enter your SerpAPI key: ")
    
    # Initialize the patent searcher
    searcher = PatentSearcher(api_key=api_key)
    
    # Get search parameters from user
    print("\n=== Google Patents Search ===\n")
    
    # Get inventor name
    inventor = input("Enter inventor name (e.g., 'Robert Langer'): ")
    if not inventor:
        inventor = "Robert Langer"  # Default example
        print(f"Using default inventor: {inventor}")
    
    # Get keywords
    keywords_input = input("Enter keywords separated by commas (e.g., 'drug delivery, nanoparticles'): ")
    keywords = [k.strip() for k in keywords_input.split(",")] if keywords_input else []
    
    # Get date range
    after_date = input("Enter start date (YYYY-MM-DD) or leave blank for no start date: ")
    before_date = input("Enter end date (YYYY-MM-DD) or leave blank for no end date: ")
    
    # Get number of results
    num_results_input = input("Enter number of results to retrieve (default: 20): ")
    num_results = int(num_results_input) if num_results_input.isdigit() else 20
    
    print("\nSearching for patents...")
    results = searcher.search_patents(
        inventor=inventor,
        keywords=keywords,
        after_date=after_date if after_date else None,
        before_date=before_date if before_date else None,
        num_results=num_results,
        save_results=True
    )
    
    # Display results
    print(f"\nFound {results['total_results']} patents for {inventor}")
    if keywords:
        print(f"Keywords: {keywords}")
    if after_date:
        print(f"After date: {after_date}")
    if before_date:
        print(f"Before date: {before_date}")
    
    # Display first few results if available
    if results['results']:
        print("\nTop results:")
        for i, patent in enumerate(results['results'][:5], 1):
            print(f"\n{i}. {patent.get('title', 'No title')}")
            print(f"   Link: {patent.get('link', 'No link')}")
            if 'publication_info' in patent and patent['publication_info']:
                print(f"   Publication: {patent['publication_info'].get('summary', 'No info')}")
            print(f"   Snippet: {patent.get('snippet', 'No snippet')}")
    
    print("\nFull results have been saved to JSON and CSV files in the current directory.")

if __name__ == "__main__":
    main()
