import json
import os
import pandas as pd

# Function to parse the Competition_CD format (e.g., "201709PJT") into year and month
def parse_competition_code(code):
    # Extract the first 6 digits (YYYYMM) from the code
    if isinstance(code, str) and len(code) >= 6:
        try:
            year = int(code[0:4])
            month = int(code[4:6])
            return year, month
        except ValueError:
            # Default to current values if parsing fails
            return 2017, 9
    else:
        # Default values if code is not in expected format
        return 2017, 9

# Define structured output schema for Levels 3 and 4
def get_schema():
    return """
    {
      "type": "object",
      "properties": {
        "level3_series_a": {
          "type": "array",
          "description": "Level 3: Series A funding round occurred; product in clinical stage I",
          "items": {
            "type": "object",
            "properties": {
              "company_name": {"type": "string", "description": "Name of the company"},
              "funding_year": {"type": "integer", "description": "Year of Series A funding"},
              "funding_amount": {"type": "string", "description": "Amount of funding if available"},
              "product_name": {"type": "string", "description": "Name of product in clinical stage I"},
              "clinical_trial_year": {"type": "integer", "description": "Year clinical trial started"}
            }
          }
        },
        "level4_series_bc": {
          "type": "array",
          "description": "Level 4: Series B and C funding rounds occurred; product in clinical stage II and III",
          "items": {
            "type": "object",
            "properties": {
              "company_name": {"type": "string", "description": "Name of the company"},
              "funding_round": {"type": "string", "description": "Series B or C"},
              "funding_year": {"type": "integer", "description": "Year of funding"},
              "funding_amount": {"type": "string", "description": "Amount of funding if available"},
              "product_name": {"type": "string", "description": "Name of product in clinical stage II/III"},
              "clinical_stage": {"type": "string", "description": "Clinical stage (II or III)"},
              "clinical_trial_year": {"type": "integer", "description": "Year clinical trial started"}
            }
          }
        }
      }
    }
    """

# Function to analyze venture funding for a single researcher/grant
def analyze_venture_funding(level125_file):
    print(f"\nAnalyzing venture funding from: {level125_file}")
    
    # Load the Level 1, 2, 5 results from the file
    try:
        with open(level125_file, 'r') as f:
            level125_data = json.load(f)
    except Exception as e:
        print(f"Error loading file: {e}")
        return None
    
    # Extract researcher info
    researcher_name = level125_data.get('researcher', 'Unknown Researcher')
    project_title = level125_data.get('project', 'Unknown Project')
    grant_date_str = level125_data.get('grant_date', '2017-09')
    
    # Extract primary institute if available
    primary_institute = None
    if 'data' in level125_data and 'primary_institute' in level125_data['data']:
        primary_institute = level125_data['data']['primary_institute']
    
    print(f"Researcher: {researcher_name}")
    print(f"Project: {project_title}")
    print(f"Grant Date: {grant_date_str}")
    if primary_institute:
        print(f"Primary Institute: {primary_institute}")
    
    # Extract companies from Level 2 data
    companies = []
    if 'data' in level125_data and 'level2_companies' in level125_data['data']:
        companies = level125_data['data']['level2_companies']
    
    if not companies:
        print("No companies found in Level 2 data. Cannot analyze venture funding.")
        return None
    
    # Build query focused on Levels 3 and 4 (venture funding and clinical trials)
    company_names = ", ".join([company.get('company_name', '') for company in companies if 'company_name' in company])
    
    print(f"\nSearching for venture funding and clinical trial information for companies: {company_names}")
    
    query = f"""
    You are an expert in analyzing venture funding and clinical trials for biotech and pharmaceutical companies.
    
    For the companies {company_names}, analyze Their latest funding rounds (after {grant_date_str}).
    """
    
    # Use the Linkup client from the main script to make the API call
    from linkup import LinkupClient
    client = LinkupClient(api_key="a1f49b55-fbd5-4fbe-a630-f86c1ae6d4d1")
    
    print(f"\nSearching for venture funding and clinical trial information for companies: {company_names}")
    
    response = client.search(
        query=query,
        depth="deep",  # Use deep search for more comprehensive results
        output_type="structured",
        structured_output_schema=get_schema(),
        include_images=False
    )
    
    # Extract the structured output
    if hasattr(response, 'structured_output'):
        structured_data = response.structured_output
    elif isinstance(response, dict) and 'structured_output' in response:
        structured_data = response['structured_output']
    else:
        structured_data = response  # Fallback if structure is different
    
    # Calculate scores
    scores = calculate_scores(structured_data)
    
    # Create a results dictionary with all the information
    results = {
        "researcher": researcher_name,
        "project": project_title,
        "grant_date": grant_date_str,
        "primary_institute": primary_institute,
        "companies_analyzed": company_names,
        "data": structured_data,
        "scores": scores
    }
    
    # Save the results to a JSON file
    output_file = f"venture_funding_{researcher_name.replace(' ', '_').replace(',', '')}.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nVenture funding analysis saved to '{output_file}'")
    print("\nSummary of findings:")
    
    # Print a summary of what was found at each level
    levels = {
        "level3": "Level 3: Series A funding & clinical stage I",
        "level4": "Level 4: Series B/C funding & clinical stage II/III",
        "total": "Total Score",
        "highest_level": "Highest Level Achieved"
    }
    
    for level_key, level_name in levels.items():
        print(f"- {level_name}: {scores.get(level_key, 0)}")
    
    # Show detailed findings
    print("\nDetailed findings:")
    
    # Level 3: Series A funding & clinical stage I
    if 'level3_series_a' in structured_data and isinstance(structured_data['level3_series_a'], list):
        print("\nLevel 3: Series A funding & clinical stage I")
        for i, entry in enumerate(structured_data['level3_series_a'], 1):
            print(f"  {i}. {entry.get('company_name', 'Unknown Company')}")
            if 'funding_year' in entry:
                print(f"     Series A funding in {entry.get('funding_year')}")
                if 'funding_amount' in entry and entry.get('funding_amount'):
                    print(f"     Amount: {entry.get('funding_amount')}")
            if 'product_name' in entry and entry.get('product_name'):
                print(f"     Product: {entry.get('product_name')}")
                if 'clinical_trial_year' in entry:
                    print(f"     Clinical trial (Phase I) started in {entry.get('clinical_trial_year')}")
    
    # Level 4: Series B/C funding & clinical stage II/III
    if 'level4_series_bc' in structured_data and isinstance(structured_data['level4_series_bc'], list):
        print("\nLevel 4: Series B/C funding & clinical stage II/III")
        for i, entry in enumerate(structured_data['level4_series_bc'], 1):
            print(f"  {i}. {entry.get('company_name', 'Unknown Company')}")
            if 'funding_round' in entry and 'funding_year' in entry:
                print(f"     {entry.get('funding_round')} funding in {entry.get('funding_year')}")
                if 'funding_amount' in entry and entry.get('funding_amount'):
                    print(f"     Amount: {entry.get('funding_amount')}")
            if 'product_name' in entry and entry.get('product_name'):
                print(f"     Product: {entry.get('product_name')}")
                if 'clinical_stage' in entry and 'clinical_trial_year' in entry:
                    print(f"     Clinical trial (Phase {entry.get('clinical_stage')}) started in {entry.get('clinical_trial_year')}")
    
    # Print the highest level achieved
    print(f"\nHighest Level Achieved: {scores.get('highest_level', 0)}")
    
    return results

# Function to calculate scores based on the levels
def calculate_scores(data):
    scores = {
        "level3": 0,
        "level4": 0,
        "total": 0,
        "highest_level": 0  # New field to store the highest level achieved (1-5)
    }
    
    # Level 3: Series A funding & clinical stage I
    if 'level3_series_a' in data and isinstance(data['level3_series_a'], list):
        scores["level3"] = len(data['level3_series_a'])
    
    # Level 4: Series B/C funding & clinical stage II/III
    if 'level4_series_bc' in data and isinstance(data['level4_series_bc'], list):
        scores["level4"] = len(data['level4_series_bc'])
    
    # Calculate total score
    scores["total"] = scores["level3"] + scores["level4"]
    
    # Determine the highest level (3-4) achieved in this analysis
    if scores["level4"] > 0:
        scores["highest_level"] = 4
    elif scores["level3"] > 0:
        scores["highest_level"] = 3
    
    return scores

# Function to combine level results and determine final highest level
def combine_level_results(level125_results, venture_funding_results):
    # Extract scores from both analyses
    level125_scores = level125_results.get('scores', {})
    venture_scores = venture_funding_results.get('scores', {})
    
    # Get highest level from each analysis
    level125_highest = level125_scores.get('highest_level', 0)
    venture_highest = venture_scores.get('highest_level', 0)
    
    # Determine the final highest level (1-5)
    final_highest_level = max(level125_highest, venture_highest)
    
    # Create combined results
    combined_results = {
        "researcher": level125_results.get('researcher', 'Unknown Researcher'),
        "project": level125_results.get('project', 'Unknown Project'),
        "grant_date": level125_results.get('grant_date', ''),
        "primary_institute": level125_results.get('data', {}).get('primary_institute', ''),
        "level_details": {
            "level1": level125_scores.get('level1', 0),
            "level2": level125_scores.get('level2', 0),
            "level3": venture_scores.get('level3', 0),
            "level4": venture_scores.get('level4', 0),
            "level5": level125_scores.get('level5', 0)
        },
        "highest_level": final_highest_level,
        "level_descriptions": {
            "1": "Patents, copyright, trademarks",
            "2": "Companies opened",
            "3": "Series A funding round occurred; product in clinical stage I",
            "4": "Series B and C funding rounds occurred; product in clinical stage II, and III",
            "5": "Companies acquired/publicly listed (exit strategies for companies)"
        }
    }
    
    return combined_results

# Function to process multiple researchers from Level 1, 2, 5 results
def process_all_level125_results(level125_file="all_level125_results.json"):
    try:
        with open(level125_file, 'r') as f:
            all_results = json.load(f)
    except Exception as e:
        print(f"Error loading file: {e}")
        return None
    
    # Process each researcher's results
    final_results = []
    for i, result in enumerate(all_results, 1):
        print(f"\nProcessing researcher {i}/{len(all_results)}: {result.get('researcher', 'Unknown')}")
        
        # Save the individual result to a separate file
        researcher_name = result.get('researcher', f"Researcher_{i}")
        output_file = f"level125_{researcher_name.replace(' ', '_').replace(',', '')}.json"
        
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)
        
        print(f"Saved to {output_file}")
        
        # Analyze venture funding for this researcher
        venture_results = analyze_venture_funding(output_file)
        
        if venture_results:
            # Combine results and determine final highest level
            combined_results = combine_level_results(result, venture_results)
            final_results.append(combined_results)
            
            # Save combined results
            combined_file = f"combined_results_{researcher_name.replace(' ', '_').replace(',', '')}.json"
            with open(combined_file, "w") as f:
                json.dump(combined_results, f, indent=2)
            print(f"Combined results saved to {combined_file}")
    
    # Save all final results to a single file
    if final_results:
        with open("all_combined_results.json", "w") as f:
            json.dump(final_results, f, indent=2)
        print("\nAll combined results saved to all_combined_results.json")
    
    print("\nAll researchers processed.")
    return final_results

# Main execution
if __name__ == "__main__":
    # Option 1: Process a single researcher's Level 1, 2, 5 results
    level125_file = "level125_results.json"
    
    if os.path.exists(level125_file):
        result = analyze_venture_funding(level125_file)
    else:
        print(f"File not found: {level125_file}")
        print("Please run Linkup.py first to generate the Level 1, 2, 5 results.")
    
    # Option 2: Process all researchers from the combined Level 1, 2, 5 results
    # Uncomment to use
    # all_level125_file = "all_level125_results.json"
    # if os.path.exists(all_level125_file):
    #     all_results = process_all_level125_results(all_level125_file)
    # else:
    #     print(f"File not found: {all_level125_file}")
    #     print("Please run Linkup.py with the process_csv function first.")

