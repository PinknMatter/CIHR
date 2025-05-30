from linkup import LinkupClient
import json
import os
import pandas as pd

# Initialize client
client = LinkupClient(api_key=os.getenv("LINKUP_API_KEY"))

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

# Define structured output schema for Levels 1, 2, and 5
def get_schema():
    return """
    {
      "type": "object",
      "properties": {
        "primary_institute": {
          "type": "string",
          "description": "Primary Institute for the grant, e.g., 'Infection and Immunity'"
        },
        "level1_ip": {
          "type": "array",
          "description": "Level 1: Patents, copyright, trademarks",
          "items": {
            "type": "object",
            "properties": {
              "title": {"type": "string", "description": "Title of the IP"},
              "type": {"type": "string", "description": "Type of IP (patent, copyright, trademark)"},
              "filing_year": {"type": "integer", "description": "Year filed or granted"},
              "assignee": {"type": "string", "description": "Organization owning the IP"},
              "description": {"type": "string", "description": "Brief description of the IP"}
            }
          }
        },
        "level2_companies": {
          "type": "array",
          "description": "Level 2: Companies opened",
          "items": {
            "type": "object",
            "properties": {
              "company_name": {"type": "string", "description": "Name of the company"},
              "founding_year": {"type": "integer", "description": "Year company was founded"},
              "industry": {"type": "string", "description": "Type of industry"},
              "connection": {"type": "string", "description": "Connection to the researcher or project"}
            }
          }
        },
        "level5_exits": {
          "type": "array",
          "description": "Level 5: Companies acquired/publicly listed (exit strategies)",
          "items": {
            "type": "object",
            "properties": {
              "company_name": {"type": "string", "description": "Name of the company"},
              "exit_type": {"type": "string", "description": "Type of exit (acquisition, IPO)"},
              "exit_year": {"type": "integer", "description": "Year of exit"},
              "value": {"type": "string", "description": "Value of the exit if available"},
              "acquirer": {"type": "string", "description": "Name of acquiring company if applicable"}
            }
          }
        }
      }
    }
    """

# Function to analyze a single researcher/grant
def analyze_researcher(researcher_name, project_title, competition_code):
    # Parse the year and month from the competition code
    grant_year, grant_month = parse_competition_code(competition_code)
    grant_date_str = f"{grant_year}-{grant_month:02d}"
    
    print(f"\nAnalyzing: {researcher_name}")
    print(f"Project: {project_title}")
    print(f"Grant Code: {competition_code} (Date: {grant_date_str})")
    
    # Build query focusing on Levels 1, 2, and 5
    query = f"""
    You are an expert in analyzing commercialization outcomes for academic research.
    
    For the researcher and project below, analyze commercialization outcomes according to these specific levels:
    
    Level 1: Patents, copyright, trademarks
    - Find ALL patents, copyrights, and trademarks related to this research filed after {grant_date_str}
    - Only include IP that is directly related to the project topic
    
    Level 2: Companies opened
    - Find ALL companies founded or co-founded by {researcher_name}
    - Find ALL companies that have licensed or commercialized technology from this research
    
    Level 5: Companies acquired/publicly listed (exit strategies)
    - Find ALL companies related to this research that were acquired after {grant_date_str}
    - Find ALL companies related to this research that went public (IPO) after {grant_date_str}
    
    Also determine the Primary Institute / theme that would be most appropriate for this research grant. This is not the university, but the overall theme of the grant. for example, a primary insititue could be infection and immunity.
    
    Researcher: {researcher_name}
    Project Title: {project_title}
    
    For each outcome, provide the year it occurred and verify it happened after {grant_date_str}.
    Only include outcomes that are directly relevant to the project title and research area.
    """
    
    # Make the API call
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
        "grant_code": competition_code,
        "grant_date": grant_date_str,
        "data": structured_data,
        "scores": scores
    }
    
    return results

# Function to calculate scores based on the levels
def calculate_scores(data):
    scores = {
        "level1": 0,
        "level2": 0,
        "level5": 0,
        "total": 0,
        "highest_level": 0  # New field to store the highest level achieved (1-5)
    }
    
    # Level 1: Patents, copyright, trademarks
    if 'level1_ip' in data and isinstance(data['level1_ip'], list):
        scores["level1"] = len(data['level1_ip'])
    
    # Level 2: Companies opened
    if 'level2_companies' in data and isinstance(data['level2_companies'], list):
        scores["level2"] = len(data['level2_companies'])
    
    # Level 5: Companies acquired/publicly listed
    if 'level5_exits' in data and isinstance(data['level5_exits'], list):
        scores["level5"] = len(data['level5_exits'])
    
    # Calculate total score
    scores["total"] = scores["level1"] + scores["level2"] + scores["level5"]
    
    # Determine the highest level (1-5) achieved
    # Level 5 takes precedence over all others
    if scores["level5"] > 0:
        scores["highest_level"] = 5
    # Level 2 is next in precedence
    elif scores["level2"] > 0:
        scores["highest_level"] = 2
    # Level 1 is the lowest level
    elif scores["level1"] > 0:
        scores["highest_level"] = 1
    
    return scores

# Function to process multiple researchers from CSV
def process_csv(csv_path):
    # Read the CSV file
    try:
        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} grants from {csv_path}")
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None
    
    # Check if required columns exist
    required_columns = ['Name', 'Project_Title', 'Competition_CD']
    for col in required_columns:
        if col not in df.columns:
            print(f"Error: Required column '{col}' not found in CSV")
            return None
    
    # Process each researcher
    all_results = []
    for i, row in df.iterrows():
        try:
            # Limit to first 5 for testing
            if i >= 5:
                break
                
            researcher_name = row['Name']
            project_title = row['Project_Title']
            competition_code = row['Competition_CD']
            
            # Analyze the researcher
            result = analyze_researcher(researcher_name, project_title, competition_code)
            all_results.append(result)
            
            # Save individual result
            researcher_filename = f"level125_{researcher_name.replace(' ', '_').replace(',', '')}.json"
            with open(researcher_filename, "w") as f:
                json.dump(result, f, indent=2)
            print(f"Results saved to {researcher_filename}")
            
        except Exception as e:
            print(f"Error processing researcher {row['Name']}: {e}")
    
    # Save all results to a single file
    with open("all_level125_results.json", "w") as f:
        json.dump(all_results, f, indent=2)
    
    return all_results

# Main execution
if __name__ == "__main__":
    # Option 1: Process a single researcher
    researcher_name = "Weaver, Donald F"
    project_title = "Design and Development of Brain Permeant Indoleamine-2,3-Dioxygenase Inhibitors"
    competition_code = "201709PJT"
    
    result = analyze_researcher(researcher_name, project_title, competition_code)
    
    # Save the result to a JSON file
    output_file = "level125_results.json"
    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)
    
    print(f"\nResults saved to {output_file}")
    
    # Display primary institute if available
    if 'data' in result and 'primary_institute' in result['data']:
        print(f"\nPrimary Institute: {result['data']['primary_institute']}")
    
    print("\nScores:")
    for level, score in result["scores"].items():
        print(f"- {level}: {score}")
    
    # Option 2: Process multiple researchers from CSV
    # Uncomment to use
    # csv_path = "All Recipients_ CMZ, PoP, CHRP.xlsx - Sheet1.csv"
    # all_results = process_csv(csv_path)

    

