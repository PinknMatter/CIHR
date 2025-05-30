from linkup.client import LinkupClient
import json
import os
import pandas as pd

# Initialize client
client = LinkupClient(api_key="a1f49b55-fbd5-4fbe-a630-f86c1ae6d4d1")

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

# Function to process data from Spark table in Databricks
def process_spark_table(limit=None):
    # Access Spark session in Databricks
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("GrantOutcomesAnalysis").getOrCreate()
    
    # Read data from the grant_outcomes_results table
    df = spark.sql("SELECT * FROM grant_outcomes_results")
    
    # Apply limit if specified
    if limit is not None:
        df = df.limit(limit)
    
    print(f"Loaded {df.count()} grants from grant_outcomes_results table")
    
    # Convert Spark DataFrame to Pandas for processing
    pandas_df = df.toPandas()
    
    # Check if required columns exist
    required_columns = ['Name', 'Project_Title', 'Competition_CD']
    for col in required_columns:
        if col not in pandas_df.columns:
            print(f"Error: Required column '{col}' not found in table")
            return None
    
    results_list = []
    # Process each row
    for index, row in pandas_df.iterrows():
        researcher_name = row['Name']
        project_title = row['Project_Title']
        competition_code = row['Competition_CD']
        
        # Analyze the researcher
        result = analyze_researcher(researcher_name, project_title, competition_code)
        results_list.append(result)
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results_list)
    
    # Prepare data for updating the Spark table
    update_data = {
        'Name': [],
        'Project_Title': [],
        'Competition_CD': [],
        'outcome_data': [],
        'level_1_score': [],
        'level_2_score': [],
        'level_5_score': [],
        'total_score': []
    }
    
    for index, row in results_df.iterrows():
        update_data['Name'].append(row['researcher'])
        update_data['Project_Title'].append(row['project'])
        update_data['Competition_CD'].append(row['grant_code'])
        update_data['outcome_data'].append(json.dumps(row['data']))
        update_data['level_1_score'].append(row['scores']['level1'])
        update_data['level_2_score'].append(row['scores']['level2'])
        update_data['level_5_score'].append(row['scores']['level5'])
        update_data['total_score'].append(row['scores']['highest_level'])
    
    # Create a new DataFrame with updated data
    update_df = pd.DataFrame(update_data)
    
    # Convert Pandas DataFrame back to Spark DataFrame
    spark_update_df = spark.createDataFrame(update_df)
    
    # Register the update DataFrame as a temporary table
    spark_update_df.createOrReplaceTempView("temp_updates")
    
    # Perform the update operation on the existing table using Databricks SQL syntax
    # Using MERGE instead of UPDATE as it's better supported in Databricks
    spark.sql("""
        MERGE INTO grant_outcomes_results AS target
        USING temp_updates AS source
        ON 
            target.Name = source.Name AND
            target.Project_Title = source.Project_Title AND
            target.Competition_CD = source.Competition_CD
        WHEN MATCHED THEN UPDATE SET
            target.outcome_data = source.outcome_data,
            target.level_1_score = source.level_1_score,
            target.level_2_score = source.level_2_score,
            target.level_5_score = source.level_5_score,
            target.total_score = source.total_score
    """)
    
    print("Updated existing rows in grant_outcomes_results table with analysis results")
    
    return results_df

# Main execution for Databricks
def main():
    # For debugging, limit to a small number of records
    results = process_spark_table(limit=3)
    if results is not None:
        print("Processing completed. Results have been saved to the grant_outcomes_results table.")
    else:
        print("Processing failed due to errors.")

if __name__ == "__main__":
    main()
