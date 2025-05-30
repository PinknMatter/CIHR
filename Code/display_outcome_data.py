"""
Display Outcome Data Script

This script displays the JSON data stored in the outcome_data column of the grant_outcomes_results table
in a readable format. It can be run in Databricks to help inspect and debug the data.
"""

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json
import json
import pandas as pd

def display_outcome_data(limit=None, researcher_name=None):
    """
    Display the JSON data from the outcome_data column in a readable format.
    
    Parameters:
    -----------
    limit : int, optional
        Limit the number of records to display
    researcher_name : str, optional
        Filter results by researcher name
    """
    # Access Spark session in Databricks
    spark = SparkSession.builder.appName("DisplayOutcomeData").getOrCreate()
    
    # Base query to select from the table
    query = "SELECT Name, Project_Title, Competition_CD, outcome_data, level_1_score, level_2_score, level_5_score, total_score FROM grant_outcomes_results"
    
    # Start building WHERE clause
    where_clauses = ["outcome_data IS NOT NULL", "outcome_data != 'null'", "outcome_data != '{}'"] 
    
    # Add filter for researcher name if provided
    if researcher_name:
        where_clauses.append(f"Name = '{researcher_name}'")
    
    # Add WHERE clause to query
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    # Execute the query
    df = spark.sql(query)
    
    # Apply limit if specified
    if limit:
        df = df.limit(limit)
    
    # Convert to Pandas for easier display
    pandas_df = df.toPandas()
    
    # Display basic information
    print(f"Found {len(pandas_df)} records")
    
    # Process each row
    for index, row in pandas_df.iterrows():
        print("\n" + "="*80)
        print(f"Record {index+1}:")
        print(f"Researcher: {row['Name']}")
        print(f"Project: {row['Project_Title']}")
        print(f"Competition Code: {row['Competition_CD']}")
        print(f"Scores: Level 1={row['level_1_score']}, Level 2={row['level_2_score']}, Level 5={row['level_5_score']}, Total={row['total_score']}")
        
        # Parse and display the JSON data
        try:
            if row['outcome_data'] and row['outcome_data'] != 'null':
                outcome_data = json.loads(row['outcome_data'])
                print("\nOutcome Data:")
                
                # Display Primary Institute if available
                if 'primary_institute' in outcome_data:
                    print(f"\nPrimary Institute: {outcome_data['primary_institute']}")
                
                # Display Level 1 IP data
                if 'level1_ip' in outcome_data and outcome_data['level1_ip']:
                    print("\nLevel 1 - Patents, Copyrights, Trademarks:")
                    for ip in outcome_data['level1_ip']:
                        print(f"  - {ip.get('type', 'IP')}: {ip.get('title', 'Untitled')}")
                        if 'filing_year' in ip:
                            print(f"    Year: {ip['filing_year']}")
                        if 'assignee' in ip:
                            print(f"    Assignee: {ip['assignee']}")
                        if 'description' in ip:
                            print(f"    Description: {ip['description']}")
                
                # Display Level 2 Companies data
                if 'level2_companies' in outcome_data and outcome_data['level2_companies']:
                    print("\nLevel 2 - Companies Opened:")
                    for company in outcome_data['level2_companies']:
                        print(f"  - Company: {company.get('company_name', 'Unnamed')}")
                        if 'founding_year' in company:
                            print(f"    Founded: {company['founding_year']}")
                        if 'industry' in company:
                            print(f"    Industry: {company['industry']}")
                        if 'connection' in company:
                            print(f"    Connection: {company['connection']}")
                
                # Display Level 5 Exits data
                if 'level5_exits' in outcome_data and outcome_data['level5_exits']:
                    print("\nLevel 5 - Acquisitions/IPOs:")
                    for exit_data in outcome_data['level5_exits']:
                        print(f"  - Company: {exit_data.get('company_name', 'Unnamed')}")
                        if 'exit_type' in exit_data:
                            print(f"    Exit Type: {exit_data['exit_type']}")
                        if 'exit_year' in exit_data:
                            print(f"    Year: {exit_data['exit_year']}")
                        if 'value' in exit_data:
                            print(f"    Value: {exit_data['value']}")
                        if 'acquirer' in exit_data and exit_data['exit_type'] == 'acquisition':
                            print(f"    Acquirer: {exit_data['acquirer']}")
            else:
                print("\nNo outcome data available")
        except Exception as e:
            print(f"\nError parsing outcome data: {e}")
            print(f"Raw data: {row['outcome_data']}")
    
    return pandas_df

# SQL version for direct use in Databricks notebook
def get_sql_query():
    """
    Returns a SQL query that can be run directly in a Databricks notebook
    to display outcome data in a structured way.
    """
    sql_query = """
    SELECT 
        Name,
        Project_Title,
        Competition_CD,
        level_1_score,
        level_2_score,
        level_5_score,
        total_score,
        outcome_data,
        -- Parse JSON fields for better display
        get_json_object(outcome_data, '$.primary_institute') as primary_institute,
        -- Count items in arrays
        size(from_json(get_json_object(outcome_data, '$.level1_ip'), 'ARRAY<STRUCT<title:STRING>>')) as ip_count,
        size(from_json(get_json_object(outcome_data, '$.level2_companies'), 'ARRAY<STRUCT<company_name:STRING>>')) as companies_count,
        size(from_json(get_json_object(outcome_data, '$.level5_exits'), 'ARRAY<STRUCT<company_name:STRING>>')) as exits_count
    FROM 
        grant_outcomes_results
    WHERE
        outcome_data IS NOT NULL AND
        outcome_data != 'null' AND
        outcome_data != '{}'
    -- Add additional filters as needed, e.g.:
    -- AND Name = 'Researcher Name'
    LIMIT 10
    """
    return sql_query

# Main execution for Databricks
def main():
    # Display outcome data for up to 5 records
    display_outcome_data(limit=5)
    
    # Print the SQL query for direct use in notebooks
    print("\n" + "="*80)
    print("SQL Query for Databricks Notebook:")
    print(get_sql_query())

if __name__ == "__main__":
    main()
