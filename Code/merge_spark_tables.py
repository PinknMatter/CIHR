# merge_spark_tables.py
# Script to merge data from "Silver" tables into the "Gold" table
# Designed to run in Databricks

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, when, lit

def initialize_spark():
    """Initialize Spark session"""
    spark = SparkSession.builder.appName("CIHR Grant Data Merger").getOrCreate()
    return spark

def load_gold_table(spark, database, table_name):
    """Load the Gold table (All Recipients CMZ, PoP, CHRP)"""
    print(f"Loading Gold table: {database}.{table_name}")
    
    # Load the gold table
    gold_df = spark.sql(f"SELECT * FROM {database}.{table_name}")
    
    # Create a clean name column for matching
    gold_df = gold_df.withColumn("clean_name", 
                               lower(trim(col("Name"))))
    
    print(f"Gold table loaded with {gold_df.count()} records")
    return gold_df

def find_silver_tables(spark, database, pattern):
    """Find all Silver tables in the database matching the pattern"""
    print(f"Finding Silver tables in {database} matching pattern '{pattern}'")
    
    # Get all tables in the database
    tables = spark.sql(f"SHOW TABLES IN {database}").collect()
    table_names = [row.tableName for row in tables]
    
    # Filter for tables matching the pattern
    silver_tables = [t for t in table_names if pattern in t]
    
    print(f"Found {len(silver_tables)} Silver tables")
    return silver_tables

def load_silver_tables(spark, database, silver_tables):
    """Load all Silver tables and prepare them for merging"""
    print(f"Loading {len(silver_tables)} Silver tables")
    
    all_silver_dfs = []
    
    for table in silver_tables:
        print(f"Processing: {table}")
        
        # Extract year from table name (customize this based on your table naming)
        # Example: cihr_investments_2010_11 -> 2010-11
        year_parts = [part for part in table.split('_') if part.isdigit()]
        fiscal_year = "-".join(year_parts) if year_parts else "Unknown"
        
        # Load the table
        silver_df = spark.sql(f"SELECT * FROM {database}.{table}")
        
        # Find name column
        name_candidates = ["NomineeNameEN_NomCandidatAN", "NomineeName_NomCandidat"]
        name_col = None
        
        for candidate in name_candidates:
            if candidate in silver_df.columns:
                name_col = candidate
                break
        
        if not name_col:
            # Try to find any column that might contain names
            name_candidates = [col for col in silver_df.columns if "name" in col.lower() or "nom" in col.lower()]
            if name_candidates:
                name_col = name_candidates[0]
            else:
                print(f"Warning: Could not find name column in {table}")
                continue
        
        # Create a clean name column for matching
        silver_df = silver_df.withColumn("clean_name", 
                                      lower(trim(col(name_col))))
        
        # Add fiscal year column
        silver_df = silver_df.withColumn("fiscal_year", lit(fiscal_year))
        
        # Select only the columns we need
        target_cols = [
            "PrimaryInstituteEN_InstitutPrincipalAN",
            "AllResearchCategoriesEN_TousCategoriesRechercheAN",
            "ApplicationKeywords_MotsClesDemande"
        ]
        
        select_cols = ["clean_name", "fiscal_year"]
        for target_col in target_cols:
            # Find the actual column name (case insensitive)
            actual_col = None
            for c in silver_df.columns:
                if c.lower() == target_col.lower():
                    actual_col = c
                    break
            
            if actual_col:
                if actual_col != target_col:
                    silver_df = silver_df.withColumnRenamed(actual_col, target_col)
                select_cols.append(target_col)
        
        silver_df = silver_df.select(*select_cols)
        all_silver_dfs.append(silver_df)
    
    # Union all silver dataframes
    if all_silver_dfs:
        combined_silver_df = all_silver_dfs[0]
        for df in all_silver_dfs[1:]:
            combined_silver_df = combined_silver_df.unionByName(df, allowMissingColumns=True)
        
        print(f"Combined Silver tables with {combined_silver_df.count()} records")
        return combined_silver_df
    else:
        print("No valid Silver tables found")
        return None

def merge_data(gold_df, silver_df):
    """Merge data from Silver tables into Gold table based on name matching"""
    print("Merging data from Silver tables into Gold table")
    
    # Join the dataframes on the clean name
    merged_df = gold_df.join(
        silver_df,
        gold_df["clean_name"] == silver_df["clean_name"],
        "left"
    )
    
    # Drop the duplicate clean_name column
    merged_df = merged_df.drop(silver_df["clean_name"])
    
    # Fill missing values with "Not Found"
    for col_name in ["PrimaryInstituteEN_InstitutPrincipalAN", 
                     "AllResearchCategoriesEN_TousCategoriesRechercheAN", 
                     "ApplicationKeywords_MotsClesDemande"]:
        if col_name in merged_df.columns:
            merged_df = merged_df.withColumn(
                col_name, 
                when(col(col_name).isNull(), "Not Found").otherwise(col(col_name))
            )
    
    print(f"Merged data with {merged_df.count()} records")
    return merged_df

def save_results(merged_df, database, output_table):
    """Save the merged data to a new table"""
    print(f"Saving merged data to: {database}.{output_table}")
    
    # Create or replace the output table
    merged_df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.{output_table}")
    
    print("Merged data saved successfully")

def main():
    """Main function to orchestrate the data merging process"""
    # Initialize Spark
    spark = initialize_spark()
    
    # Define database and table names
    database = "cihr"  # Update this to your actual database name
    gold_table = "all_recipients_cmz_pop_chrp"  # Update this to your actual gold table name
    silver_pattern = "cihr_investments"  # Update this to match your silver tables pattern
    output_table = "enriched_recipients"  # Name for the output table
    
    # Load Gold table
    gold_df = load_gold_table(spark, database, gold_table)
    
    # Find Silver tables
    silver_tables = find_silver_tables(spark, database, silver_pattern)
    
    # Load Silver tables
    silver_df = load_silver_tables(spark, database, silver_tables)
    
    if silver_df is not None:
        # Merge data
        merged_df = merge_data(gold_df, silver_df)
        
        # Save results
        save_results(merged_df, database, output_table)
    
    print("Data merging process completed")

# Entry point for the script
if __name__ == "__main__":
    main()
