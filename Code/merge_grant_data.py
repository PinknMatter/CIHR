# merge_grant_data.py
# Script to merge data from "Silver" sheets into the "Gold" sheet
# Designed to run in Databricks

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, concat_ws, trim, when, lit
import os
from datetime import datetime

def initialize_spark():
    """Initialize Spark session"""
    spark = SparkSession.builder.appName("CIHR Grant Data Merger").getOrCreate()
    return spark

def load_gold_sheet(spark, gold_sheet_path):
    """Load the Gold sheet (All Recipients CMZ, PoP, CHRP)"""
    print(f"Loading Gold sheet from: {gold_sheet_path}")
    
    # Load the gold sheet
    gold_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(gold_sheet_path)
    
    # Create a clean name column for matching
    gold_df = gold_df.withColumn("clean_name", 
                                 lower(trim(col("Name"))))
    
    print(f"Gold sheet loaded with {gold_df.count()} records")
    return gold_df

def load_silver_sheets(spark, silver_dir_path):
    """Load all Silver sheets from the specified directory"""
    print(f"Loading Silver sheets from directory: {silver_dir_path}")
    
    # Get list of all Excel files in the directory
    silver_files = [f for f in os.listdir(silver_dir_path) if f.endswith('.xlsx')]
    print(f"Found {len(silver_files)} silver files")
    
    all_silver_dfs = []
    
    for file in silver_files:
        file_path = os.path.join(silver_dir_path, file)
        print(f"Processing: {file}")
        
        # Extract year from filename (format: cihr_investments_investissements_irsc_YYYYMM.xlsx)
        year_str = file.split('_')[-1].split('.')[0]
        fiscal_year = year_str[:4] + "-" + year_str[4:6]
        
        # Load the Excel file
        silver_df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("dataAddress", "'Sheet1'!A1") \
            .load(file_path)
        
        # Check if the required columns exist
        required_columns = [
            "PrimaryInstituteEN_InstitutPrincipalAN",
            "AllResearchCategoriesEN_TousCategoriesRechercheAN",
            "ApplicationKeywords_MotsClesDemande"
        ]
        
        # Get actual column names (case insensitive)
        actual_columns = silver_df.columns
        column_mapping = {}
        
        for req_col in required_columns:
            for act_col in actual_columns:
                if req_col.lower() == act_col.lower():
                    column_mapping[req_col] = act_col
        
        # Select only the columns we need
        if "NomineeNameEN_NomCandidatAN" in actual_columns:
            name_col = "NomineeNameEN_NomCandidatAN"
        elif "NomineeName_NomCandidat" in actual_columns:
            name_col = "NomineeName_NomCandidat"
        else:
            # Try to find any column that might contain names
            name_candidates = [col for col in actual_columns if "name" in col.lower() or "nom" in col.lower()]
            if name_candidates:
                name_col = name_candidates[0]
            else:
                print(f"Warning: Could not find name column in {file}")
                continue
        
        # Create a clean name column for matching
        silver_df = silver_df.withColumn("clean_name", 
                                        lower(trim(col(name_col))))
        
        # Add fiscal year column
        silver_df = silver_df.withColumn("fiscal_year", lit(fiscal_year))
        
        # Select only the columns we need
        select_cols = ["clean_name", "fiscal_year"]
        for req_col, act_col in column_mapping.items():
            silver_df = silver_df.withColumnRenamed(act_col, req_col)
            select_cols.append(req_col)
        
        silver_df = silver_df.select(*select_cols)
        all_silver_dfs.append(silver_df)
    
    # Union all silver dataframes
    if all_silver_dfs:
        combined_silver_df = all_silver_dfs[0]
        for df in all_silver_dfs[1:]:
            combined_silver_df = combined_silver_df.unionByName(df, allowMissingColumns=True)
        
        print(f"Combined Silver sheets with {combined_silver_df.count()} records")
        return combined_silver_df
    else:
        print("No valid Silver sheets found")
        return None

def merge_data(gold_df, silver_df):
    """Merge data from Silver sheets into Gold sheet based on name matching"""
    print("Merging data from Silver sheets into Gold sheet")
    
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

def save_results(merged_df, output_path):
    """Save the merged data to a new CSV file"""
    print(f"Saving merged data to: {output_path}")
    
    # Write the merged data to a CSV file
    merged_df.write.format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(output_path)
    
    print("Merged data saved successfully")

def debug_file_access(spark, gold_sheet_path, silver_dir_path):
    """Debug function to verify file access"""
    print("\n=== DEBUGGING FILE ACCESS ===")
    
    # Check if gold sheet exists
    try:
        # In Databricks, use dbutils.fs.ls
        if 'dbutils' in globals():
            print("\nChecking Gold sheet using dbutils.fs.ls:")
            files = dbutils.fs.ls(os.path.dirname(gold_sheet_path))
            gold_file_name = os.path.basename(gold_sheet_path)
            gold_exists = any(f.name == gold_file_name for f in files)
            print(f"Gold sheet {gold_file_name} exists: {gold_exists}")
            
            print("\nListing files in Gold directory:")
            for f in files:
                print(f"  {f.name} ({f.size} bytes)")
        else:
            # Local environment
            print("\nChecking Gold sheet using os.path.exists:")
            gold_exists = os.path.exists(gold_sheet_path)
            print(f"Gold sheet exists: {gold_exists}")
            
            if gold_exists:
                gold_size = os.path.getsize(gold_sheet_path)
                print(f"Gold sheet size: {gold_size} bytes")
            
            print("\nListing files in Gold directory:")
            gold_dir = os.path.dirname(gold_sheet_path)
            for f in os.listdir(gold_dir):
                file_path = os.path.join(gold_dir, f)
                if os.path.isfile(file_path):
                    print(f"  {f} ({os.path.getsize(file_path)} bytes)")
    except Exception as e:
        print(f"Error checking Gold sheet: {e}")
    
    # Check silver directory
    try:
        if 'dbutils' in globals():
            print("\nChecking Silver directory using dbutils.fs.ls:")
            files = dbutils.fs.ls(silver_dir_path)
            excel_files = [f for f in files if f.name.endswith('.xlsx')]
            print(f"Found {len(excel_files)} Excel files in Silver directory")
            
            print("\nListing Excel files in Silver directory:")
            for f in excel_files:
                print(f"  {f.name} ({f.size} bytes)")
        else:
            # Local environment
            print("\nChecking Silver directory using os.path.exists:")
            silver_exists = os.path.exists(silver_dir_path)
            print(f"Silver directory exists: {silver_exists}")
            
            if silver_exists:
                excel_files = [f for f in os.listdir(silver_dir_path) if f.endswith('.xlsx')]
                print(f"Found {len(excel_files)} Excel files in Silver directory")
                
                print("\nListing Excel files in Silver directory:")
                for f in excel_files:
                    file_path = os.path.join(silver_dir_path, f)
                    print(f"  {f} ({os.path.getsize(file_path)} bytes)")
    except Exception as e:
        print(f"Error checking Silver directory: {e}")
    
    print("\n=== END DEBUGGING FILE ACCESS ===\n")

def debug_data_loading(spark, gold_df, silver_df=None):
    """Debug function to verify data loading"""
    print("\n=== DEBUGGING DATA LOADING ===")
    
    # Check gold dataframe
    print("\nGold DataFrame:")
    print(f"  Number of rows: {gold_df.count()}")
    print(f"  Number of columns: {len(gold_df.columns)}")
    print("  Columns: " + ", ".join(gold_df.columns))
    
    # Show a sample of the gold data
    print("\nGold DataFrame sample (5 rows):")
    gold_df.show(5, truncate=False)
    
    # Check silver dataframe if provided
    if silver_df is not None:
        print("\nSilver DataFrame:")
        print(f"  Number of rows: {silver_df.count()}")
        print(f"  Number of columns: {len(silver_df.columns)}")
        print("  Columns: " + ", ".join(silver_df.columns))
        
        # Show a sample of the silver data
        print("\nSilver DataFrame sample (5 rows):")
        silver_df.show(5, truncate=False)
    
    print("\n=== END DEBUGGING DATA LOADING ===\n")

def main():
    """Main function to orchestrate the data merging process"""
    # Initialize Spark
    spark = initialize_spark()
    
    # Define paths
    # These paths should be updated to match your Databricks environment
    gold_sheet_path = "/Volumes/cihr/default/grant_datasets_bronze/all_recipients_cmz_pop_chrp.csv"
    silver_dir_path = "/Volumes/cihr/default/grant_datasets_bronze/"
    output_path = "/Volumes/cihr/default/grant_datasets_silver/enriched_recipients"
    
    # Debug file access
    debug_file_access(spark, gold_sheet_path, silver_dir_path)
    
    # Load Gold sheet
    gold_df = load_gold_sheet(spark, gold_sheet_path)
    
    # Load Silver sheets
    silver_df = load_silver_sheets(spark, silver_dir_path)
    
    if silver_df is not None:
        # Merge data
        merged_df = merge_data(gold_df, silver_df)
        
        # Save results
        save_results(merged_df, output_path)
    
    print("Data merging process completed")

# Entry point for the script
if __name__ == "__main__":
    main()
