# Add this code as a new cell in your Databricks notebook
# Place it after the cell where you define the table names but before loading the data

print("\n=== DEBUGGING SPARK TABLE ACCESS ===")

# List all available databases
print("\nAvailable databases:")
spark.sql("SHOW DATABASES").show()

# Define your database and table names
# Update these to match your actual database and table names
gold_database = "cihr"  # or "default" or whatever your database is called
gold_table = "all_recipients_cmz_pop_chrp"  # your gold table name
silver_database = "cihr"  # or "default" or whatever your database is called

# Check if the gold table exists
print(f"\nChecking Gold table: {gold_database}.{gold_table}")
try:
    tables = spark.sql(f"SHOW TABLES IN {gold_database}").collect()
    table_names = [row.tableName for row in tables]
    
    if gold_table in table_names:
        print(f"✅ Gold table exists: {gold_database}.{gold_table}")
        
        # Get table information
        print("\nGold table information:")
        spark.sql(f"DESCRIBE TABLE {gold_database}.{gold_table}").show(truncate=False)
        
        # Count rows
        row_count = spark.sql(f"SELECT COUNT(*) FROM {gold_database}.{gold_table}").collect()[0][0]
        print(f"Gold table row count: {row_count}")
        
        # Show sample data
        print("\nGold table sample data:")
        spark.sql(f"SELECT * FROM {gold_database}.{gold_table} LIMIT 5").show(truncate=False)
    else:
        print(f"❌ Gold table NOT found: {gold_database}.{gold_table}")
        print(f"Available tables in {gold_database}:")
        for table in table_names:
            print(f"  {table}")
except Exception as e:
    print(f"❌ Error checking Gold table: {e}")

# List all tables in the silver database
print(f"\nListing tables in Silver database: {silver_database}")
try:
    tables = spark.sql(f"SHOW TABLES IN {silver_database}").collect()
    table_names = [row.tableName for row in tables]
    
    # Look for tables that might be your silver tables
    print(f"Found {len(table_names)} tables in {silver_database} database")
    
    # Filter for tables that might be your silver tables (customize this pattern)
    silver_pattern = "cihr_investments"  # Adjust this pattern to match your silver tables
    potential_silver_tables = [t for t in table_names if silver_pattern in t]
    
    if potential_silver_tables:
        print(f"\nPotential Silver tables found ({len(potential_silver_tables)}):")
        for table in potential_silver_tables:
            # Count rows
            row_count = spark.sql(f"SELECT COUNT(*) FROM {silver_database}.{table}").collect()[0][0]
            print(f"  {table} ({row_count} rows)")
            
            # Show sample of first silver table
            if table == potential_silver_tables[0]:
                print(f"\nSample data from {table}:")
                spark.sql(f"SELECT * FROM {silver_database}.{table} LIMIT 3").show(truncate=False)
                
                print(f"\nSchema for {table}:")
                spark.sql(f"DESCRIBE TABLE {silver_database}.{table}").show(truncate=False)
    else:
        print(f"No potential Silver tables found matching pattern '{silver_pattern}'")
        print("All available tables:")
        for table in table_names:
            print(f"  {table}")
except Exception as e:
    print(f"❌ Error listing Silver tables: {e}")

print("\n=== END DEBUGGING SPARK TABLE ACCESS ===")

# Add this code as another cell to debug the dataframes after loading

print("\n=== DEBUGGING DATAFRAMES ===")

# Check if gold_df exists and has data
if 'gold_df' in locals():
    print(f"\n✅ Gold dataframe exists with {gold_df.count()} rows and {len(gold_df.columns)} columns")
    print("\nGold dataframe schema:")
    gold_df.printSchema()
    
    # Check for the Name column which is critical for matching
    if "Name" in gold_df.columns:
        print("\n✅ Name column found in Gold dataframe")
        # Show sample of names
        print("\nSample names:")
        name_samples = gold_df.select("Name").limit(10).collect()
        for row in name_samples:
            print(f"  {row['Name']}")
    else:
        print("\n❌ Name column NOT found in Gold dataframe")
        print("Available columns:", gold_df.columns)
        
    # Show sample data
    print("\nGold dataframe sample data:")
    gold_df.show(5, truncate=False)
else:
    print("❌ Gold dataframe not loaded")

# Check if silver dataframes exist
if 'all_silver_dfs' in locals() and all_silver_dfs:
    print(f"\n✅ {len(all_silver_dfs)} Silver dataframes loaded")
    
    for i, df in enumerate(all_silver_dfs):
        print(f"\nSilver dataframe #{i+1}:")
        print(f"  Rows: {df.count()}")
        print(f"  Columns: {len(df.columns)}")
        print("  Column names:", df.columns)
        
        # Show sample data for first dataframe
        if i == 0:
            print("\nSample data:")
            df.show(3, truncate=False)
elif 'combined_silver_df' in locals():
    print(f"\n✅ Combined Silver dataframe exists with {combined_silver_df.count()} rows")
    print("\nCombined Silver dataframe schema:")
    combined_silver_df.printSchema()
    
    # Check for key columns
    key_columns = ["clean_name", "PrimaryInstituteEN_InstitutPrincipalAN", 
                  "AllResearchCategoriesEN_TousCategoriesRechercheAN", 
                  "ApplicationKeywords_MotsClesDemande"]
    
    for col in key_columns:
        if col in combined_silver_df.columns:
            print(f"✅ Column found: {col}")
        else:
            print(f"❌ Column NOT found: {col}")
    
    # Show sample data
    print("\nCombined Silver dataframe sample data:")
    combined_silver_df.show(5, truncate=False)
else:
    print("❌ No Silver dataframes loaded")

print("\n=== END DEBUGGING DATAFRAMES ===")
