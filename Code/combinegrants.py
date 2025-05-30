from pyspark.sql.functions import col, lower, trim, regexp_replace, length, levenshtein, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Step 1: Load both tables
grants_df = spark.table("cihr.default.combined_grants").dropDuplicates(["ApplicationTitle_TitreDemande"])
outcomes_df = spark.table("cihr.default.commercialization")

# Print some statistics about the data
print(f"Total grants records: {grants_df.count()}")
print(f"Total outcomes records: {outcomes_df.count()}")

# Function to normalize text for better matching
def normalize_text(df, col_name):
    return df.withColumn(
        f"normalized_{col_name}",
        lower(
            regexp_replace(
                regexp_replace(
                    trim(col(col_name)),
                    "[^a-zA-Z0-9\\s]", ""  # Remove special characters
                ),
                "\\s+", " "  # Replace multiple spaces with a single space
            )
        )
    )

# Normalize project titles in both dataframes
outcomes_df = normalize_text(outcomes_df, "Project_Title")
grants_df = normalize_text(grants_df, "ApplicationTitle_TitreDemande")

# Step 2: First try exact matching on normalized text
exact_joined_df = outcomes_df.alias("o").join(
    grants_df.select(
        col("ApplicationTitle_TitreDemande").alias("join_key"),
        col("normalized_ApplicationTitle_TitreDemande").alias("normalized_join_key"),
        col("PrimaryInstituteEN_InstitutPrincipalAN").alias("enriched_primary"),
        col("AllResearchCategoriesEN_TousCategoriesRechercheAN").alias("enriched_categories"),
        col("FundingStartDate_DatePremierVersement").alias("enriched_start"),
        col("FundingEndDate_DateDernierVersement").alias("enriched_end")
    ).alias("g"),
    col("o.normalized_Project_Title") == col("g.normalized_join_key"),
    how="left"
)

# Find unmatched records after exact matching
unmatched_df = exact_joined_df.filter(col("g.join_key").isNull()).select(
    "o.*"
)

print(f"Records matched with exact normalized matching: {exact_joined_df.count() - unmatched_df.count()}")
print(f"Records not matched with exact normalized matching: {unmatched_df.count()}")

# For unmatched records, try fuzzy matching
joined_df = exact_joined_df  # Default to exact matches only

if unmatched_df.count() > 0:
    # Instead of a full cross join, we'll use a more efficient approach
    # First, collect the unmatched project titles to Python (should be a manageable number)
    unmatched_titles = [row.normalized_Project_Title for row in unmatched_df.select("normalized_Project_Title").distinct().collect()]
    
    print(f"Processing {len(unmatched_titles)} unique unmatched titles")
    
    # Create a broadcast variable for faster lookups
    # This is much more efficient than a cross join
    grants_for_fuzzy = grants_df.select(
        col("ApplicationTitle_TitreDemande").alias("join_key"),
        col("normalized_ApplicationTitle_TitreDemande").alias("normalized_join_key"),
        col("PrimaryInstituteEN_InstitutPrincipalAN").alias("enriched_primary"),
        col("AllResearchCategoriesEN_TousCategoriesRechercheAN").alias("enriched_categories"),
        col("FundingStartDate_DatePremierVersement").alias("enriched_start"),
        col("FundingEndDate_DateDernierVersement").alias("enriched_end")
    ).cache()  # Cache this dataframe since we'll reuse it
    
    # Register a UDF for Levenshtein distance calculation
    from pyspark.sql.types import IntegerType
    
    @F.udf(IntegerType())
    def levenshtein_udf(s1, s2):
        if s1 is None or s2 is None:
            return None
        if s1 == s2:
            return 0
        
        # Simple Levenshtein implementation
        m, n = len(s1), len(s2)
        if m < n:
            s1, s2 = s2, s1
            m, n = n, m
        
        # Skip computation for strings with large length difference
        if m - n > 10:  # If length difference is too large, not worth comparing
            return 999
        
        # Initialize distance matrix
        d = [[i if j == 0 else j if i == 0 else 0 for j in range(n + 1)] for i in range(m + 1)]
        
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                cost = 0 if s1[i-1] == s2[j-1] else 1
                d[i][j] = min(d[i-1][j] + 1,      # deletion
                              d[i][j-1] + 1,      # insertion
                              d[i-1][j-1] + cost) # substitution
        
        return d[m][n]
    
    # Process each unmatched title separately with filtering
    fuzzy_matches = []
    
    for title in unmatched_titles:
        # Filter grants to only include those with similar length
        # This drastically reduces the number of comparisons
        title_len = len(title) if title else 0
        
        # Filter by length first (much faster than computing Levenshtein for all)
        length_filtered_grants = grants_for_fuzzy.filter(
            (length(col("normalized_join_key")) >= title_len - 10) & 
            (length(col("normalized_join_key")) <= title_len + 10)
        )
        
        # Then compute Levenshtein distance for this smaller set
        distances_df = length_filtered_grants.withColumn(
            "levenshtein_distance", 
            levenshtein_udf(lit(title), col("normalized_join_key"))
        )
        
        # Get the best match for this title
        threshold = 5  # Adjust as needed
        best_match = distances_df.orderBy("levenshtein_distance").limit(1)
        
        # Only keep matches below threshold
        best_match_below_threshold = best_match.filter(col("levenshtein_distance") <= threshold)
        
        # Add to our collection if we found a match
        if best_match_below_threshold.count() > 0:
            # Add the original title for joining later
            match_with_title = best_match_below_threshold.withColumn("original_title", lit(title))
            fuzzy_matches.append(match_with_title)
    
    # If we found any fuzzy matches, union them together
    if fuzzy_matches:
        fuzzy_matches_df = fuzzy_matches[0]
        for df in fuzzy_matches[1:]:
            fuzzy_matches_df = fuzzy_matches_df.unionByName(df)
        
        # Join the fuzzy matches back to the original unmatched records
        fuzzy_joined = unmatched_df.join(
            fuzzy_matches_df.select(
                col("original_title").alias("normalized_Project_Title"),
                col("join_key"),
                col("enriched_primary"),
                col("enriched_categories"),
                col("enriched_start"),
                col("enriched_end"),
                col("levenshtein_distance")
            ),
            "normalized_Project_Title",
            "inner"
        )
        
        print(f"Records matched with optimized fuzzy matching: {fuzzy_joined.count()}")
    
        # Prepare the fuzzy matches dataframe for joining with exact matches
        fuzzy_matches_df = fuzzy_joined.select(
            "*",
            col("join_key").alias("g.join_key"),
            col("enriched_primary").alias("g.enriched_primary"),
            col("enriched_categories").alias("g.enriched_categories"),
            col("enriched_start").alias("g.enriched_start"),
            col("enriched_end").alias("g.enriched_end")
        )
    else:
        print("No fuzzy matches found within threshold")
        fuzzy_matches_df = spark.createDataFrame([], unmatched_df.schema)
    
    # Combine exact matches with fuzzy matches
    exact_matches_df = exact_joined_df.filter(col("g.join_key").isNotNull())
    
    # Union the exact and fuzzy matches
    if fuzzy_matches_df.count() > 0:
        # Drop any columns from fuzzy_matches_df that would cause conflicts in the union
        columns_to_select = [c for c in fuzzy_matches_df.columns 
                          if c not in ["join_key", "enriched_primary", "enriched_categories", 
                                       "enriched_start", "enriched_end", "levenshtein_distance"]]
        
        fuzzy_formatted_df = fuzzy_matches_df.select(*columns_to_select)
        
        # Union the dataframes
        joined_df = exact_matches_df.unionByName(
            fuzzy_formatted_df,
            allowMissingColumns=True
        )
    else:
        joined_df = exact_matches_df
    
    # Find records that still don't have a match
    still_unmatched = unmatched_df.count() - (fuzzy_matches_df.count() if fuzzy_matches_df.count() > 0 else 0)
    print(f"Records still unmatched after fuzzy matching: {still_unmatched}")
    
    # Show some examples of unmatched records
    if still_unmatched > 0:
        print("\nSample of unmatched Project_Title values:")
        # Get the titles that didn't match
        if 'fuzzy_joined' in locals():
            matched_titles = [row.normalized_Project_Title for row in fuzzy_joined.select("normalized_Project_Title").collect()]
        else:
            matched_titles = []
            
        sample_unmatched = unmatched_df.filter(~col("normalized_Project_Title").isin(matched_titles)).select(
            "Project_Title", "normalized_Project_Title"
        ).limit(5)
        
        sample_unmatched.show(truncate=False)

# Step 3: Replace the 4 enrichment columns
enriched_df = joined_df.select(
    *[col(f"o.{c}") for c in outcomes_df.columns if c not in [
        "Primary_Institute", "Research_Categories", "Funding_Start", "Funding_End", "normalized_Project_Title"
    ]],
    col("g.enriched_primary").alias("Primary_Institute"),
    col("g.enriched_categories").alias("Research_Categories"),
    col("g.enriched_start").alias("Funding_Start"),
    col("g.enriched_end").alias("Funding_End")
)

# Step 4: Overwrite the target table with enriched data
enriched_df.write.format("delta").mode("overwrite").saveAsTable("cihr.default.commercialization")

print("✅ All rows enriched and saved to: cihr.default.commercialization")

# Optional: Check enrichment statistics
total_records = enriched_df.count()
enriched_records = enriched_df.filter(col("Primary_Institute").isNotNull()).count()
enrichment_rate = (enriched_records / total_records) * 100 if total_records > 0 else 0

print(f"\nEnrichment Statistics:")
print(f"Total records: {total_records}")
print(f"Enriched records: {enriched_records}")
print(f"Enrichment rate: {enrichment_rate:.2f}%")

# Show sample of enriched records
print("\nSample of enriched records:")
enriched_df.select(
    "Project_Title",
    "Primary_Institute",
    "Research_Categories",
    "Funding_Start",
    "Funding_End"
).where(col("Primary_Institute").isNotNull()).show(5, truncate=False)

# Show sample of non-enriched records
print("\nSample of non-enriched records:")
enriched_df.select(
    "Project_Title",
    "Primary_Institute",
    "Research_Categories",
    "Funding_Start",
    "Funding_End"
).where(col("Primary_Institute").isNull()).show(5, truncate=False)
