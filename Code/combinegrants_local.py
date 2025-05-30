import pandas as pd
import re
from difflib import SequenceMatcher

def normalize_text(text):
    """Normalize text for better matching"""
    if not isinstance(text, str):
        return ""
    # Convert to lowercase
    text = text.lower()
    # Remove special characters
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)
    # Trim whitespace
    text = text.strip()
    return text

def levenshtein_distance(s1, s2):
    """Calculate Levenshtein distance between two strings"""
    if s1 == s2:
        return 0
    
    # Skip computation for strings with large length difference
    if abs(len(s1) - len(s2)) > 10:
        return 999
    
    # Use SequenceMatcher for a quick similarity ratio
    similarity = SequenceMatcher(None, s1, s2).ratio()
    # Convert similarity (0-1) to distance (lower is better)
    # This is a rough approximation but works well for our purposes
    distance = int((1 - similarity) * 20)
    
    return distance

def main():
    # Load both CSV files
    print("Loading CSV files...")
    try:
        grants_df = pd.read_csv("combined_grants.csv")
        outcomes_df = pd.read_csv("Check1.csv")
    except Exception as e:
        print(f"Error loading CSV files: {e}")
        print("Make sure 'combined_grants.csv' and 'Check1.csv' are in the current directory.")
        return
    
    # Print some statistics
    print(f"Total grants records: {len(grants_df)}")
    print(f"Total outcomes records: {len(outcomes_df)}")
    
    # Drop duplicates in grants dataframe
    if "ApplicationTitle_TitreDemande" in grants_df.columns:
        grants_df = grants_df.drop_duplicates(subset=["ApplicationTitle_TitreDemande"])
    else:
        print("Warning: 'ApplicationTitle_TitreDemande' column not found in grants file.")
        print(f"Available columns: {grants_df.columns.tolist()}")
        # Try to find a suitable column for project title
        title_columns = [col for col in grants_df.columns if 'title' in col.lower() or 'titre' in col.lower()]
        if title_columns:
            print(f"Using '{title_columns[0]}' as the title column")
            grants_df = grants_df.drop_duplicates(subset=[title_columns[0]])
        else:
            print("No suitable title column found. Using all records.")
    
    # Normalize text for better matching
    print("Normalizing text for better matching...")
    
    # Add normalized columns to outcomes dataframe
    if "Project_Title" in outcomes_df.columns:
        outcomes_df["normalized_Project_Title"] = outcomes_df["Project_Title"].apply(normalize_text)
    else:
        print("Warning: 'Project_Title' column not found in outcomes file.")
        print(f"Available columns: {outcomes_df.columns.tolist()}")
        return
    
    # Add normalized columns to grants dataframe
    if "ApplicationTitle_TitreDemande" in grants_df.columns:
        grants_df["normalized_ApplicationTitle_TitreDemande"] = grants_df["ApplicationTitle_TitreDemande"].apply(normalize_text)
        join_key_col = "ApplicationTitle_TitreDemande"
    elif title_columns:
        grants_df[f"normalized_{title_columns[0]}"] = grants_df[title_columns[0]].apply(normalize_text)
        join_key_col = title_columns[0]
    else:
        print("No suitable title column found in grants file. Cannot proceed.")
        return
    
    # Prepare grants dataframe for joining
    grants_for_join = grants_df.copy()
    grants_for_join = grants_for_join.rename(columns={
        join_key_col: "join_key",
        f"normalized_{join_key_col}": "normalized_join_key"
    })
    
    # Check for required enrichment columns
    enrichment_columns = {
        "PrimaryInstituteEN_InstitutPrincipalAN": "enriched_primary",
        "AllResearchCategoriesEN_TousCategoriesRechercheAN": "enriched_categories",
        "FundingStartDate_DatePremierVersement": "enriched_start",
        "FundingEndDate_DateDernierVersement": "enriched_end"
    }
    
    # Check which enrichment columns are available
    available_enrichment = {}
    for orig_col, new_col in enrichment_columns.items():
        if orig_col in grants_df.columns:
            available_enrichment[orig_col] = new_col
        else:
            print(f"Warning: Enrichment column '{orig_col}' not found in grants file.")
    
    if not available_enrichment:
        print("No enrichment columns found. Cannot proceed.")
        return
    
    # Rename enrichment columns
    for orig_col, new_col in available_enrichment.items():
        grants_for_join = grants_for_join.rename(columns={orig_col: new_col})
    
    # Step 2: First try exact matching on normalized text
    print("Performing exact matching on normalized text...")
    
    # Merge dataframes on normalized text (exact match)
    exact_joined_df = pd.merge(
        outcomes_df,
        grants_for_join[["join_key", "normalized_join_key"] + list(available_enrichment.values())],
        left_on="normalized_Project_Title",
        right_on="normalized_join_key",
        how="left"
    )
    
    # Find unmatched records
    unmatched_df = exact_joined_df[exact_joined_df["join_key"].isna()].copy()
    unmatched_df = unmatched_df.drop(columns=[col for col in unmatched_df.columns if col not in outcomes_df.columns])
    
    print(f"Records matched with exact normalized matching: {len(exact_joined_df) - len(unmatched_df)}")
    print(f"Records not matched with exact normalized matching: {len(unmatched_df)}")
    
    # For unmatched records, try fuzzy matching
    if len(unmatched_df) > 0:
        print("Performing fuzzy matching for unmatched records...")
        
        # Get unique unmatched titles
        unmatched_titles = unmatched_df["normalized_Project_Title"].unique()
        print(f"Processing {len(unmatched_titles)} unique unmatched titles")
        
        # Prepare grants data for fuzzy matching
        grants_for_fuzzy = grants_for_join[["join_key", "normalized_join_key"] + list(available_enrichment.values())].copy()
        
        # Process each unmatched title
        fuzzy_matches = []
        
        for title in unmatched_titles:
            if not isinstance(title, str) or not title.strip():
                continue
                
            # Filter grants by length similarity
            title_len = len(title)
            length_filtered_grants = grants_for_fuzzy[
                (grants_for_fuzzy["normalized_join_key"].str.len() >= title_len - 10) & 
                (grants_for_fuzzy["normalized_join_key"].str.len() <= title_len + 10)
            ].copy()  # Create an explicit copy to avoid SettingWithCopyWarning
            
            # Calculate Levenshtein distance using .loc to avoid warnings
            length_filtered_grants.loc[:, "levenshtein_distance"] = length_filtered_grants["normalized_join_key"].apply(
                lambda x: levenshtein_distance(title, x)
            )
            
            # Get the best match
            threshold = 8  # Increased threshold to match more records
            if not length_filtered_grants.empty:
                best_match = length_filtered_grants.sort_values("levenshtein_distance").iloc[0]
                
                # Only keep matches below threshold
                if best_match["levenshtein_distance"] <= threshold:
                    match_dict = best_match.to_dict()
                    match_dict["original_title"] = title
                    fuzzy_matches.append(match_dict)
        
        # Start with the original outcomes dataframe to ensure all records are included
        combined_df = outcomes_df.copy()
        
        # Create a dictionary to map Project_Title to enrichment data
        enrichment_map = {}
        
        # Process exact matches
        exact_matches = exact_joined_df[~exact_joined_df["join_key"].isna()].copy()
        for idx, row in exact_matches.iterrows():
            key = row["Project_Title"]
            enrichment_map[key] = {}
            for source_col, target_col in enrichment_mapping.items():
                if source_col in available_enrichment.values() and source_col in row:
                    enrichment_map[key][target_col] = row[source_col]
        
        # Process fuzzy matches if any
        if fuzzy_matches:
            fuzzy_matches_df = pd.DataFrame(fuzzy_matches)
            
            # Join fuzzy matches back to unmatched records
            fuzzy_joined = pd.merge(
                unmatched_df,
                fuzzy_matches_df[["original_title", "join_key"] + list(available_enrichment.values()) + ["levenshtein_distance"]],
                left_on="normalized_Project_Title",
                right_on="original_title",
                how="inner"
            )
            
            print(f"Records matched with fuzzy matching: {len(fuzzy_joined)}")
            
            # Add fuzzy matches to the enrichment map
            for idx, row in fuzzy_joined.iterrows():
                key = row["Project_Title"]
                enrichment_map[key] = {}
                for source_col, target_col in enrichment_mapping.items():
                    if source_col in available_enrichment.values() and source_col in row:
                        enrichment_map[key][target_col] = row[source_col]
            
            # Calculate how many records are still unmatched
            matched_count = len(exact_matches) + len(fuzzy_joined)
            still_unmatched = len(outcomes_df) - matched_count
            print(f"Records still unmatched after fuzzy matching: {still_unmatched}")
        else:
            print("No fuzzy matches found within threshold")
            # Calculate how many records are still unmatched
            matched_count = len(exact_matches)
            still_unmatched = len(outcomes_df) - matched_count
            print(f"Records still unmatched after fuzzy matching: {still_unmatched}")
    else:
        # If no unmatched records, just use the exact matches
        matched_count = len(exact_joined_df) - len(unmatched_df)
        print(f"All records matched with exact matching: {matched_count}")
        
        # Start with the original outcomes dataframe to ensure all records are included
        combined_df = outcomes_df.copy()
        
        # Create a dictionary to map Project_Title to enrichment data
        enrichment_map = {}
        
        # Process exact matches
        exact_matches = exact_joined_df[~exact_joined_df["join_key"].isna()].copy()
        for idx, row in exact_matches.iterrows():
            key = row["Project_Title"]
            enrichment_map[key] = {}
            for source_col, target_col in enrichment_mapping.items():
                if source_col in available_enrichment.values() and source_col in row:
                    enrichment_map[key][target_col] = row[source_col]
    
    # Step 3: Apply the enrichment data to the combined dataframe
    print("Applying enrichment data...")
    
    # Map from enriched column names to target column names
    enrichment_mapping = {
        "enriched_primary": "Primary_Institute",
        "enriched_categories": "Research_Categories",
        "enriched_start": "Funding_Start",
        "enriched_end": "Funding_End"
    }
    
    # Initialize enrichment columns with None
    for target_col in enrichment_mapping.values():
        combined_df[target_col] = None
    
    # Apply the enrichment data from our map
    for idx, row in combined_df.iterrows():
        key = row["Project_Title"]
        if key in enrichment_map:
            for target_col, value in enrichment_map[key].items():
                combined_df.at[idx, target_col] = value
    
    # The enriched dataframe is now the combined dataframe
    enriched_df = combined_df
    
    # Drop any temporary columns we might have added
    temp_columns = ["normalized_Project_Title"]
    temp_columns = [col for col in temp_columns if col in enriched_df.columns]
    if temp_columns:
        enriched_df = enriched_df.drop(columns=temp_columns)
        
    # Step 4: Save the enriched data
    output_file = "enriched_outcomes.csv"
    
    # Verify row count before saving
    original_count = len(outcomes_df)
    enriched_count = len(enriched_df)
    
    # Check for duplicates
    duplicates = enriched_df.duplicated().sum()
    if duplicates > 0:
        print(f"WARNING: Found {duplicates} duplicate rows. Removing duplicates...")
        enriched_df = enriched_df.drop_duplicates().reset_index(drop=True)
    
    # Check for row count mismatch
    if len(enriched_df) != original_count:
        print(f"WARNING: Row count mismatch! Original: {original_count}, Enriched: {len(enriched_df)}")
        
        if len(enriched_df) < original_count:
            print("Some records are missing. This shouldn't happen with our approach.")
        else:
            print("There are more records in the output than in the input. Fixing...")
            # Create a unique identifier for each record
            if "Name" in outcomes_df.columns and "Project_Title" in outcomes_df.columns:
                outcomes_df["unique_id"] = outcomes_df["Name"].astype(str) + "_" + outcomes_df["Project_Title"].astype(str)
                # Keep only records that were in the original dataset
                enriched_df["unique_id"] = enriched_df["Name"].astype(str) + "_" + enriched_df["Project_Title"].astype(str)
                original_ids = set(outcomes_df["unique_id"])
                enriched_df = enriched_df[enriched_df["unique_id"].isin(original_ids)].reset_index(drop=True)
                # Drop the temporary ID column
                enriched_df = enriched_df.drop(columns=["unique_id"])
                outcomes_df = outcomes_df.drop(columns=["unique_id"])
    
    # Final verification
    print(f"Final row count check - Original: {original_count}, Enriched: {len(enriched_df)}")
    
    # Save to CSV
    enriched_df.to_csv(output_file, index=False)
    
    print(f" All rows enriched and saved to: {output_file}")
    
    # Check enrichment statistics
    total_records = len(enriched_df)
    enriched_records = enriched_df["Primary_Institute"].notna().sum()
    enrichment_rate = (enriched_records / total_records) * 100 if total_records > 0 else 0
    
    print(f"\nEnrichment Statistics:")
    print(f"Total records: {total_records}")
    print(f"Enriched records: {enriched_records}")
    print(f"Enrichment rate: {enrichment_rate:.2f}%")
    
    # Show sample of enriched records
    print("\nSample of enriched records:")
    enriched_sample = enriched_df[enriched_df["Primary_Institute"].notna()].head(5)
    print(enriched_sample[["Project_Title", "Primary_Institute", "Research_Categories", "Funding_Start", "Funding_End"]].to_string(index=False))
    
    # Show sample of non-enriched records
    print("\nSample of non-enriched records:")
    non_enriched_sample = enriched_df[enriched_df["Primary_Institute"].isna()].head(5)
    print(non_enriched_sample[["Project_Title", "Primary_Institute", "Research_Categories", "Funding_Start", "Funding_End"]].to_string(index=False))

if __name__ == "__main__":
    main()
