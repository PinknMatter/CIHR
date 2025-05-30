import pandas as pd
import re

# Load the enriched data
enriched_df = pd.read_csv('enriched_outcomes.csv')
print(f"Total records: {len(enriched_df)}")
print(f"Matched records: {enriched_df['Primary_Institute'].notna().sum()}")
print(f"Unmatched records: {enriched_df['Primary_Institute'].isna().sum()}")

# Get unmatched records
unmatched_df = enriched_df[enriched_df['Primary_Institute'].isna()].copy()

# Load the grants data for comparison
grants_df = pd.read_csv('combined_grants.csv')

# Function to normalize text (same as in combinegrants_local.py)
def normalize_text(text):
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    return text

# Analyze patterns in unmatched records
print("\n=== ANALYSIS OF UNMATCHED RECORDS ===")

# 1. Check for length of project titles
unmatched_df['title_length'] = unmatched_df['Project_Title'].apply(lambda x: len(str(x)) if pd.notna(x) else 0)
print(f"\nAverage length of unmatched titles: {unmatched_df['title_length'].mean():.1f} characters")

# 2. Check for common words or patterns
unmatched_df['normalized_title'] = unmatched_df['Project_Title'].apply(normalize_text)
common_words = {}
for title in unmatched_df['normalized_title']:
    for word in title.split():
        if len(word) > 3:  # Skip short words
            common_words[word] = common_words.get(word, 0) + 1

print("\nMost common words in unmatched titles:")
for word, count in sorted(common_words.items(), key=lambda x: x[1], reverse=True)[:10]:
    print(f"  {word}: {count}")

# 3. Check for partial matches
print("\nChecking for partial matches...")

# Normalize grant titles
grants_df['normalized_title'] = grants_df['ApplicationTitle_TitreDemande'].apply(normalize_text)

# Function to find partial matches
def find_partial_matches(title, grant_titles, min_length=5):
    """Find grant titles that contain parts of the unmatched title or vice versa"""
    matches = []
    words = [w for w in title.split() if len(w) >= min_length]
    
    for grant_title in grant_titles:
        # Check if any significant word from unmatched title is in grant title
        for word in words:
            if word in grant_title:
                matches.append((grant_title, "word_in_grant"))
                break
                
        # Check if any significant part of grant title is in unmatched title
        grant_words = [w for w in grant_title.split() if len(w) >= min_length]
        for word in grant_words:
            if word in title:
                matches.append((grant_title, "word_in_unmatched"))
                break
    
    return matches[:5]  # Return up to 5 matches

# Sample 5 unmatched records for detailed analysis
sample_unmatched = unmatched_df.sample(min(5, len(unmatched_df)))
grant_titles = grants_df['normalized_title'].tolist()

print("\nSample analysis of potential partial matches:")
for idx, row in sample_unmatched.iterrows():
    title = row['normalized_title']
    print(f"\nUnmatched title: {row['Project_Title']}")
    print(f"Normalized: {title}")
    
    partial_matches = find_partial_matches(title, grant_titles)
    if partial_matches:
        print("  Potential partial matches:")
        for match, match_type in partial_matches:
            print(f"  - {match} ({match_type})")
    else:
        print("  No significant partial matches found")

# 4. Check for researcher name matches
print("\n\n=== RESEARCHER NAME MATCHING ===")
if 'Name' in unmatched_df.columns and 'NomineeNameEN_NomCandidatAN' in grants_df.columns:
    # Normalize researcher names
    unmatched_df['normalized_name'] = unmatched_df['Name'].apply(normalize_text)
    grants_df['normalized_name'] = grants_df['NomineeNameEN_NomCandidatAN'].apply(normalize_text)
    
    # Check for name matches
    print("\nChecking for researcher name matches...")
    for idx, row in sample_unmatched.iterrows():
        name = row['normalized_name']
        print(f"\nUnmatched researcher: {row['Name']}")
        print(f"Normalized: {name}")
        
        # Find grants with matching researcher
        name_matches = grants_df[grants_df['normalized_name'] == name]
        if not name_matches.empty:
            print(f"  Found {len(name_matches)} grants with matching researcher name")
            print("  Sample grant titles:")
            for title in name_matches['ApplicationTitle_TitreDemande'].head(3):
                print(f"  - {title}")
        else:
            print("  No exact name matches found")
else:
    print("Researcher name columns not found in both datasets")

# 5. Suggest improvements
print("\n\n=== SUGGESTED IMPROVEMENTS ===")
print("1. Implement word-based partial matching")
print("2. Try matching by researcher name + institution")
print("3. Use more aggressive text normalization (e.g., stemming/lemmatization)")
print("4. Reduce the Levenshtein distance threshold for fuzzy matching")
print("5. Try matching on other fields like Program_Name or Competition_CD")
