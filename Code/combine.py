import pandas as pd
import os
import sys
from datetime import datetime

# Folder with your Excel files
folder_path = r"C:\Users\noahk\OneDrive\Desktop\Project Jayce\DataSets\Grants"
sheet_name = "G&A_S&B"
output_dir = r"C:\Users\noahk\OneDrive\Desktop\Project Jayce\CIHR\Code"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = os.path.join(output_dir, f"combined_grants_{timestamp}.xlsx")

try:
    # Check if the folder exists
    if not os.path.exists(folder_path):
        print(f"❌ Error: The folder '{folder_path}' does not exist.")
        sys.exit(1)
    
    # List all Excel files in the folder
    excel_files = [f for f in os.listdir(folder_path) if f.endswith((".xlsx", ".xls"))]
    
    if not excel_files:
        print(f"❌ No Excel files found in '{folder_path}'.")
        sys.exit(1)
    
    print(f"📊 Found {len(excel_files)} Excel files to process.")
    
    # Create an empty list to store dataframes
    dfs = []
    
    # Process each Excel file
    for i, file in enumerate(excel_files, 1):
        file_path = os.path.join(folder_path, file)
        try:
            print(f"Processing [{i}/{len(excel_files)}]: {file}")
            
            # Check if the sheet exists in the file
            xls = pd.ExcelFile(file_path)
            if sheet_name not in xls.sheet_names:
                print(f"  ⚠️ Warning: Sheet '{sheet_name}' not found in '{file}'. Available sheets: {xls.sheet_names}")
                continue
            
            # Read the Excel file
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Add a column to identify the source file
            df['Source_File'] = file
            
            # Append to the list
            dfs.append(df)
            
        except Exception as e:
            print(f"  ❌ Error processing '{file}': {str(e)}")
    
    if not dfs:
        print("❌ No data could be extracted from any of the Excel files.")
        sys.exit(1)
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Save the combined data
    combined_df.to_excel(output_file, index=False)
    
    print(f"✅ Successfully combined {len(dfs)} files into '{output_file}'.")
    print(f"📈 Total rows: {len(combined_df)}")
    
except Exception as e:
    print(f"❌ An error occurred: {str(e)}")
    sys.exit(1)
