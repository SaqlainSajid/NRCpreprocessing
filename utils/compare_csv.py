import pandas as pd
import sys

def compare_csv_files(file1_path, file2_path):
    """
    Compare two CSV files and check if smaller file's data exists in the bigger file
    
    Args:
        file1_path (str): Path to first CSV file
        file2_path (str): Path to second CSV file
    """
    try:
        # Read the CSV files
        df1 = pd.read_csv(file1_path)
        df2 = pd.read_csv(file2_path)
        
        # Identify bigger and smaller dataframes
        bigger_df = df1 if df1.shape[0] >= df2.shape[0] else df2
        smaller_df = df2 if df1.shape[0] >= df2.shape[0] else df1
        bigger_file = file1_path if df1.shape[0] >= df2.shape[0] else file2_path
        smaller_file = file2_path if df1.shape[0] >= df2.shape[0] else file1_path
        
        print(f"\nBigger file: {bigger_file}")
        print(f"Shape: {bigger_df.shape}")
        print(f"\nSmaller file: {smaller_file}")
        print(f"Shape: {smaller_df.shape}\n")
        
        # Compare column names
        if not set(smaller_df.columns).issubset(set(bigger_df.columns)):
            print("The smaller file has columns that don't exist in the bigger file:")
            print(set(smaller_df.columns) - set(bigger_df.columns))
            return False
            
        # Check if smaller df's data exists in bigger df
        common_columns = list(set(smaller_df.columns) & set(bigger_df.columns))
        
        # Convert dataframes to sets of tuples for comparison
        smaller_records = set(map(tuple, smaller_df[common_columns].values))
        bigger_records = set(map(tuple, bigger_df[common_columns].values))
        
        # Find records that exist in smaller but not in bigger
        missing_records = smaller_records - bigger_records
        
        if not missing_records:
            print("All records from the smaller file exist in the bigger file!")
            return True
        else:
            print(f"Found {len(missing_records)} records in smaller file that don't exist in bigger file.")
            print("\nExample of missing records (up to 5):")
            for i, record in enumerate(list(missing_records)[:5]):
                print(f"\nRecord {i+1}:")
                for col, val in zip(common_columns, record):
                    print(f"{col}: {val}")
            return False
            
    except Exception as e:
        print(f"Error comparing files: {str(e)}")
        return False

if __name__ == "__main__":
        
    file1_path = 'C:/Users/saqla/OneDrive/Desktop/NRC_Data/All_Data/Canada_PP_Baseline/all_eosd_landcover/all_pp_eo/all_pp_photo_plot.csv'
    file2_path = 'C:/Users/saqla/OneDrive/Desktop/NRC_Data/All_Data/Canada_PP_Baseline/all_pp_landuse/all_pp_lu/all_pp_photo_plot.csv'
    compare_csv_files(file1_path, file2_path)
