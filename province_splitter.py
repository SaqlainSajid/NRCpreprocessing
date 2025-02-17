import pandas as pd
import os
from typing import Dict, List, Optional
from datetime import datetime

class ProvinceBoundaries:
    """Define UTM boundaries for Canadian provinces."""
    
    # Dictionary mapping province codes to full names
    PROVINCE_CODES = {
        'AB': 'Alberta',
        'BC': 'British Columbia',
        'MB': 'Manitoba',
        'NB': 'New Brunswick',
        'NL': 'Newfoundland and Labrador',
        'NS': 'Nova Scotia',
        'NT': 'Northwest Territories',
        'NU': 'Nunavut',
        'ON': 'Ontario',
        'PE': 'Prince Edward Island',
        'QC': 'Quebec',
        'SK': 'Saskatchewan',
        'YT': 'Yukon'
    }

    @staticmethod
    def get_province_boundaries(utm_zone: int) -> Dict[str, Dict]:
        """
        Get UTM boundaries for provinces in a specific UTM zone.
        Returns a dictionary with province codes as keys and their UTM boundaries as values.
        """
        # Define boundaries for each province in different UTM zones
        boundaries = {
            # Alberta (UTM zones 11-12)
            'AB': {
                11: {'e': (250000, 750000), 'n': (5400000, 6300000)},
                12: {'e': (250000, 750000), 'n': (5400000, 6300000)}
            },
            # British Columbia (UTM zones 8-11)
            'BC': {
                8: {'e': (350000, 750000), 'n': (5400000, 6600000)},
                9: {'e': (250000, 750000), 'n': (5400000, 6600000)},
                10: {'e': (250000, 750000), 'n': (5400000, 6600000)},
                11: {'e': (250000, 500000), 'n': (5400000, 6600000)}
            },
            # Saskatchewan (UTM zones 12-13)
            'SK': {
                12: {'e': (600000, 750000), 'n': (5400000, 6300000)},
                13: {'e': (250000, 750000), 'n': (5400000, 6300000)}
            },
            # Manitoba (UTM zones 14-15)
            'MB': {
                14: {'e': (400000, 750000), 'n': (5400000, 6300000)},
                15: {'e': (250000, 500000), 'n': (5400000, 6300000)}
            }
            # Add more provinces as needed
        }

        # Return only the boundaries for the specified UTM zone
        return {
            province: bounds[utm_zone]
            for province, bounds in boundaries.items()
            if utm_zone in bounds
        }

def determine_province_by_coordinates(row: pd.Series, utm_zone: int, boundaries: Dict) -> Optional[str]:
    """
    Determine the province based on UTM coordinates.
    Returns province code if coordinates fall within a province's boundaries, None otherwise.
    """
    utm_e, utm_n = row['utm_e'], row['utm_n']
    
    for province, bounds in boundaries.items():
        e_min, e_max = bounds['e']
        n_min, n_max = bounds['n']
        
        if (e_min <= utm_e <= e_max) and (n_min <= utm_n <= n_max):
            return province
    
    return None

def split_by_province(input_file: str, output_dir: str) -> None:
    """
    Split a CSV file into multiple files based on provinces.
    If province information is available in the data, use that.
    Otherwise, determine province based on UTM coordinates.
    Only includes data points after December 31, 2015.
    
    Parameters:
        input_file (str): Path to input CSV file
        output_dir (str): Directory to save output files
    """
    try:
        # Read the CSV file
        data = pd.read_csv(input_file)
        
        # Check for required columns
        if 'utm_e' not in data.columns or 'utm_n' not in data.columns:
            raise ValueError("Missing required columns: utm_e and utm_n")

        # Parse and filter dates
        cutoff_date = datetime(2015, 12, 31)
        
        if 'sample_date' in data.columns:
            data['parsed_date'] = pd.to_datetime(data['sample_date'], format='%Y-%m-%d %H:%M')
            data = data[data['parsed_date'] > cutoff_date]
        elif 'meas_date' in data.columns:
            # Using a more flexible date parser since meas_date could be in different formats
            data['parsed_date'] = pd.to_datetime(data['meas_date'])
            # print some of the data to see if it's in a format we're expecting
            print(data['parsed_date'].head())
            data = data[data['parsed_date'] > cutoff_date]
        else:
            raise ValueError("Neither sample_date nor meas_date column found in the data")
        
        # Drop the parsed_date column as it's no longer needed
        data = data.drop('parsed_date', axis=1)
        
        # Check if we have any data left after filtering
        if len(data) == 0:
            print("No data points found after December 31, 2015")
            return
        
        # Check if we have a single UTM zone
        if 'utm_zone' in data.columns:
            unique_zones = data['utm_zone'].unique()
            if len(unique_zones) > 1:
                raise ValueError(f"Multiple UTM zones found: {unique_zones}. Please split by UTM zone first.")
            utm_zone = unique_zones[0]
        else:
            raise ValueError("UTM zone information is missing")
        
        # Get province boundaries for this UTM zone
        province_boundaries = ProvinceBoundaries.get_province_boundaries(utm_zone)
        
        # Create output directory
        parent_folder = os.path.basename(os.path.dirname(input_file))
        output_subdir = os.path.join(output_dir, f"{parent_folder}_provinces")
        os.makedirs(output_subdir, exist_ok=True)
        
        # Check if province information is available in the data
        province_col = next((col for col in data.columns 
                           if col.lower() in ['juris_id', 'province']), None)
        
        if province_col:
            print(f"Using existing province information from column: {province_col}")
            # Group by existing province information
            grouped = data.groupby(province_col)
        else:
            print("Determining provinces based on UTM coordinates")
            # Determine province for each point based on coordinates
            data['province'] = data.apply(
                lambda row: determine_province_by_coordinates(row, utm_zone, province_boundaries),
                axis=1
            )
            grouped = data.groupby('province')
        
        # Save each province's data to a separate file
        for province, group in grouped:
            if province is None:
                filename = "unknown_province.csv"
            else:
                # Try to get full province name, fallback to the code/name we have
                input_base_name = os.path.splitext(os.path.basename(input_file))[0]
                province_name = ProvinceBoundaries.PROVINCE_CODES.get(province, province)
                # Check parent folder name for pp or gp
                parent_folder = os.path.basename(os.path.dirname(input_file)).lower()
                prefix = ""
                if "pp" in parent_folder:
                    prefix = "pp_"
                elif "gp" in parent_folder:
                    prefix = "gp_"
                filename = f"{prefix}{province_name.lower().replace(' ', '_')}_{input_base_name}.csv"
            
            output_file = os.path.join(output_subdir, filename)
            group.to_csv(output_file, index=False)
            print(f"Saved {len(group)} points for {province or 'unknown province'} to {filename}")
        
        print(f"\nProcessing complete. Files saved in: {output_subdir}")
        
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    # Example usage
    input_csv = "C:/Users/saqla/OneDrive/Desktop/NRC_Data/Preprocessing/all_gp_trees_mixed/utm_zone_11.csv"
    output_directory = "C:/Users/saqla/OneDrive/Desktop/NRC_Data/Preprocessing"
    split_by_province(input_csv, output_directory)
