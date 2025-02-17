# Canadian Geographic Data Preprocessing Tool

This tool processes geographic data across Canadian provinces, specifically designed to handle UTM (Universal Transverse Mercator) coordinate data and organize it by province and UTM zones.

## Features

- Splits input data by UTM zones
- Automatically identifies provinces based on UTM coordinates
- Organizes data into separate files by province and UTM zone
- Supports all Canadian provinces and territories
- Handles large CSV datasets efficiently

## Project Structure

- `main.py`: Main script for splitting data by UTM zones
- `province_splitter.py`: Contains province boundary definitions and splitting logic

## Usage

1. Prepare your input CSV file with UTM coordinates and zone information
2. Set the input and output paths in either script
3. Run the main script:

```bash
python main.py
```

### Input Data Requirements

The input CSV file should contain:
- UTM coordinates (easting and northing)
- UTM zone information
- Optional: Province information

### Output

The tool will create:
- Separate CSV files for each UTM zone
- Further subdivided files by province within each UTM zone

## Supported Provinces

- Alberta (AB)
- British Columbia (BC)
- Manitoba (MB)
- New Brunswick (NB)
- Newfoundland and Labrador (NL)
- Nova Scotia (NS)
- Northwest Territories (NT)
- Nunavut (NU)
- Ontario (ON)
- Prince Edward Island (PE)
- Quebec (QC)
- Saskatchewan (SK)
- Yukon (YT)

## Technical Details

The tool uses predefined UTM boundaries for each province to accurately split and categorize the data. It handles edge cases where provinces span multiple UTM zones and ensures data is correctly attributed to the appropriate region.

## Dependencies

- pandas
- os
- typing

## Error Handling

The tool includes error handling for:
- Missing input files
- Invalid CSV formats
- Missing required columns
- Invalid UTM coordinates

## Contributing

Feel free to submit issues and enhancement requests.

## Last Updated

2025-01-07
