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
- `create_biomass_raster.py`: Creates Raster
- `convert_to_geojson.py`: Converts Shape file to geojson.py

## Usage

1. Prepare your input CSV file with UTM coordinates and zone information
2. Set the input and output paths in either script
3. Run the main script:

```bash
python main.py
```
4. Put your shape file in the root or specify the path, then run
```bash
python convert_to_geojson.py
```
5. modify the paths of your csv file and geojson file in create_biomass_raster.py then run 
```bash
python create_biomass_raster.py
```
This will create indices for biomass data and nfi ids for better search

### Input Data Requirements

The input CSV file should contain:
- UTM coordinates (easting and northing)
- UTM zone information
- Optional: Province information

### Output

The tool will create:
- Separate CSV files for each UTM zone
- Further subdivided files by province within each UTM zone


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
