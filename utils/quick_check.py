import pandas as pd
import json
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_csv_nfi_plots():
    current_dir = Path.cwd()
    csv_path = current_dir / "all_pp_poly_summ.csv"
    
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return set()
        
    logger.info(f"\nReading CSV file: {csv_path}")
    logger.info(f"File size: {csv_path.stat().st_size / (1024*1024):.2f} MB")
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Check NFI plot IDs
        if 'nfi_plot' in df.columns:
            nfi_plots = df['nfi_plot'].unique()
            logger.info(f"\nFound {len(nfi_plots)} unique NFI plots in CSV")
            logger.info("\nSample NFI plots from CSV:")
            for plot in sorted(nfi_plots)[:10]:
                logger.info(f"- {plot}")
                
            # Check if we have the plots mentioned in the error logs
            test_plots = [1066826, 1073681, 1073686, 1073691, 1073696]
            logger.info("\nChecking for specific NFI plots that failed:")
            for plot in test_plots:
                if plot in nfi_plots:
                    sample_row = df[df['nfi_plot'] == plot].iloc[0]
                    logger.info(f"\nFound plot {plot}:")
                    logger.info(f"- poly_id: {sample_row['poly_id']}")
                    logger.info(f"- Full row: {sample_row.to_dict()}")
                else:
                    logger.info(f"Plot {plot} not found in CSV")
            
            return set(str(x) for x in nfi_plots)
        else:
            logger.error("No 'nfi_plot' column found in CSV")
            logger.info(f"Available columns: {df.columns.tolist()}")
            return set()
            
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return set()

def check_geojson_nfi_plots():
    current_dir = Path.cwd()
    geojson_path = current_dir / "all_pp_lc.geojson"
    
    if not geojson_path.exists():
        logger.error(f"GeoJSON file not found: {geojson_path}")
        return set()
        
    logger.info(f"\nReading GeoJSON file: {geojson_path}")
    logger.info(f"File size: {geojson_path.stat().st_size / (1024*1024*1024):.2f} GB")
    
    try:
        nfi_plots = set()
        features_checked = 0
        
        with open(geojson_path, 'r', encoding='utf-8') as f:
            # Read and process the file in chunks
            chunk_size = 10000
            chunk = f.read(chunk_size)
            
            while chunk and features_checked < 100:  # Check first 100 features
                # Look for POLY_ID patterns
                if '"POLY_ID"' in chunk:
                    start = chunk.find('"POLY_ID"')
                    value_start = chunk.find(':', start)
                    if value_start > -1:
                        value_end = chunk.find(',', value_start)
                        if value_end == -1:
                            value_end = chunk.find('}', value_start)
                        if value_end > -1:
                            poly_id = chunk[value_start+1:value_end].strip().strip('"')
                            logger.info(f"Found POLY_ID: {poly_id}")
                            
                            # Try to extract NFI plot ID from POLY_ID
                            parts = poly_id.split('_')
                            if len(parts) > 0:
                                try:
                                    nfi_plot = parts[0].strip()
                                    nfi_plots.add(nfi_plot)
                                    features_checked += 1
                                except:
                                    pass
                
                # Read next chunk
                chunk = f.read(chunk_size)
        
        logger.info(f"\nFound {len(nfi_plots)} unique NFI plots in first {features_checked} features")
        logger.info("\nSample NFI plots from GeoJSON:")
        for plot in sorted(list(nfi_plots))[:10]:
            logger.info(f"- {plot}")
            
        return nfi_plots
        
    except Exception as e:
        logger.error(f"Error reading GeoJSON: {e}")
        return set()

def main():
    try:
        # Get NFI plots from both files
        csv_plots = check_csv_nfi_plots()
        geojson_plots = check_geojson_nfi_plots()
        
        # Compare the sets
        if csv_plots and geojson_plots:
            common_plots = csv_plots & geojson_plots
            logger.info(f"\nComparison results:")
            logger.info(f"- CSV plots: {len(csv_plots)}")
            logger.info(f"- GeoJSON plots: {len(geojson_plots)}")
            logger.info(f"- Common plots: {len(common_plots)}")
            
            if common_plots:
                logger.info("\nSample common plots:")
                for plot in sorted(list(common_plots))[:5]:
                    logger.info(f"- {plot}")
            else:
                logger.warning("No common plots found between CSV and GeoJSON!")
                
    except Exception as e:
        logger.error(f"Error in main: {e}")
        logger.error("Stack trace:", exc_info=True)

if __name__ == "__main__":
    main()
