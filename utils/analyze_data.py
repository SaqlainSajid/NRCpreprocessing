import json
import pandas as pd
import logging
from pathlib import Path
import ijson

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_csv(csv_path):
    """Analyze the structure of the CSV file"""
    logger.info(f"\nAnalyzing CSV file: {csv_path}")
    
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Basic info
        logger.info(f"\nCSV Structure:")
        logger.info(f"Number of rows: {len(df)}")
        logger.info(f"Columns: {', '.join(df.columns)}")
        
        # Look at NFI plot IDs
        logger.info("\nNFI Plot ID Analysis:")
        if 'nfi_plot' in df.columns:
            nfi_plots = df['nfi_plot'].unique()
            logger.info(f"Number of unique NFI plots: {len(nfi_plots)}")
            logger.info(f"Sample NFI plot IDs: {sorted(nfi_plots)[:5]}")
            logger.info(f"NFI plot ID type: {df['nfi_plot'].dtype}")
        
        # Look at poly_id format if it exists
        if 'poly_id' in df.columns:
            logger.info("\nPoly ID Analysis:")
            sample_poly_ids = df['poly_id'].head()
            logger.info(f"Sample Poly IDs: {sample_poly_ids.tolist()}")
        
        return df['nfi_plot'].unique() if 'nfi_plot' in df.columns else []
        
    except Exception as e:
        logger.error(f"Error analyzing CSV: {str(e)}")
        return []

def analyze_geojson(geojson_path):
    """Analyze the structure of the GeoJSON file"""
    logger.info(f"\nAnalyzing GeoJSON file: {geojson_path}")
    
    try:
        property_names = set()
        nfi_plot_values = set()
        poly_id_values = set()
        features_checked = 0
        
        with open(geojson_path, 'rb') as f:
            # Use ijson for memory-efficient parsing
            parser = ijson.parse(f)
            current_properties = None
            
            for prefix, event, value in parser:
                if prefix.endswith('.properties') and event == 'start_map':
                    current_properties = {}
                elif prefix.endswith('.properties') and event == 'end_map':
                    if features_checked < 1000:  # Check first 1000 features
                        # Check for NFI plot ID in POLY_ID
                        poly_id = current_properties.get('POLY_ID', '')
                        if poly_id:
                            poly_id_values.add(poly_id)
                            # Try to extract NFI plot ID from POLY_ID
                            parts = poly_id.split('_')
                            if len(parts) > 0:
                                nfi_plot_values.add(parts[0])
                        
                        features_checked += 1
                    else:
                        break
                        
                elif current_properties is not None and event == 'map_key':
                    property_names.add(value)
                elif current_properties is not None and prefix.endswith('.properties.' + value):
                    current_properties[value] = value
        
        logger.info("\nGeoJSON Analysis:")
        logger.info(f"Property names found: {sorted(property_names)}")
        logger.info(f"\nSample POLY_ID values ({len(poly_id_values)} found):")
        for poly_id in list(poly_id_values)[:5]:
            logger.info(f"- {poly_id}")
        
        logger.info(f"\nExtracted NFI plot IDs ({len(nfi_plot_values)} found):")
        for nfi_plot in sorted(list(nfi_plot_values))[:5]:
            logger.info(f"- {nfi_plot}")
            
        return nfi_plot_values
        
    except Exception as e:
        logger.error(f"Error analyzing GeoJSON: {str(e)}")
        return set()

def main():
    try:
        # Get the current directory
        current_dir = Path(__file__).parent
        
        # Define input paths relative to the script location
        geojson_path = current_dir / "all_pp_lc.geojson"
        csv_path = current_dir / "all_pp_poly_summ.csv"
        
        # Verify input files exist
        if not geojson_path.exists():
            raise FileNotFoundError(f"GeoJSON file not found: {geojson_path}")
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        logger.info(f"Found input files:")
        logger.info(f"GeoJSON: {geojson_path} ({geojson_path.stat().st_size / (1024*1024*1024):.2f} GB)")
        logger.info(f"CSV: {csv_path} ({csv_path.stat().st_size / (1024*1024):.2f} MB)")
        
        # Analyze both files
        csv_nfi_plots = analyze_csv(csv_path)
        geojson_nfi_plots = analyze_geojson(geojson_path)
        
        # Compare NFI plot IDs between files
        if csv_nfi_plots and geojson_nfi_plots:
            csv_set = set(str(x).strip() for x in csv_nfi_plots)
            geojson_set = set(str(x).strip() for x in geojson_nfi_plots)
            
            logger.info("\nComparing NFI plot IDs between files:")
            logger.info(f"NFI plots in CSV: {len(csv_set)}")
            logger.info(f"NFI plots in GeoJSON: {len(geojson_set)}")
            logger.info(f"NFI plots in both files: {len(csv_set & geojson_set)}")
            logger.info(f"NFI plots only in CSV: {len(csv_set - geojson_set)}")
            logger.info(f"NFI plots only in GeoJSON: {len(geojson_set - csv_set)}")
            
            if len(csv_set & geojson_set) == 0:
                logger.warning("No matching NFI plot IDs found between files!")
                logger.info("\nSample CSV NFI plots:")
                for plot_id in list(csv_set)[:5]:
                    logger.info(f"- {plot_id}")
                logger.info("\nSample GeoJSON NFI plots:")
                for plot_id in list(geojson_set)[:5]:
                    logger.info(f"- {plot_id}")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        logger.error("Stack trace:", exc_info=True)
        raise

if __name__ == "__main__":
    main()
