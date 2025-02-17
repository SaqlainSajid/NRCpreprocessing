import json
import logging
import ijson
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def inspect_geojson(geojson_path):
    """Inspect the structure of a GeoJSON file"""
    if not os.path.exists(geojson_path):
        logger.error(f"GeoJSON file not found: {geojson_path}")
        return
        
    logger.info(f"Inspecting GeoJSON file: {geojson_path}")
    logger.info(f"File size: {os.path.getsize(geojson_path) / (1024*1024):.2f} MB")
    
    # First try to get a sample of the structure
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            # Try to read the first few characters to verify it's valid JSON
            start = f.read(1000)
            logger.info("\nFirst 1000 characters of file:")
            logger.info(start)
            
            # Reset file pointer
            f.seek(0)
            
            # Read first feature to understand structure
            parser = ijson.parse(f)
            
            # Track the current path and collect property names
            property_names = set()
            sample_properties = []
            features_checked = 0
            current_properties = None
            
            for prefix, event, value in parser:
                if prefix.endswith('.properties') and event == 'start_map':
                    current_properties = {}
                elif prefix.endswith('.properties') and event == 'end_map':
                    if features_checked < 5 and current_properties:
                        sample_properties.append(current_properties)
                        features_checked += 1
                        current_properties = None
                elif current_properties is not None and event == 'map_key':
                    property_names.add(value)
                elif current_properties is not None and prefix.endswith('.properties.' + value):
                    current_properties[value] = value

            logger.info(f"\nFound {len(property_names)} unique property names:")
            for prop in sorted(property_names):
                logger.info(f"- {prop}")
            
            if sample_properties:
                logger.info("\nSample properties from first few features:")
                for i, props in enumerate(sample_properties):
                    logger.info(f"\nFeature {i+1} properties:")
                    for key, value in props.items():
                        logger.info(f"- {key}: {value}")
            else:
                logger.warning("No properties found in the first few features")

    except json.JSONDecodeError as je:
        logger.error(f"Invalid JSON format: {str(je)}")
    except Exception as e:
        logger.error(f"Error inspecting GeoJSON: {str(e)}")
        logger.error("Stack trace:", exc_info=True)

if __name__ == "__main__":
    # Try both possible paths
    paths_to_try = [
        r"C:\Users\saqla\OneDrive\Desktop\NRC_Data\Preprocessing\all_pp_lc.geojson"
    ]
    
    for path in paths_to_try:
        if os.path.exists(path):
            inspect_geojson(path)
            break
    else:
        logger.error("Could not find GeoJSON file in any of the expected locations:")
        for path in paths_to_try:
            logger.error(f"- {path}")
