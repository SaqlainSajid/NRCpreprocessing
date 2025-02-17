import json
import logging
import os
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_geojson_file():
    """Find the GeoJSON file by searching in common locations"""
    possible_paths = [
        Path.cwd() / "all_pp_lc.geojson",
        Path.cwd().parent / "all_pp_lc.geojson",
        Path(r"C:\Users\LabAdmin\Desktop\NRC Data\all_pp_lc.geojson"),
        Path(r"C:\Users\saqla\OneDrive\Desktop\NRC_Data\Preprocessing\all_pp_land_cover.geojson")
    ]
    
    # Also search in current directory and parent directory for any .geojson files
    possible_paths.extend(Path.cwd().glob("*.geojson"))
    possible_paths.extend(Path.cwd().parent.glob("*.geojson"))
    
    for path in possible_paths:
        if path.exists():
            logger.info(f"Found GeoJSON file: {path}")
            logger.info(f"File size: {path.stat().st_size / (1024*1024*1024):.2f} GB")
            return str(path)
            
    logger.error("Could not find GeoJSON file in any of these locations:")
    for path in possible_paths:
        logger.error(f"- {path}")
    return None

def inspect_first_feature(geojson_path):
    """Just inspect the first feature of the GeoJSON file"""
    try:
        path = Path(geojson_path)
        if not path.exists():
            logger.error(f"GeoJSON file not found: {path}")
            return
            
        logger.info(f"Inspecting GeoJSON file: {path}")
        logger.info(f"File size: {path.stat().st_size / (1024*1024):.2f} MB")
        
        with path.open('r', encoding='utf-8') as f:
            # Read the first chunk to get at least one complete feature
            chunk = f.read(50000)  # Read first 50KB
            logger.info("\nFirst part of the file:")
            logger.info("-" * 80)
            logger.info(chunk[:1000])  # Show first 1000 characters
            logger.info("-" * 80)
            
            # Try to find the properties section
            prop_start = chunk.find('"properties"')
            if prop_start > -1:
                # Find the opening brace after "properties"
                brace_start = chunk.find("{", prop_start)
                if brace_start > -1:
                    # Find the matching closing brace
                    brace_count = 1
                    pos = brace_start + 1
                    while brace_count > 0 and pos < len(chunk):
                        if chunk[pos] == "{":
                            brace_count += 1
                        elif chunk[pos] == "}":
                            brace_count -= 1
                        pos += 1
                    
                    if brace_count == 0:
                        properties = chunk[brace_start:pos]
                        logger.info("\nFound properties section:")
                        logger.info("-" * 80)
                        logger.info(properties)
                        logger.info("-" * 80)
                        
                        # Try to parse it as JSON
                        try:
                            props = json.loads(properties)
                            logger.info("\nParsed properties:")
                            for key, value in props.items():
                                logger.info(f"- {key}: {value}")
                        except json.JSONDecodeError:
                            logger.warning("Could not parse properties as JSON")

    except Exception as e:
        logger.error(f"Error inspecting GeoJSON: {str(e)}")
        logger.error("Stack trace:", exc_info=True)

if __name__ == "__main__":
    geojson_path = find_geojson_file()
    if geojson_path:
        inspect_first_feature(geojson_path)
