import fiona
from fiona.crs import from_epsg
import os
from tqdm import tqdm

# Register all drivers
fiona.drvsupport.supported_drivers['ESRI Shapefile'] = 'rw'
fiona.drvsupport.supported_drivers['GeoJSON'] = 'rw'

print("Reading shapefile...")
# Read the source shapefile
with fiona.open('all_pp_lc.shp', 'r') as source:
    print(f"Source CRS: {source.crs}")
    print(f"Source schema: {source.schema}")
    
    # Get total number of features
    num_features = len(source)
    print(f"Total features to convert: {num_features}")
    
    # Prepare the schema for the output GeoJSON, ensuring MultiPolygon geometry
    output_schema = source.schema.copy()
    output_schema['geometry'] = 'MultiPolygon'
    
    # Create the output GeoJSON
    print("Converting to GeoJSON...")
    with fiona.open('all_pp_lc.geojson', 'w',
                   driver='GeoJSON',
                   crs=source.crs,
                   schema=output_schema) as output:
        # Copy all features with progress bar
        for feature in tqdm(source, total=num_features, desc="Converting features"):
            # Ensure geometry is MultiPolygon
            if feature['geometry']['type'] == 'Polygon':
                feature['geometry']['type'] = 'MultiPolygon'
                feature['geometry']['coordinates'] = [feature['geometry']['coordinates']]
            output.write(feature)

print("Conversion completed successfully!")
