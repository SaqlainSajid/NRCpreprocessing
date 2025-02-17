import os
import pickle
import re
import json
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.transform import from_bounds, Affine
from rasterio.features import rasterize, geometry_mask
from rasterio.warp import reproject, Resampling
from shapely.geometry import shape, MultiPolygon
from shapely.ops import unary_union
from tqdm import tqdm
from pyproj import CRS
import ijson

# ESRI:102001 WKT definition
LAMBERT_CRS_WKT = '''PROJCS["NAD_1983_Canada_Lambert",
    GEOGCS["GCS_North_American_1983",
        DATUM["D_North_American_1983",
            SPHEROID["GRS_1980",6378137.0,298.257222101]],
        PRIMEM["Greenwich",0.0],
        UNIT["Degree",0.0174532925199433]],
    PROJECTION["Lambert_Conformal_Conic"],
    PARAMETER["False_Easting",0.0],
    PARAMETER["False_Northing",0.0],
    PARAMETER["Central_Meridian",-95.0],
    PARAMETER["Standard_Parallel_1",49.0],
    PARAMETER["Standard_Parallel_2",77.0],
    PARAMETER["Latitude_Of_Origin",49.0],
    UNIT["Meter",1.0]]'''


def create_indexes(geojson_path, csv_path, index_dir="indexes"):
    """
    Create index files for faster lookups using ijson.
    The GeoJSON index maps NFI_PLOT (as float) to a dictionary containing:
        - poly_ids: list of polygon IDs (as lower-case strings)
        - feature_positions: list of byte positions in the file for direct access
    The biomass index maps poly_id (as lower-case string) to its biomass_total_dead value.
    """
    os.makedirs(index_dir, exist_ok=True)

    geojson_index_path = os.path.join(index_dir, "nfi_plot_index.pkl")
    biomass_index_path = os.path.join(index_dir, "biomass_index.pkl")

    # If indexes already exist, we use them.
    if os.path.exists(geojson_index_path) and os.path.exists(biomass_index_path):
        print("Using existing indexes...")
        return

    print("\nCreating new indexes...")

    # ---------------------------
    # Step 1: Create GeoJSON index
    # ---------------------------
    print("\nStep 1: Indexing GeoJSON file...")

    nfi_plot_index = {}  # key: float(nfi_plot), value: dict with poly_ids and positions
    feature_count = 0
    error_count = 0

    # First pass: count features (for progress bar)
    print("Counting total features (this may take a moment)...")
    with open(geojson_path, 'rb') as file:
        features = ijson.items(file, 'features.item')
        total_features = sum(1 for _ in tqdm(features, desc="Counting features"))
    print(f"Found {total_features} total features to process.")

    # Second pass: build index with feature positions
    print("\nBuilding NFI_PLOT index...")
    with open(geojson_path, 'rb') as file:
        parser = ijson.parse(file)
        current_feature = {}
        current_position = None
        
        for prefix, event, value in tqdm(parser, total=total_features*10, desc="Indexing features"):  # Rough estimate of events per feature
            if prefix == 'features.item' and event == 'start_map':
                current_feature = {}
                current_position = file.tell()
            elif prefix.endswith('.NFI_PLOT'):
                try:
                    current_feature['nfi_plot'] = float(value)
                except (ValueError, TypeError):
                    continue
            elif prefix.endswith('.POLY_ID'):
                current_feature['poly_id'] = str(value).strip().lower()
            elif prefix == 'features.item' and event == 'end_map':
                if 'nfi_plot' in current_feature and 'poly_id' in current_feature:
                    nfi_plot = current_feature['nfi_plot']
                    poly_id = current_feature['poly_id']
                    
                    if nfi_plot not in nfi_plot_index:
                        nfi_plot_index[nfi_plot] = {
                            'poly_ids': [],
                            'feature_positions': []
                        }
                    
                    nfi_plot_index[nfi_plot]['poly_ids'].append(poly_id)
                    nfi_plot_index[nfi_plot]['feature_positions'].append(current_position)
                    feature_count += 1
                current_feature = {}

    print(f"\nProcessed {feature_count} features successfully.")
    if error_count > 0:
        print(f"Encountered {error_count} errors during processing.")
    print(f"Found {len(nfi_plot_index)} unique NFI_PLOT values.")

    # ---------------------------
    # Step 2: Create biomass index from CSV
    # ---------------------------
    print("\nStep 2: Creating biomass index from CSV...")
    # Note: CSV field names are in lower-case
    df = pd.read_csv(csv_path, usecols=['poly_id', 'biomass_total_dead', 'nfi_plot'])
    print(f"Processing {len(df)} biomass records...")

    # Ensure poly_id is lower-case and stripped for consistency
    df['poly_id'] = df['poly_id'].astype(str).str.strip().str.lower()
    biomass_index = df.set_index('poly_id')['biomass_total_dead'].to_dict()

    # Save indexes to disk
    print("\nStep 3: Saving indexes to disk...")
    with open(geojson_index_path, 'wb') as f:
        pickle.dump(nfi_plot_index, f)
        print(f"Saved NFI_PLOT index to {geojson_index_path}")
    with open(biomass_index_path, 'wb') as f:
        pickle.dump(biomass_index, f)
        print(f"Saved biomass index to {biomass_index_path}")

    print("\nIndexes created successfully!")
    print(f"- Total features processed: {feature_count}")
    print(f"- Unique NFI_PLOT values: {len(nfi_plot_index)}")
    print(f"- Biomass records: {len(biomass_index)}")
    print(f"- Errors encountered: {error_count}")


def load_indexes(index_dir="indexes"):
    """Load the previously saved index files."""
    geojson_index_path = os.path.join(index_dir, "nfi_plot_index.pkl")
    biomass_index_path = os.path.join(index_dir, "biomass_index.pkl")

    with open(geojson_index_path, 'rb') as f:
        nfi_plot_index = pickle.load(f)
    with open(biomass_index_path, 'rb') as f:
        biomass_index = pickle.load(f)

    return nfi_plot_index, biomass_index


def extract_plot_features(geojson_path, target_nfi_plot, nfi_plot_index):
    """
    Extract all features for a specific NFI_PLOT using the indexed positions.
    Returns a GeoDataFrame with features (polygons) matching the target NFI_PLOT.
    """
    print(f"Extracting features for NFI_PLOT {target_nfi_plot}...")

    if target_nfi_plot not in nfi_plot_index:
        raise ValueError(f"NFI_PLOT {target_nfi_plot} not found in the index.")

    # Get the target polygon ids (as lower-case strings)
    index_data = nfi_plot_index[target_nfi_plot]
    target_poly_ids = set(index_data['poly_ids'])
    print(f"Looking for {len(target_poly_ids)} polygons...")

    features = []
    with open(geojson_path, 'rb') as file:
        # Use a single pass through the file
        parser = ijson.items(file, 'features.item')
        for feature in tqdm(parser, desc="Scanning features"):
            try:
                poly_id = str(feature['properties']['POLY_ID']).strip().lower()
                if poly_id in target_poly_ids:
                    features.append(feature)
                    # If we've found all our features, we can stop
                    if len(features) == len(target_poly_ids):
                        break
            except (KeyError, ValueError) as e:
                print(f"Warning: Could not process a feature: {str(e)}")
                continue

    if not features:
        raise ValueError(f"No valid features found for NFI_PLOT {target_nfi_plot}")

    print(f"Successfully extracted {len(features)} features")

    # Create GeoDataFrame from features with the specified CRS
    crs = CRS.from_wkt(LAMBERT_CRS_WKT)
    gdf = gpd.GeoDataFrame.from_features(features, crs=crs)
    return gdf


def create_biomass_raster(geojson_path, csv_path, nfi_plot, output_path, resolution=10):
    """
    Creates a raster covering a grid of 2km x 2km or larger where each polygon is colored based on
    the normalized biomass_total_dead value. The raster preserves the CRS, uses the specified
    resolution, and is saved as a 16-bit GeoTIFF.
    """
    # Build or load indexes
    create_indexes(geojson_path, csv_path)
    nfi_plot_index, biomass_index = load_indexes()

    # Ensure the target NFI_PLOT is a float
    target_nfi_plot = float(nfi_plot)

    # Extract features (polygons) for the target NFI_PLOT
    plot_gdf = extract_plot_features(geojson_path, target_nfi_plot, nfi_plot_index)
    
    # Reproject to EPSG:3857 (Web Mercator) for better orientation
    # plot_gdf = plot_gdf.to_crs('EPSG:3857')

    # Map biomass data using the lower-case poly_id
    plot_gdf['poly_id_lower'] = plot_gdf['POLY_ID'].astype(str).str.strip().str.lower()
    plot_gdf['biomass_total_dead'] = plot_gdf['poly_id_lower'].map(biomass_index)

    # Check for missing biomass data
    if plot_gdf['biomass_total_dead'].isnull().any():
        missing = plot_gdf.loc[plot_gdf['biomass_total_dead'].isnull(), 'POLY_ID'].tolist()
        raise ValueError(f"Missing biomass data for POLY_IDs: {missing}")

    # Debug specific polygon before normalization
    specific_polygon_check = '1176871_33'.lower()
    if specific_polygon_check in plot_gdf['poly_id_lower'].values:
        poly_data = plot_gdf[plot_gdf['poly_id_lower'] == specific_polygon_check]
        print(f"\nDebug info for polygon 1176871_33 (before normalization):")
        print(f"- Raw biomass value: {poly_data['biomass_total_dead'].values[0]}")
        print(f"- Geometry type: {poly_data.geometry.iloc[0].geom_type}")
        print(f"- Geometry validity: {poly_data.geometry.iloc[0].is_valid}")
        print(f"- Geometry bounds: {poly_data.geometry.iloc[0].bounds}")

    # Normalize biomass values.
    # Map the values to the full range of uint16 (0 to 65535) where 0 is low biomass (black)
    # and 65535 is high biomass (white).
    min_biomass = plot_gdf['biomass_total_dead'].min()
    max_biomass = plot_gdf['biomass_total_dead'].max()
    print(f"\nBiomass range for normalization:")
    print(f"- Minimum biomass: {min_biomass}")
    print(f"- Maximum biomass: {max_biomass}")
    
    if min_biomass == max_biomass:
        print("All biomass values are identical. Assigning a constant normalized value.")
        plot_gdf['normalized_biomass'] = 0
    else:
        plot_gdf['normalized_biomass'] = (
            (plot_gdf['biomass_total_dead'] - min_biomass) /
            (max_biomass - min_biomass) * 60000
        ).astype(np.uint16)

    # Debug specific polygon after normalization
    if specific_polygon_check in plot_gdf['poly_id_lower'].values:
        poly_data = plot_gdf[plot_gdf['poly_id_lower'] == specific_polygon_check]
        print(f"\nDebug info for polygon 1176871_33 (after normalization):")
        print(f"- Raw biomass value: {poly_data['biomass_total_dead'].values[0]}")
        print(f"- Normalized value: {poly_data['normalized_biomass'].values[0]}")

    # Prepare shapes for rasterization: a list of (geometry, normalized_value) tuples.
    shapes = [
        (geom, value)
        for geom, value in zip(plot_gdf.geometry, plot_gdf['normalized_biomass'])
    ]

    # Debug shapes list for our specific polygon
    for i, (geom, value) in enumerate(shapes):
        if plot_gdf.iloc[i]['poly_id_lower'] == specific_polygon_check:
            print(f"\nShape info for polygon 1176871_33 in rasterization list:")
            print(f"- Value being rasterized: {value}")
            print(f"- Geometry type: {geom.geom_type}")
            print(f"- Geometry validity: {geom.is_valid}")

    # Save GeoJSON with biomass values
    geojson_output_path = f"biomass_data_NFI_{nfi_plot}.geojson"
    print(f"\nSaving GeoJSON to {geojson_output_path}...")

    # Create output dataframe with relevant fields
    output_gdf = plot_gdf[['POLY_ID', 'biomass_total_dead', 'normalized_biomass', 'geometry']].copy()
    output_gdf.rename(columns={
        'biomass_total_dead': 'biomass',
        'normalized_biomass': 'biomass_norm'
    }, inplace=True)

    output_gdf.to_file(geojson_output_path, driver='GeoJSON')

    # Get the total bounds of all polygons
    minx, miny, maxx, maxy = plot_gdf.total_bounds

    # Calculate the size needed to contain all polygons
    width_needed = maxx - minx
    height_needed = maxy - miny

    # Calculate the size of the grid that will fully contain the polygons.
    # Round up to the nearest 2km.
    grid_size = max(
        2000 * (width_needed // 2000 + (1 if width_needed % 2000 > 0 else 0)),
        2000 * (height_needed // 2000 + (1 if height_needed % 2000 > 0 else 0)),
        2000  # Minimum size is 2km
    )

    # Calculate the adjustment needed to center the polygons within the grid
    width_adjust = (grid_size - width_needed) / 2
    height_adjust = (grid_size - height_needed) / 2

    # Define the bounds of our raster, expanded and centered
    left = minx - width_adjust
    right = maxx + width_adjust
    bottom = miny - height_adjust
    top = maxy + height_adjust

    print(f"\nRaster bounds:")
    print(f"Original extent: {width_needed:.2f}m x {height_needed:.2f}m")
    print(f"Adjusted size: {grid_size:.2f}m x {grid_size:.2f}m")

    # Calculate raster dimensions in pixels
    width = height = int(grid_size / resolution)

    # First create the raster in a standard orientation
    initial_transform = from_bounds(left, bottom, right, top, width, height)
    
    # Create initial raster
    initial_raster = rasterize(
        shapes,
        out_shape=(height, width),
        transform=initial_transform,
        fill=65535,
        dtype=np.uint16,
        all_touched=True
    )
    
    # Create destination raster with same dimensions
    final_raster = np.zeros((height, width), dtype=np.uint16)
    
    # Create rotated transform based on CRS
    dx = (right - left) / width
    dy = (top - bottom) / height
    final_transform = Affine.translation(left, top) * Affine.scale(dx, -dy)
    
    # Reproject the raster to match CRS orientation
    reproject(
        source=initial_raster,
        destination=final_raster,
        src_transform=initial_transform,
        src_crs=plot_gdf.crs,
        dst_transform=final_transform,
        dst_crs=plot_gdf.crs,
        resampling=Resampling.nearest
    )
    
    raster = final_raster

    # --- Masking the Raster ---
    # Create the union of the photoplot polygons.
    # This union represents the exact (possibly slanted) photoplot boundary.
    photoplot_union = unary_union(list(plot_gdf.geometry))

    # Create a Boolean mask for the full grid:
    # Pixels outside the photoplot (i.e. outside the union geometry) will be True.
    mask = geometry_mask(
        [photoplot_union],
        out_shape=(height, width),
        transform=final_transform,
        invert=False  # do not invert: returns True for pixels outside the geometry
    )

    # Set nodata (65535) for pixels outside the photoplot.
    raster[mask] = 65535

    # Save the raster as a GeoTIFF.
    print("Saving raster...")
    with rasterio.open(
            output_path,
            'w',
            driver='GTiff',
            height=height,
            width=width,
            count=1,
            dtype=np.uint16,
            crs=plot_gdf.crs,
            transform=final_transform,
            nodata=65535  # Set nodata value to 65535
    ) as dst:
        dst.write(raster, 1)

    print(f"\nRaster created successfully at: {output_path}")
    print(f"Raster dimensions: {width}x{height} pixels")
    print(f"Pixel resolution: {resolution}m")
    print(f"Grid size: {grid_size / 1000:.1f}km x {grid_size / 1000:.1f}km")
    print(f"Biomass range: {min_biomass:.2f} to {max_biomass:.2f}")


if __name__ == "__main__":
    # Update these paths to your actual file locations
    geojson_path = "all_pp_lc.geojson"
    csv_path = "all_pp_poly_summ.csv"
    nfi_plot = 1176871.0  # Example NFI_PLOT value
    output_path = f"biomass_raster_NFI_{nfi_plot}.tif"

    create_biomass_raster(geojson_path, csv_path, nfi_plot, output_path)
