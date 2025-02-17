import sqlite3
import pandas as pd
from datetime import datetime
import os

def ensure_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def get_plot_coordinates():
    # Connect to the database
    conn = sqlite3.connect('forest_inventory.db')
    
    # Create output directories
    ensure_directory('plots_before_2016')
    ensure_directory('plots_from_2016')
    
    # Modified query to get only unique plots with their most recent information
    join_query = """
    WITH RankedPlots AS (
        SELECT 
            lc.nfi_plot,
            lc.juris_id,
            pp.utm_e as easting,
            pp.utm_n as northing,
            pp.utm_zone,
            lc.info_date,
            ROW_NUMBER() OVER (PARTITION BY lc.nfi_plot ORDER BY lc.info_date DESC) as rn
        FROM all_pp_landcover lc
        LEFT JOIN all_pp_photo_plot pp ON lc.nfi_plot = pp.nfi_plot
        WHERE pp.utm_e IS NOT NULL 
          AND pp.utm_n IS NOT NULL 
          AND pp.utm_zone IS NOT NULL
    )
    SELECT 
        nfi_plot,
        juris_id,
        easting,
        northing,
        utm_zone,
        info_date
    FROM RankedPlots
    WHERE rn = 1
    ORDER BY juris_id, utm_zone, info_date, nfi_plot
    """
    
    try:
        print("Starting export of unique plot coordinates...")
        cutoff_date = datetime.strptime('2016-10-19', '%Y-%m-%d')
        
        # Initialize counters
        counts = {
            'before': {},  # province -> zone -> count
            'after': {}    # province -> zone -> count
        }
        
        # Read all data at once since we're only getting unique plots
        df = pd.read_sql_query(join_query, conn)
        df['info_date'] = pd.to_datetime(df['info_date'])
        
        total_unique_plots = len(df)
        print(f"Total unique plots found: {total_unique_plots:,}")
        
        # Process each province
        for province in df['juris_id'].unique():
            province_data = df[df['juris_id'] == province]
            
            # Create province directories
            ensure_directory(f'plots_before_2016/{province}')
            ensure_directory(f'plots_from_2016/{province}')
            
            # Split province data by date
            before_2016_province = province_data[province_data['info_date'] < cutoff_date]
            after_2016_province = province_data[province_data['info_date'] >= cutoff_date]
            
            # Save all province data
            if len(before_2016_province) > 0:
                before_2016_province.to_csv(f'plots_before_2016/{province}/all_zones.csv', index=False)
            
            if len(after_2016_province) > 0:
                after_2016_province.to_csv(f'plots_from_2016/{province}/all_zones.csv', index=False)
            
            # Process each UTM zone
            for zone in province_data['utm_zone'].unique():
                if pd.isna(zone):
                    continue
                    
                zone_data = province_data[province_data['utm_zone'] == zone]
                
                # Split by date
                before_2016 = zone_data[zone_data['info_date'] < cutoff_date]
                after_2016 = zone_data[zone_data['info_date'] >= cutoff_date]
                
                # Initialize counters
                if province not in counts['before']:
                    counts['before'][province] = {}
                if province not in counts['after']:
                    counts['after'][province] = {}
                
                # Save before 2016 data
                if len(before_2016) > 0:
                    filename = f'plots_before_2016/{province}/zone_{int(zone)}.csv'
                    before_2016.to_csv(filename, index=False)
                    counts['before'][province][zone] = len(before_2016)
                
                # Save 2016 and after data
                if len(after_2016) > 0:
                    filename = f'plots_from_2016/{province}/zone_{int(zone)}.csv'
                    after_2016.to_csv(filename, index=False)
                    counts['after'][province][zone] = len(after_2016)
        
        print("\nExport completed:")
        
        # Print statistics for each period
        for period, period_label in [('before', 'Before 2016'), ('after', 'From 2016 onwards')]:
            print(f"\n{period_label}:")
            for province in sorted(counts[period].keys()):
                print(f"\n{province}:")
                total_province = 0
                for zone, count in sorted(counts[period][province].items()):
                    print(f"  - Zone {int(zone)}: {count:,} unique plots")
                    total_province += count
                print(f"  Total: {total_province:,} unique plots")
        
        # Display sample data
        print("\nSample of unique plots from largest provinces:")
        for period, directory in [("Before 2016", "plots_before_2016"), ("From 2016", "plots_from_2016")]:
            print(f"\n{period}:")
            period_key = 'before' if period == "Before 2016" else 'after'
            
            # Get the top province with the most data
            if counts[period_key]:
                top_province = max(
                    [(p, sum(zones.values())) for p, zones in counts[period_key].items()],
                    key=lambda x: x[1]
                )[0]
                
                filename = f"{directory}/{top_province}/all_zones.csv"
                if os.path.exists(filename):
                    sample = pd.read_csv(filename, nrows=3)
                    print(f"\n{top_province} sample plots:")
                    print(sample[['nfi_plot', 'info_date', 'easting', 'northing', 'utm_zone']])
            
    except Exception as e:
        print(f"Error during export: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    get_plot_coordinates()
