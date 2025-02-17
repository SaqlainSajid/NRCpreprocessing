import sqlite3
import pandas as pd
import os
from pathlib import Path

class ForestDatabaseCreator:
    def __init__(self, db_name="forest_inventory.db"):
        """Initialize database connection"""
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        print(f"Created database: {db_name}")

    def create_table_from_csv(self, csv_path, table_name=None):
        """Create a table from a CSV file"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            
            # If table_name not provided, use CSV filename without extension
            if table_name is None:
                table_name = Path(csv_path).stem
            
            # Clean column names (remove spaces, special characters)
            df.columns = [col.lower().replace(' ', '_').replace('-', '_') 
                         for col in df.columns]
            
            # Create table and insert data
            df.to_sql(table_name, self.conn, if_exists='replace', index=False)
            print(f"Created table '{table_name}' with {len(df)} rows")
            
            # Create indices for commonly queried columns
            common_index_columns = ['nfi_plot', 'measurement_date', 'province']
            for col in common_index_columns:
                if col in df.columns:
                    index_name = f"idx_{table_name}_{col}"
                    self.cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({col})")
            
            return True
        except Exception as e:
            print(f"Error processing {csv_path}: {str(e)}")
            return False

    def process_directory(self, directory_path):
        """Process all CSV files in a directory"""
        dir_path = Path(directory_path)
        if not dir_path.exists():
            print(f"Directory not found: {directory_path}")
            return
        
        # Process all CSV files in the directory
        csv_files = list(dir_path.glob("*.csv"))
        print(f"\nProcessing {len(csv_files)} CSV files in {directory_path}")
        
        for csv_file in csv_files:
            print(f"\nProcessing: {csv_file.name}")
            self.create_table_from_csv(csv_file)

    def create_views(self):
        """Create useful views combining data from different tables"""
        try:
            # Example view combining land cover and ownership
            self.cursor.execute("""
                CREATE VIEW IF NOT EXISTS v_landcover_ownership AS
                SELECT 
                    l.*,
                    o.ownership_type,
                    o.ownership_detail
                FROM all_pp_landcover l
                LEFT JOIN all_pp_ownership o 
                ON l.nfi_plot = o.nfi_plot
                WHERE l.nfi_plot IS NOT NULL
            """)
            
            # Add more views as needed
            
            self.conn.commit()
            print("\nCreated database views successfully")
        except Exception as e:
            print(f"Error creating views: {str(e)}")

    def close(self):
        """Close database connection"""
        self.conn.close()
        print("\nDatabase connection closed")

def main():
    # Initialize database creator
    db_creator = ForestDatabaseCreator()
    
    # List of directories to process
    directories = [
        r"C:\Users\saqla\OneDrive\Desktop\NRC_Data\All_Data\Canada_PP_First_Remeasurement\all_pp_landcover\all_pp_lc",
        r"C:\Users\saqla\OneDrive\Desktop\NRC_Data\All_Data\Canada_PP_First_Remeasurement\all_pp_landuse\all_pp_lu",
        r"C:\Users\saqla\OneDrive\Desktop\NRC_Data\All_Data\Canada_PP_First_Remeasurement\all_pp_ownership\all_pp_ow",
        r"C:\Users\saqla\OneDrive\Desktop\NRC_Data\All_Data\Canada_PP_First_Remeasurement\all_pp_protect_status\all_pp_ps"
    ]
    
    # Process each directory
    for directory in directories:
        db_creator.process_directory(directory)
    
    # Create photo plot table
    photo_plot_path = r"C:\Users\saqla\OneDrive\Desktop\NRC_Data\All_Data\Canada_PP_Baseline\all_pp_landuse\all_pp_lu\all_pp_photo_plot.csv"
    db_creator.create_table_from_csv(photo_plot_path, 'all_pp_photo_plot')
    
    # Create useful database views
    db_creator.create_views()
    
    # Close database connection
    db_creator.close()

if __name__ == "__main__":
    main()
