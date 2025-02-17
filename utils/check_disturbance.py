import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('forest_inventory.db')
cursor = conn.cursor()

nfi_plot = 1176871

# Check if table exists
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='all_pp_std_layer_disturbance'")
if cursor.fetchone():
    print("Table exists!")
    
    # Get total count
    cursor.execute("SELECT COUNT(*) FROM all_pp_std_layer_disturbance")
    total = cursor.fetchone()[0]
    print(f"Total rows in table: {total}")
    
    # Get count for our nfi_plot
    cursor.execute("SELECT COUNT(*) FROM all_pp_std_layer_disturbance WHERE nfi_plot = ?", (nfi_plot,))
    matches = cursor.fetchone()[0]
    print(f"Rows for nfi_plot {nfi_plot}: {matches}")
    
    # Get sample data
    cursor.execute("SELECT * FROM all_pp_std_layer_disturbance WHERE nfi_plot = ? LIMIT 5", (nfi_plot,))
    rows = cursor.fetchall()
    if rows:
        print("\nSample rows:")
        for row in rows:
            print(row)
else:
    print("Table does not exist!")

conn.close()
