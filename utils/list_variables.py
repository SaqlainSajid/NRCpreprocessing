import sqlite3
import os

def list_table_variables(db_path="forest_inventory.db"):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    # Create or open a text file to write the results
    with open('table_variables.txt', 'w') as f:
        f.write("Forest Inventory Database Table Variables\n")
        f.write("======================================\n\n")
        
        # For each table, get and write its column names
        for table in tables:
            table_name = table[0]
            f.write(f"Table: {table_name}\n")
            f.write("-" * (len(table_name) + 7) + "\n")
            
            # Get column information
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            
            # Write column names and types
            for col in columns:
                f.write(f"- {col[1]} ({col[2]})\n")
            f.write("\n")
    
    conn.close()
    print("Variable list has been saved to 'table_variables.txt'")

if __name__ == "__main__":
    list_table_variables()
