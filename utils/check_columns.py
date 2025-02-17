import sqlite3

def check_table_columns():
    conn = sqlite3.connect('forest_inventory.db')
    cursor = conn.cursor()
    
    # Get list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print("Table Columns:")
    print("-" * 50)
    
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        print(f"\nTable: {table_name}")
        for col in columns:
            print(f"- {col[1]} ({col[2]})")
        print("-" * 30)
    
    conn.close()

if __name__ == "__main__":
    check_table_columns()
