import json

def print_file_preview(filename):
    print(f"\nPreview of {filename}:")
    print("-" * 80)
    try:
        count = 0
        with open(filename, 'r', encoding='utf-8', errors='replace') as f:
            # Read chunk by chunk
            chunk_size = 8192
            buffer = ""
            while count < 20:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                buffer += chunk
                lines = buffer.split('\n')
                
                # Print complete lines
                for line in lines[:-1]:
                    if line.strip():
                        count += 1
                        print(f"Line {count}: {line.strip()}")
                    if count >= 20:
                        break
                
                # Keep the last partial line in the buffer
                buffer = lines[-1]
                
                if count >= 20:
                    break
    except Exception as e:
        print(f"Error reading file {filename}: {str(e)}")

# File paths
file1 = r"C:\Users\saqla\OneDrive\Desktop\NRC_Data\Preprocessing\all_pp_lc.geojson"
file2 = r"C:\Users\saqla\OneDrive\Desktop\NRC_Data\Preprocessing\all_pp_land_cover.geojson"

# Print preview of both files
print("\nComparing the first 20 non-empty lines of both GeoJSON files:")
print("=" * 80)
print_file_preview(file1)
print_file_preview(file2)
