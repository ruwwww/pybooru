import os
import pandas as pd
import pyarrow.parquet as pq
import io
from PIL import Image

PARQUET_DIR = os.path.join("hoard", "parquet")

def inspect_parquet():
    if not os.path.exists(PARQUET_DIR):
        print(f"Directory {PARQUET_DIR} does not exist.")
        return

    files = [f for f in os.listdir(PARQUET_DIR) if f.endswith(".parquet")]
    files.sort()

    if not files:
        print("No parquet files found.")
        return

    total_images = 0
    total_size = 0

    print(f"{'Filename':<30} | {'Rows':<6} | {'Size (MB)':<10} | {'Avg Img (KB)':<12}")
    print("-" * 70)

    for f in files:
        path = os.path.join(PARQUET_DIR, f)
        file_size_mb = os.path.getsize(path) / (1024 * 1024)
        total_size += file_size_mb
        
        try:
            table = pq.read_table(path)
            df = table.to_pandas()
            row_count = len(df)
            total_images += row_count
            
            # Calculate average image size
            avg_img_size_kb = 0
            if row_count > 0:
                # sum of length of bytes in image_bytes column
                total_bytes = df['image_bytes'].apply(len).sum()
                avg_img_size_kb = (total_bytes / row_count) / 1024

            print(f"{f:<30} | {row_count:<6} | {file_size_mb:<10.2f} | {avg_img_size_kb:<12.2f}")

            # Inspect first image dimensions
            if row_count > 0:
                first_img_bytes = df.iloc[0]['image_bytes']
                img = Image.open(io.BytesIO(first_img_bytes))
                print(f"   > Sample Image 1: {img.format} {img.size} (Artist ID: {df.iloc[0]['artist_id']})")

        except Exception as e:
            print(f"{f:<30} | ERROR: {e}")

    print("-" * 70)
    print(f"Total: {len(files)} files, {total_images} images, {total_size:.2f} MB")

if __name__ == "__main__":
    inspect_parquet()
