import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

PARQUET_DIR = os.path.join("hoard", "parquet")
SHARD_SIZE_LIMIT_MB = 500

class ParquetStorage:
    def __init__(self, source="danbooru"):
        self.source = source
        self.buffer = []
        self.seen_hashes = set()
        self.current_shard_index = self._get_next_shard_index()
        self.buffer_size_bytes = 0

    def _get_next_shard_index(self):
        existing_files = [f for f in os.listdir(PARQUET_DIR) if f.startswith(f"{self.source}_") and f.endswith(".parquet")]
        if not existing_files:
            return 0
        
        indices = []
        for f in existing_files:
            try:
                # Extract index from filename: source_0001.parquet
                part = f.replace(f"{self.source}_", "").replace(".parquet", "")
                indices.append(int(part))
            except ValueError:
                continue
        
        return max(indices) + 1 if indices else 0

    def add_image(self, image_bytes, artist_id, post_id, image_hash, timestamp=None):
        if image_hash in self.seen_hashes:
            return None

        if timestamp is None:
            timestamp = datetime.now()

        row = {
            "image_bytes": image_bytes,
            "artist_id": artist_id,
            "source": self.source,
            "post_id": post_id,
            "hash": image_hash,
            "timestamp": timestamp
        }
        
        self.buffer.append(row)
        self.seen_hashes.add(image_hash)
        self.buffer_size_bytes += len(image_bytes)

        # Check if buffer is full (approximate check)
        if self.buffer_size_bytes >= SHARD_SIZE_LIMIT_MB * 1024 * 1024:
            return self.flush()
        
        return None

    def flush(self):
        if not self.buffer:
            return None

        shard_filename = f"{self.source}_{self.current_shard_index:04d}.parquet"
        shard_path = os.path.join(PARQUET_DIR, shard_filename)

        df = pd.DataFrame(self.buffer)
        table = pa.Table.from_pandas(df)

        # Write to Parquet
        pq.write_table(table, shard_path)
        
        print(f"Flushed {len(self.buffer)} images to {shard_filename}")

        # Return info for database update
        result = {
            "shard_file": shard_filename,
            "count": len(self.buffer),
            "rows": self.buffer # Return rows so we can update DB with offsets if needed
        }

        self.buffer = []
        self.seen_hashes = set()
        self.buffer_size_bytes = 0
        self.current_shard_index += 1
        
        return result

    def close(self):
        return self.flush()
