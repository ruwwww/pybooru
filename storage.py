import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

PARQUET_BASE_DIR = os.path.join("hoard", "parquet")
SHARD_SIZE_LIMIT_MB = 64  # Increased shard size target

class ParquetStorage:
    def __init__(self, source="danbooru"):
        self.source = source
        self.buffer = []
        self.seen_hashes = set()
        
        # Ensure source directory exists
        self.source_dir = os.path.join(PARQUET_BASE_DIR, self.source)
        os.makedirs(self.source_dir, exist_ok=True)
        
        self._init_shard_state()

    def _init_shard_state(self):
        # Find the latest shard
        existing_files = [f for f in os.listdir(self.source_dir) if f.endswith(".parquet")]
        if not existing_files:
            self.current_shard_index = 0
            self.existing_shard_size = 0
            self.buffer_size_bytes = 0
            return

        indices = []
        for f in existing_files:
            try:
                part = f.replace("shard_", "").replace(".parquet", "")
                indices.append(int(part))
            except ValueError:
                continue
        
        if not indices:
            self.current_shard_index = 0
            self.existing_shard_size = 0
            self.buffer_size_bytes = 0
            return

        max_idx = max(indices)
        last_shard_path = os.path.join(self.source_dir, f"shard_{max_idx:04d}.parquet")
        
        try:
            size = os.path.getsize(last_shard_path)
            if size < SHARD_SIZE_LIMIT_MB * 1024 * 1024:
                # Append to this shard
                self.current_shard_index = max_idx
                self.existing_shard_size = size
                self.buffer_size_bytes = size
                print(f"Resuming shard {max_idx} ({size/1024/1024:.2f} MB)")
            else:
                # Start new shard
                self.current_shard_index = max_idx + 1
                self.existing_shard_size = 0
                self.buffer_size_bytes = 0
        except OSError:
             self.current_shard_index = max_idx + 1
             self.existing_shard_size = 0
             self.buffer_size_bytes = 0

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
            "timestamp": timestamp,
            "size": len(image_bytes)
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

        shard_filename = f"shard_{self.current_shard_index:04d}.parquet"
        shard_path = os.path.join(self.source_dir, shard_filename)

        new_df = pd.DataFrame(self.buffer)
        start_offset = 0
        
        # Check if we need to append
        if os.path.exists(shard_path):
            try:
                existing_df = pd.read_parquet(shard_path)
                start_offset = len(existing_df)
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
            except Exception as e:
                print(f"Error reading existing shard {shard_path}: {e}. Creating new one.")
                # Fallback: increment index and write new
                self.current_shard_index += 1
                shard_filename = f"shard_{self.current_shard_index:04d}.parquet"
                shard_path = os.path.join(self.source_dir, shard_filename)
                final_df = new_df
                start_offset = 0
        else:
            final_df = new_df

        table = pa.Table.from_pandas(final_df)
        pq.write_table(table, shard_path)
        
        print(f"Flushed {len(self.buffer)} new images to {shard_path} (Total rows: {len(final_df)})")

        # Return info for database update
        relative_path = os.path.join(self.source, shard_filename).replace("\\", "/")
        
        result = {
            "shard_file": relative_path,
            "count": len(self.buffer),
            "rows": self.buffer,
            "start_offset": start_offset
        }

        self.buffer = []
        self.seen_hashes = set()
        
        # Update sizes and check if we need to rotate shard
        self.existing_shard_size = os.path.getsize(shard_path)
        self.buffer_size_bytes = self.existing_shard_size
        
        if self.buffer_size_bytes >= SHARD_SIZE_LIMIT_MB * 1024 * 1024:
            self.current_shard_index += 1
            self.existing_shard_size = 0
            self.buffer_size_bytes = 0
        
        return result

    def close(self):
        return self.flush()
