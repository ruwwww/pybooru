import asyncio
import aiohttp
import logging
import sqlite3
from pybooru import Danbooru
from database import Database
from storage import ParquetStorage
from image_processor import process_image_bytes, calculate_hash
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DEDUPLICATE_IMAGES = False  # Set to True to enable hash-based deduplication

class AsyncScraper:
    def __init__(self):
        self.db = Database()
        self.storage = ParquetStorage()
        self.client = Danbooru('danbooru')
        self.concurrency_limit = 5
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)

    async def download_image(self, session, url, retries=5):
        for attempt in range(retries):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        return await response.read()
                    elif response.status == 429: # Too Many Requests
                        wait_time = (2 ** attempt) + 1
                        logger.warning(f"Rate limited on {url}. Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.warning(f"Failed to download {url}: Status {response.status}")
                        return None
            except Exception as e:
                if attempt < retries - 1:
                    wait_time = (2 ** attempt) + 1
                    logger.warning(f"Error downloading {url}: {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Failed to download {url} after {retries} attempts: {e}")
                    return None
        return None

    async def process_post(self, session, post, artist_id):
        post_id = str(post['id'])
        
        # Check if post already exists in DB (optional optimization, but hash check is better)
        # For now, we rely on hash check for deduplication, but we can skip if we know we have this post_id
        # self.db.cursor.execute("SELECT 1 FROM images WHERE post_id = ?", (post_id,))
        # if self.db.cursor.fetchone():
        #     return

        # URL Extraction Logic (Prioritize file_url as requested)
        file_url = post.get('file_url')

        if not file_url:
            if 'media_asset' in post and 'variants' in post['media_asset']:
                file_url = next(
                    (v['url'] for v in post['media_asset']['variants'] if v['type'] == 'sample'),
                    None
                )
        
        if not file_url:
            file_url = post.get('large_file_url') or post.get('sample_url')

        if not file_url:
            logger.warning(f"No image URL found for post {post_id}. Skipping.")
            return

        async with self.semaphore:
            image_bytes = await self.download_image(session, file_url)
        
        if not image_bytes:
            logger.warning(f"Download failed for post {post_id} (URL: {file_url})")
            return

        # Process Image (CPU bound, run in executor)
        loop = asyncio.get_event_loop()
        compressed_bytes = await loop.run_in_executor(None, process_image_bytes, image_bytes)
        
        if not compressed_bytes:
            logger.warning(f"Failed to process image for post {post_id} (processing returned None)")
            return

        # Calculate Hash
        image_hash = await loop.run_in_executor(None, calculate_hash, compressed_bytes)
        
        if not image_hash:
            logger.warning(f"Failed to calculate hash for post {post_id}")
            return

        # Check Hash Index (Deduplication)
        if DEDUPLICATE_IMAGES:
            self.db.cursor.execute("SELECT image_id FROM hash_index WHERE hash = ?", (image_hash,))
            if self.db.cursor.fetchone():
                logger.info(f"Duplicate found for post {post_id} (hash: {image_hash}). Skipping.")
                return

        # Add to Storage (Buffer)
        flush_result = self.storage.add_image(compressed_bytes, artist_id, post_id, image_hash)
        if flush_result is None and len(self.storage.buffer) > 0:
             # If flush_result is None, it means it was buffered.
             pass
        elif flush_result:
             self._update_db_from_flush(flush_result)
        
        logger.info(f"Successfully processed post {post_id}")

    def _update_db_from_flush(self, flush_result):
        shard_file = flush_result['shard_file']
        rows = flush_result['rows']
        start_offset = flush_result.get('start_offset', 0)
        
        for i, row in enumerate(rows):
            try:
                # Insert into images
                self.db.cursor.execute("""
                    INSERT INTO images (artist_id, source, post_id, hash, timestamp, shard_file, offset, size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (row['artist_id'], row['source'], row['post_id'], row['hash'], row['timestamp'], shard_file, start_offset + i, row['size']))
                
                image_id = self.db.cursor.lastrowid
                
                # Insert into hash_index (Ignore duplicates if they exist)
                self.db.cursor.execute("""
                    INSERT OR IGNORE INTO hash_index (hash, image_id)
                    VALUES (?, ?)
                """, (row['hash'], image_id))
            except sqlite3.IntegrityError as e:
                logger.warning(f"Integrity error inserting image {row['post_id']} (hash: {row['hash']}): {e}")
        
        self.db.conn.commit()

    async def scrape_artist(self, artist_id, artist_name, last_scraped_post=None):
        logger.info(f"Scraping artist: {artist_name}")
        
        page = 1
        posts_count = 0
        new_last_post = last_scraped_post
        
        async with aiohttp.ClientSession() as session:
            while True:
                # Fetch metadata (Sync call wrapped in executor if needed, but fast enough)
                try:
                    # Increased limit to 100 (max for most boorus) and removed page limit
                    posts = self.client.post_list(tags=f"{artist_name} -animated", page=page, limit=100)
                    logger.info(f"Page {page}: Fetched {len(posts)} posts for {artist_name}")
                except Exception as e:
                    logger.error(f"Error fetching posts for {artist_name}: {e}")
                    break

                if not posts:
                    break

                tasks = []
                for post in posts:
                    post_id = str(post['id'])
                    
                    # Stop if we reached the last scraped post
                    if last_scraped_post and post_id == last_scraped_post:
                        logger.info(f"Reached last scraped post {last_scraped_post}. Stopping.")
                        # Wait for pending tasks
                        await asyncio.gather(*tasks)
                        return new_last_post

                    if page == 1 and posts_count == 0:
                        new_last_post = post_id

                    tasks.append(self.process_post(session, post, artist_id))
                    posts_count += 1

                await asyncio.gather(*tasks)
                page += 1
                
                # Safety break removed to allow full scraping
                # if page > 10: 
                #    break

        # Final flush
        flush_result = self.storage.flush()
        if flush_result:
            self._update_db_from_flush(flush_result)
            
        return new_last_post

    def close(self):
        self.db.close()
        self.storage.close()
