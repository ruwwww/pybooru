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

class AsyncScraper:
    def __init__(self):
        self.db = Database()
        self.storage = ParquetStorage()
        self.client = Danbooru('danbooru')
        self.concurrency_limit = 5
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)

    async def download_image(self, session, url):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.read()
                else:
                    logger.warning(f"Failed to download {url}: Status {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None

    async def process_post(self, session, post, artist_id):
        post_id = str(post['id'])
        
        # Check if post already exists in DB (optional optimization, but hash check is better)
        # For now, we rely on hash check for deduplication, but we can skip if we know we have this post_id
        # self.db.cursor.execute("SELECT 1 FROM images WHERE post_id = ?", (post_id,))
        # if self.db.cursor.fetchone():
        #     return

        file_url = post.get('file_url')
        if not file_url:
            return

        async with self.semaphore:
            image_bytes = await self.download_image(session, file_url)
        
        if not image_bytes:
            return

        # Process Image (CPU bound, run in executor)
        loop = asyncio.get_event_loop()
        compressed_bytes = await loop.run_in_executor(None, process_image_bytes, image_bytes)
        
        if not compressed_bytes:
            return

        # Calculate Hash
        image_hash = await loop.run_in_executor(None, calculate_hash, compressed_bytes)
        
        if not image_hash:
            return

        # Check Hash Index (Deduplication)
        self.db.cursor.execute("SELECT image_id FROM hash_index WHERE hash = ?", (image_hash,))
        if self.db.cursor.fetchone():
            logger.info(f"Duplicate found for post {post_id} (hash: {image_hash}). Skipping.")
            return

        # Add to Storage (Buffer)
        flush_result = self.storage.add_image(compressed_bytes, artist_id, post_id, image_hash)
        
        # If flushed, update DB
        if flush_result:
            self._update_db_from_flush(flush_result)

    def _update_db_from_flush(self, flush_result):
        shard_file = flush_result['shard_file']
        rows = flush_result['rows']
        
        for i, row in enumerate(rows):
            try:
                # Insert into images
                self.db.cursor.execute("""
                    INSERT INTO images (artist_id, source, post_id, hash, timestamp, shard_file, offset, size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (row['artist_id'], row['source'], row['post_id'], row['hash'], row['timestamp'], shard_file, i, row['size']))
                
                image_id = self.db.cursor.lastrowid
                
                # Insert into hash_index
                self.db.cursor.execute("""
                    INSERT INTO hash_index (hash, image_id)
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
                    posts = self.client.post_list(tags=f"{artist_name} -animated", page=page, limit=20)
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
                
                # Safety break
                if page > 10: # Limit pages for MVP testing
                    break

        # Final flush
        flush_result = self.storage.flush()
        if flush_result:
            self._update_db_from_flush(flush_result)
            
        return new_last_post

    def close(self):
        self.db.close()
        self.storage.close()
