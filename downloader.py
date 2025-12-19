from __future__ import unicode_literals
from pybooru import Danbooru
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BooruDownloader:
    def __init__(self, base_save_dir="downloads"):
        self.client = Danbooru('danbooru')
        self.base_save_dir = base_save_dir

    def download_image(self, item, tags_dir, force=False, max_retries=5):
        """Downloads a single image file with retry mechanism using the configured session."""
        url = item['url']
        post_id = item['id']
        ext = item['ext']
        file_name = os.path.join(tags_dir, f"{post_id}_sample.{ext}")
        
        if os.path.exists(file_name) and not force:
            return False # Skipped

        for attempt in range(max_retries):
            try:
                # Use the session from pybooru client which has proxies configured
                with self.client.client.get(url, stream=True, timeout=30) as response:
                    response.raise_for_status()
                    with open(file_name, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                return True # Downloaded
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed {post_id} after {max_retries} attempts: {e}")
                    # Clean up partial file if it exists
                    if os.path.exists(file_name):
                        try:
                            os.remove(file_name)
                        except:
                            pass
                    return False
        return False

    def process_job(self, tags, force=False, proxy_url=None, progress_callback=None, stop_event=None):
        """
        Executes the download job.
        progress_callback: function(status_dict)
        stop_event: threading.Event to signal cancellation
        """
        
        # Configure proxy if provided
        if proxy_url:
            proxies = {
                "http": proxy_url,
                "https": proxy_url
            }
            self.client.client.proxies.update(proxies)
            logger.info(f"Using proxy: {proxy_url}")
        else:
            # Clear proxies if not provided (in case reused)
            self.client.client.proxies.clear()

        # Sanitize tags for directory name
        safe_tags = "".join([c for c in tags if c.isalpha() or c.isdigit() or c in " ._-"]).strip()
        save_dir = os.path.join(self.base_save_dir, safe_tags)
        
        if os.path.exists(save_dir) and not force:
             if progress_callback:
                progress_callback({
                    "status": "skipped",
                    "message": f"Directory {save_dir} already exists. Use force to overwrite."
                })
             return

        os.makedirs(save_dir, exist_ok=True)

        if progress_callback:
            progress_callback({"status": "fetching_metadata", "message": f"Fetching metadata for {tags}..."})

        all_posts = []
        page = 1
        
        while True:
            if stop_event and stop_event.is_set():
                if progress_callback: progress_callback({"status": "cancelled", "message": "Job cancelled during metadata fetch."})
                return

            try:
                current_posts = self.client.post_list(tags=f"{tags} -animated", page=page, limit=100)
                if not current_posts:
                    break
                all_posts.extend(current_posts)
                page += 1
                
                if progress_callback:
                    progress_callback({"status": "fetching_metadata", "message": f"Fetched page {page-1} ({len(all_posts)} posts)..."})

            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break

        download_items = []
        for post in all_posts:
            sample_url = None
            if 'media_asset' in post and 'variants' in post['media_asset']:
                sample_url = next(
                    (v['url'] for v in post['media_asset']['variants'] if v['type'] == 'sample'),
                    None
                )
            
            if not sample_url:
                sample_url = post.get('large_file_url') or post.get('file_url')

            if sample_url:
                ext = sample_url.split('.')[-1]
                download_items.append({
                    'url': sample_url, 
                    'id': post['id'], 
                    'ext': ext
                })

        total_items = len(download_items)
        if progress_callback:
            progress_callback({
                "status": "downloading",
                "total": total_items,
                "completed": 0,
                "message": f"Starting download of {total_items} images..."
            })

        MAX_WORKERS = max(1, int((os.cpu_count() or 1) * 2 / 3))
        success_count = 0
        completed_count = 0

        if download_items:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(self.download_image, item, save_dir, force): item for item in download_items}
                
                for future in as_completed(futures):
                    if stop_event and stop_event.is_set():
                        executor.shutdown(wait=False, cancel_futures=True)
                        if progress_callback: progress_callback({"status": "cancelled", "message": "Job cancelled."})
                        return

                    result = future.result()
                    completed_count += 1
                    if result:
                        success_count += 1
                    
                    if progress_callback:
                        progress_callback({
                            "status": "downloading",
                            "total": total_items,
                            "completed": completed_count,
                            "success": success_count,
                            "message": f"Downloaded {success_count}/{total_items} (Processed {completed_count})"
                        })

        if progress_callback:
            progress_callback({
                "status": "completed",
                "total": total_items,
                "completed": completed_count,
                "success": success_count,
                "message": f"Job finished. Downloaded {success_count} new images."
            })
