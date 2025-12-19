import asyncio
import random
import logging
from datetime import datetime
from database import Database
from scraper import AsyncScraper

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Scheduler:
    def __init__(self):
        self.db = Database()
        self.scraper = AsyncScraper()

    def select_artist(self):
        artists = self.db.get_all_artists()
        if not artists:
            return None
        
        # Filter out artists checked very recently (optional, e.g., within last hour)
        # For now, just use weights
        
        weights = [a['probability_weight'] for a in artists]
        selected = random.choices(artists, weights=weights, k=1)[0]
        return selected

    async def run(self):
        logger.info("Starting Scheduler...")
        
        try:
            while True:
                artist = self.select_artist()
                if not artist:
                    logger.info("No artists found in database. Please seed the database.")
                    break

                logger.info(f"Selected artist: {artist['name']} (Weight: {artist['probability_weight']})")
                
                # Scrape
                last_post = await self.scraper.scrape_artist(
                    artist['id'], 
                    artist['name'], 
                    artist['last_scraped_post']
                )

                # Update Stats
                new_weight = artist['probability_weight']
                if last_post and last_post != artist['last_scraped_post']:
                    # Found new posts, increase weight
                    new_weight = min(new_weight * 1.1, 100.0)
                    logger.info(f"Found new posts. Increasing weight to {new_weight:.2f}")
                else:
                    # No new posts, decrease weight
                    new_weight = max(new_weight * 0.9, 0.1)
                    logger.info(f"No new posts. Decreasing weight to {new_weight:.2f}")

                self.db.cursor.execute("""
                    UPDATE artists 
                    SET last_checked = ?, last_scraped_post = ?, probability_weight = ?
                    WHERE id = ?
                """, (datetime.now(), last_post or artist['last_scraped_post'], new_weight, artist['id']))
                self.db.conn.commit()

                # Sleep between artists to be polite
                await asyncio.sleep(5)

        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user.")
        finally:
            self.scraper.close()
            self.db.close()

if __name__ == "__main__":
    scheduler = Scheduler()
    asyncio.run(scheduler.run())
