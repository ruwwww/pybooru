import os
from database import Database

DOWNLOADS_DIR = "downloads"

def seed_artists():
    db = Database()
    
    if not os.path.exists(DOWNLOADS_DIR):
        print(f"Directory '{DOWNLOADS_DIR}' not found.")
        return

    print(f"Scanning '{DOWNLOADS_DIR}' for artists...")
    
    count = 0
    for item in os.listdir(DOWNLOADS_DIR):
        item_path = os.path.join(DOWNLOADS_DIR, item)
        if os.path.isdir(item_path):
            artist_name = item
            
            # Check if artist already exists
            if not db.get_artist_by_name(artist_name):
                print(f"Adding artist: {artist_name}")
                db.add_artist(artist_name)
                count += 1
            else:
                print(f"Skipping existing artist: {artist_name}")

    print(f"Seeding complete. Added {count} new artists.")
    db.close()

if __name__ == "__main__":
    seed_artists()
