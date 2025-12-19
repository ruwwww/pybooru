import sqlite3
import os
from datetime import datetime

# Store DB in the hoard directory for easier container persistence
os.makedirs("hoard", exist_ok=True)
DB_PATH = os.path.join("hoard", "hoard.db")

class Database:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.init_db()

    def connect(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def init_db(self):
        # 1. artists
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS artists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                source TEXT DEFAULT 'danbooru',
                favorite BOOLEAN DEFAULT 0,
                last_scraped_post TEXT,
                avg_upload_interval_hours REAL DEFAULT 24.0,
                probability_weight REAL DEFAULT 1.0,
                last_checked DATETIME
            )
        """)

        # 2. images
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artist_id INTEGER,
                source TEXT,
                post_id TEXT,
                hash TEXT,
                timestamp DATETIME,
                shard_file TEXT,
                offset INTEGER,
                size INTEGER,
                FOREIGN KEY(artist_id) REFERENCES artists(id)
            )
        """)

        # 3. hash_index
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS hash_index (
                hash TEXT PRIMARY KEY,
                image_id INTEGER,
                FOREIGN KEY(image_id) REFERENCES images(id)
            )
        """)
        
        self.conn.commit()

    def add_artist(self, name, source='danbooru', favorite=False):
        try:
            self.cursor.execute("""
                INSERT INTO artists (name, source, favorite, probability_weight, last_checked)
                VALUES (?, ?, ?, ?, ?)
            """, (name, source, favorite, 10.0 if favorite else 1.0, datetime.now()))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Error adding artist {name}: {e}")
            return None

    def get_artist_by_name(self, name):
        self.cursor.execute("SELECT * FROM artists WHERE name = ?", (name,))
        return self.cursor.fetchone()

    def get_all_artists(self):
        self.cursor.execute("SELECT * FROM artists")
        return self.cursor.fetchall()

    def close(self):
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    db = Database()
    print(f"Database initialized at {DB_PATH}")
    db.close()
