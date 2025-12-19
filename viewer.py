import os
import io
import sqlite3
import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, Response, RedirectResponse
from fastapi.templating import Jinja2Templates
from database import Database

app = FastAPI()
db = Database()

# Cache for parquet files to avoid re-reading from disk constantly
# Key: shard_filename, Value: DataFrame
parquet_cache = {}

PARQUET_DIR = os.path.join("hoard", "parquet")

def get_image_data(shard_file, offset):
    global parquet_cache
    
    # shard_file now includes the source folder, e.g. "danbooru/shard_0001.parquet"
    shard_path = os.path.join(PARQUET_DIR, shard_file)
    
    if shard_file not in parquet_cache:
        if not os.path.exists(shard_path):
            return None
        # Read the parquet file
        df = pd.read_parquet(shard_path)
        parquet_cache[shard_file] = df
    else:
        df = parquet_cache[shard_file]
    
    try:
        # Get the row
        row = df.iloc[offset]
        return row['image_bytes']
    except IndexError:
        return None

@app.post("/add_artist")
async def add_artist(name: str = Form(...), source: str = Form("danbooru")):
    try:
        db.cursor.execute("INSERT INTO artists (name, source) VALUES (?, ?)", (name, source))
        db.conn.commit()
    except sqlite3.IntegrityError:
        pass # Already exists
    return RedirectResponse(url="/", status_code=303)

@app.post("/upload_artists")
async def upload_artists(file: UploadFile = File(...)):
    contents = await file.read()
    filename = file.filename.lower()
    
    try:
        if filename.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(contents))
        elif filename.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Invalid file format")
            
        # Normalize columns
        df.columns = [c.lower().strip() for c in df.columns]
        
        if 'name' not in df.columns:
             raise HTTPException(status_code=400, detail="File must contain a 'name' column")
             
        count = 0
        for _, row in df.iterrows():
            name = str(row['name']).strip()
            source = str(row['source']).strip() if 'source' in df.columns and pd.notna(row['source']) else 'danbooru'
            
            if name:
                try:
                    db.cursor.execute("INSERT INTO artists (name, source) VALUES (?, ?)", (name, source))
                    count += 1
                except sqlite3.IntegrityError:
                    pass
        
        db.conn.commit()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
        
    return RedirectResponse(url="/", status_code=303)

@app.get("/", response_class=HTMLResponse)
async def index():
    artists = db.get_all_artists()
    
    # Get total images count
    db.cursor.execute("SELECT COUNT(*) FROM images")
    total_images = db.cursor.fetchone()[0]
    
    html = f"""
    <html>
    <head>
        <title>Hoard Viewer</title>
        <style>
            body {{ font-family: sans-serif; margin: 20px; background: #222; color: #eee; }}
            a {{ color: #4da6ff; text-decoration: none; }}
            .stats {{ margin-bottom: 20px; padding: 10px; background: #333; border-radius: 5px; }}
            .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px; }}
            .card {{ background: #333; padding: 5px; border-radius: 5px; text-align: center; }}
            .card img {{ max-width: 100%; height: auto; display: block; margin-bottom: 5px; }}
            .artist-list {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 20px; }}
            .artist-tag {{ background: #444; padding: 5px 10px; border-radius: 15px; font-size: 0.9em; }}
        </style>
    </head>
    <body>
        <h1>Hoard Viewer</h1>
        
        <div class="stats">
            <strong>Total Images:</strong> {total_images} | 
            <strong>Artists:</strong> {len(artists)}
        </div>

        <div style="margin-bottom: 20px; padding: 10px; background: #333; border-radius: 5px;">
            <h3>Add Artist</h3>
            <div style="display: flex; gap: 20px;">
                <form action="/add_artist" method="post">
                    <input type="text" name="name" placeholder="Artist Name (e.g. wachiwo)" required style="padding: 5px;">
                    <select name="source" style="padding: 5px;">
                        <option value="danbooru">Danbooru</option>
                        <option value="gelbooru">Gelbooru</option>
                    </select>
                    <button type="submit" style="padding: 5px 10px; background: #4da6ff; border: none; color: white; cursor: pointer;">Add</button>
                </form>
                
                <form action="/upload_artists" method="post" enctype="multipart/form-data" style="border-left: 1px solid #555; padding-left: 20px;">
                    <label>Import CSV/Excel:</label>
                    <input type="file" name="file" accept=".csv, .xlsx" required style="color: #eee;">
                    <button type="submit" style="padding: 5px 10px; background: #4da6ff; border: none; color: white; cursor: pointer;">Upload</button>
                    <br>
                    <small style="color: #aaa;">Columns: name, source (optional)</small>
                </form>
            </div>
        </div>

        <div class="artist-list">
            {''.join([f'<a href="/artist/{a["id"]}" class="artist-tag">{a["name"]} ({a["probability_weight"]:.1f})</a>' for a in artists])}
        </div>

        <h2>Latest Images</h2>
        <div class="grid">
    """
    
    # Get latest 50 images
    db.cursor.execute("SELECT id, post_id, size FROM images ORDER BY timestamp DESC LIMIT 50")
    images = db.cursor.fetchall()
    
    for img in images:
        size_kb = img['size'] / 1024 if img['size'] else 0

        html += f"""
            <div class="card">
                <a href="/img_view/{img['id']}">
                    <img src="/img/{img['id']}" loading="lazy">
                </a>
                <small>{img['post_id']} ({size_kb:.1f} KB)</small>
            </div>
        """
    
    html += """
        </div>
    </body>
    </html>
    """
    return html

@app.get("/artist/{artist_id}", response_class=HTMLResponse)
async def artist_view(artist_id: int):
    db.cursor.execute("SELECT * FROM artists WHERE id = ?", (artist_id,))
    artist = db.cursor.fetchone()
    
    if not artist:
        raise HTTPException(status_code=404, detail="Artist not found")

    db.cursor.execute("SELECT id, post_id, size FROM images WHERE artist_id = ? ORDER BY timestamp DESC LIMIT 100", (artist_id,))
    images = db.cursor.fetchall()

    html = f"""
    <html>
    <head>
        <title>{artist['name']} - Hoard Viewer</title>
        <style>
            body {{ font-family: sans-serif; margin: 20px; background: #222; color: #eee; }}
            a {{ color: #4da6ff; text-decoration: none; }}
            .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 10px; }}
            .card {{ background: #333; padding: 5px; border-radius: 5px; text-align: center; }}
            .card img {{ max-width: 100%; height: auto; display: block; margin-bottom: 5px; }}
        </style>
    </head>
    <body>
        <h1><a href="/">Home</a> / {artist['name']}</h1>
        <div class="grid">
    """
    
    for img in images:
        size_kb = img['size'] / 1024 if img['size'] else 0

        html += f"""
            <div class="card">
                <a href="/img_view/{img['id']}">
                    <img src="/img/{img['id']}" loading="lazy">
                </a>
                <small>{img['post_id']} ({size_kb:.1f} KB)</small>
            </div>
        """
    
    html += "</div></body></html>"
    return html

@app.get("/img/{image_id}")
async def get_image(image_id: int):
    db.cursor.execute("SELECT shard_file, offset FROM images WHERE id = ?", (image_id,))
    result = db.cursor.fetchone()
    
    if not result:
        raise HTTPException(status_code=404, detail="Image not found")
    
    shard_file, offset = result
    image_bytes = get_image_data(shard_file, offset)
    
    if not image_bytes:
        raise HTTPException(status_code=500, detail="Could not read image data")
        
    return Response(content=image_bytes, media_type="image/webp")

@app.get("/img_view/{image_id}", response_class=HTMLResponse)
async def view_image_page(image_id: int):
    db.cursor.execute("SELECT * FROM images WHERE id = ?", (image_id,))
    img = db.cursor.fetchone()
    
    if not img:
        raise HTTPException(status_code=404, detail="Image not found")

    html = f"""
    <html>
    <body style="background: #222; color: #eee; text-align: center;">
        <a href="/" style="color: #4da6ff;">Home</a>
        <br><br>
        <img src="/img/{image_id}" style="max-height: 90vh; max-width: 90vw;">
        <p>Post ID: {img['post_id']} | Hash: {img['hash']}</p>
    </body>
    </html>
    """
    return html

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
