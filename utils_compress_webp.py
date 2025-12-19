import os

# Add libvips to PATH before importing pyvips
# Ensure this path points to the 'bin' folder containing libvips-42.dll
vipshome = r'C:\Users\darlis\Downloads\vips-dev-w64-web-8.17.3-static-ffi\vips-dev-8.17\bin'
os.environ['PATH'] = vipshome + ';' + os.environ['PATH']

import pyvips
import tqdm
import shutil
from concurrent.futures import ThreadPoolExecutor

# --- Configuration (Danbooru Defaults) ---
INPUT_DIR = r"C:\Users\darlis\AppData\Roaming\Kemono Downloader\Kemono Downloader\Downloads\122450244_Qewie"        # Folder containing your big images
OUTPUT_DIR = r"C:\Users\darlis\AppData\Roaming\Kemono Downloader\Kemono Downloader\Downloads\122450244_Qewie_resized_webp3"  # Folder to save samples
MAX_DIMENSION = 850            # Max dimension (width for portrait, height for landscape)
MIN_FILESIZE_KB = 200          # Minimum filesize (KB) to process. Smaller files are just copied.
WEBP_QUALITY_MAX = 65          # Max quality (standard images)
WEBP_QUALITY_MIN = 60          # Min quality (very large/long images)
THREADS = os.cpu_count()       # Maximize CPU usage

# Supported extensions
VALID_EXTS = {'.jpg', '.jpeg', '.png', '.webp', '.bmp'}

def process_image(file_info):
    """
    Resizes a single image using Danbooru's exact libvips strategy.
    """
    input_path, output_path = file_info
    
    try:
        # Check for minimum filesize
        if os.path.getsize(input_path) < MIN_FILESIZE_KB * 1024:
            shutil.copy2(input_path, output_path)
            return True

        # 1. Load the image efficiently (access='sequential' is faster for file-to-file)
        # fail_on_error=False allows processing slightly corrupted images
        # Note: fail_on_error is not supported by all loaders in older/some versions of libvips, removing it for compatibility
        image = pyvips.Image.new_from_file(input_path, access='sequential')

        # 2. Determine resize targets based on orientation
        # Portrait (Height > Width) -> Constrain Width to MAX_DIMENSION
        # Landscape (Width > Height) -> Constrain Height to MAX_DIMENSION
        if image.width > image.height:
            # Landscape
            target_width = 10000000
            target_height = MAX_DIMENSION
        else:
            # Portrait or Square
            target_width = MAX_DIMENSION
            target_height = 10000000

        # 3. Intelligent Resizing & Color Profile Handling
        # Danbooru uses 'thumbnail_image' which maps to pyvips.thumbnail.
        # This handles the complex logic seen in your Ruby snippet:
        # - Auto-converts to sRGB (export_profile="srgb")
        # - Handles CMYK/Grayscale conversion correctly
        # - Uses high-quality Lanczos3 downscaling
        thumb = pyvips.Image.thumbnail(
            input_path, 
            target_width,
            height=target_height,
            size='down',     # Only downscale, never upscale
            export_profile='srgb' # Ensure web-safe colors (fixes dull colors)
        )

        # Calculate adaptive quality based on aspect ratio
        # For very long/tall images (high aspect ratio), reduce quality slightly to save space
        w = thumb.width
        h = thumb.height
        aspect_ratio = max(w, h) / min(w, h)

        ratio_threshold_low = 1.3
        ratio_threshold_high = 2
        
        if aspect_ratio <= ratio_threshold_low:
            q_value = WEBP_QUALITY_MAX
        elif aspect_ratio >= ratio_threshold_high:
            q_value = WEBP_QUALITY_MIN
        else:
            # Linear interpolation
            progress = (aspect_ratio - ratio_threshold_low) / (ratio_threshold_high - ratio_threshold_low)
            q_value = int(WEBP_QUALITY_MAX - (progress * (WEBP_QUALITY_MAX - WEBP_QUALITY_MIN)))

        # 4. Save with WebP Optimization
        thumb.write_to_file(
            output_path,
            Q=q_value,
            strip=True,                 # Remove metadata (EXIF) to save space
            effort=6,                   # CPU effort 0-6 (6 is slowest but best compression)
            smart_subsample=True        # High quality subsampling
        )
        return True

    except pyvips.Error as e:
        print(f"\n[Error] Could not process {os.path.basename(input_path)}: {e}")
        return False
    except Exception as e:
        print(f"\n[Error] Unexpected error on {os.path.basename(input_path)}: {e}")
        return False

def main():
    if not os.path.exists(INPUT_DIR):
        print(f"Error: Input directory '{INPUT_DIR}' not found.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Gather tasks
    tasks = []
    files = [f for f in os.listdir(INPUT_DIR) if os.path.splitext(f)[1].lower() in VALID_EXTS]
    
    print(f"Found {len(files)} images. optimizing with libvips...")

    for f in files:
        in_path = os.path.join(INPUT_DIR, f)
        filename_no_ext, ext = os.path.splitext(f)
        
        # Determine output path based on file size
        if os.path.getsize(in_path) < MIN_FILESIZE_KB * 1024:
            out_path = os.path.join(OUTPUT_DIR, f"{filename_no_ext}_sample{ext}")
        else:
            out_path = os.path.join(OUTPUT_DIR, f"{filename_no_ext}_sample.webp")
        
        # Skip if exists (optional)
        if not os.path.exists(out_path):
            tasks.append((in_path, out_path))

    # Run in parallel
    # libvips releases the GIL, so threading works beautifully here.
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        results = list(tqdm.tqdm(
            executor.map(process_image, tasks), 
            total=len(tasks), 
            desc="Processing Images",
            unit="img"
        ))

    success = results.count(True)
    print(f"\nDone! Processed {success}/{len(tasks)} images.")

if __name__ == '__main__':
    main()