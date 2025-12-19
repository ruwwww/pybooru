import os
import io
import imagehash
from PIL import Image

# Add libvips to PATH before importing pyvips
vipshome = r'C:\Users\darlis\Downloads\vips-dev-w64-web-8.17.3-static-ffi\vips-dev-8.17\bin'
if os.path.exists(vipshome):
    os.environ['PATH'] = vipshome + ';' + os.environ['PATH']

import pyvips

# Configuration
MAX_DIMENSION = 850
WEBP_QUALITY_MAX = 65
WEBP_QUALITY_MIN = 60

def process_image_bytes(image_bytes):
    """
    Resizes and compresses image bytes using libvips.
    Returns compressed bytes (WebP).
    """
    try:
        # Load image from memory
        image = pyvips.Image.new_from_buffer(image_bytes, "")

        # Determine resize targets
        if image.width > image.height:
            # Landscape
            target_width = 10000000
            target_height = MAX_DIMENSION
        else:
            # Portrait or Square
            target_width = MAX_DIMENSION
            target_height = 10000000

        # Resize
        thumb = image.thumbnail_image(
            target_width,
            height=target_height,
            size='down',
            export_profile='srgb'
        )

        # Calculate adaptive quality
        w = thumb.width
        h = thumb.height
        aspect_ratio = max(w, h) / min(w, h) if min(w, h) > 0 else 1

        ratio_threshold_low = 1.3
        ratio_threshold_high = 2
        
        if aspect_ratio <= ratio_threshold_low:
            q_value = WEBP_QUALITY_MAX
        elif aspect_ratio >= ratio_threshold_high:
            q_value = WEBP_QUALITY_MIN
        else:
            progress = (aspect_ratio - ratio_threshold_low) / (ratio_threshold_high - ratio_threshold_low)
            q_value = int(WEBP_QUALITY_MAX - (progress * (WEBP_QUALITY_MAX - WEBP_QUALITY_MIN)))

        # Save to buffer
        output_buffer = thumb.write_to_buffer(
            ".webp",
            Q=q_value,
            strip=True,
            effort=6,
            smart_subsample=True
        )
        
        return output_buffer

    except Exception as e:
        print(f"Error processing image: {e}")
        return None

def calculate_hash(image_bytes):
    """
    Calculates perceptual hash of the image.
    """
    try:
        image = Image.open(io.BytesIO(image_bytes))
        return str(imagehash.phash(image))
    except Exception as e:
        print(f"Error calculating hash: {e}")
        return None
