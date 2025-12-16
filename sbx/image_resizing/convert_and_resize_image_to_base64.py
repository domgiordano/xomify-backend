import os
import json
import base64
from PIL import Image
from io import BytesIO

INPUT_DIR = "wrapped_2026"
OUTPUT_JSON = "wrapped_2026_base64.json"

MAX_SIZE = 262144  # 256 KB
JPEG_QUALITY = 70
DIMENSIONS = (300, 300)

def png_to_spotify_base64(path):
    img = Image.open(path).convert("RGB")
    img = img.resize(DIMENSIONS)

    buffer = BytesIO()
    img.save(buffer, format="JPEG", quality=JPEG_QUALITY)

    data = buffer.getvalue()

    if len(data) > MAX_SIZE:
        raise ValueError(f"{os.path.basename(path)} exceeds Spotify size limit")

    return base64.b64encode(data).decode("utf-8"), len(data)

def process_folder(input_dir):
    output = {}

    for filename in os.listdir(input_dir):
        if not filename.lower().endswith(".png"):
            continue

        path = os.path.join(input_dir, filename)

        try:
            base64_image, size = png_to_spotify_base64(path)
            output[int(filename.split(".")[0])] = base64_image
            print(f"✅ {filename} → {size} bytes")
        except Exception as e:
            print(f"❌ {filename}: {e}")

    output_sorted = {k: output[k] for k in sorted(output.keys())}
    return output_sorted

if __name__ == "__main__":
    result = process_folder(INPUT_DIR)

    with open(OUTPUT_JSON, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nSaved {len(result)} images to {OUTPUT_JSON}")
