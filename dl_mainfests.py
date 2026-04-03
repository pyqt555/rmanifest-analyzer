import asyncio
import httpx
import os
import json
import time 
# --- Configuration ---
CATALOG_URL = "https://raw.githubusercontent.com/RiotArchiveProject/catalog-download-script/refs/heads/main/catalog.json"
BASE_CDN_URL = "https://lol.secure.dyn.riotcdn.net/channels/public/releases/{}.manifest"
OUTPUT_DIR = "manifests"
CONCURRENT_LIMIT = 20 # Async is so light you can safely go higher than 8
MAX_FILES = 10000

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
}

async def download_file(client, h, semaphore):
    file_path = os.path.join(OUTPUT_DIR, f"{h}.manifest")
    if os.path.exists(file_path):
        return False

    url = BASE_CDN_URL.format(h)
    
    # Semaphore controls actual simultaneous connections
    async with semaphore:
        try:
            resp = await client.get(url, timeout=30.0)
            if resp.status_code == 200:
                # Use a thread pool for the disk write to avoid blocking the event loop
                await asyncio.to_thread(save_to_disk, file_path, resp.content)
                return True
        except Exception as e:
            print(f"Error downloading {h}: {e}")
    return False

def save_to_disk(path, content):
    with open(path, "wb") as f:
        f.write(content)

async def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 1. Get Catalog
    async with httpx.AsyncClient(headers=HEADERS) as client:
        print("Fetching catalog...")
        r = await client.get(CATALOG_URL)
        catalog = r.json()

    lol_manifests = catalog.get("lol", {})
    hashes = [h for h, data in lol_manifests.items() 
              if data.get("platform") in ["Windows", "Neutral"]][:MAX_FILES]

    print(f"Starting async download for {len(hashes)} files...")

    # 2. Limit concurrency with a Semaphore
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)

    # 3. Create a single client with HTTP/2 enabled
    # HTTP/2 is the biggest speed boost here
    async with httpx.AsyncClient(http2=True, headers=HEADERS, limits=httpx.Limits(max_connections=CONCURRENT_LIMIT)) as client:
        tasks = [download_file(client, h, semaphore) for h in hashes]
        
        # Process as they complete
        completed = 0
        start_time=time.time()
        for task in asyncio.as_completed(tasks):
            success = await task
            completed += 1
            if completed % 25 == 0:
                elapsed=time.time()-start_time
                expected=(elapsed/completed)*(len(hashes)-completed)
                print(f"Progress: {completed}/{len(hashes)}, {elapsed}, {expected}")

    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())