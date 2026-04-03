import os
import json
import csv
import asyncio
import aiohttp
from tqdm.asyncio import tqdm
from google.cloud import storage

# ================= Configuration =================
BASE_CDN_URL = "https://xcdn.next.co.uk/common/items/default/default/itemimages/3_4Ratio/product/lge/"
BUCKET_NAME = "your-bucket-name"
FAILED_LOG_FILE = "failed_downloads_p8.csv"

MAX_RETRIES = 4
MAX_CONCURRENT_REQUESTS = 20

JSON_FILE = "output_p8.json"

# ================= Init =================
storage_client = storage.Client()
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# ================= GCS Helpers =================

def get_existing_images(prod_id):
    """Fetch all existing images for a product (FAST resume)"""
    prefix = f"{prod_id}/"
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix)
    return set(blob.name.split("/")[-1] for blob in blobs)


def upload_to_gcs(blob_name, content):
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(content)


# ================= Download Logic =================

async def download_with_retries(session, url):
    error_msg = "Unknown error"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=10) as response:

                    if response.status == 200:
                        return True, await response.read(), None

                    elif response.status == 404:
                        return False, None, "HTTP 404"

                    else:
                        error_msg = f"HTTP {response.status}"

        except Exception as e:
            error_msg = str(e)

        if attempt < MAX_RETRIES:
            await asyncio.sleep(1)

    return False, None, error_msg


async def process_image(session, prod_id, image_name, existing_images, writer):
    """Download + upload one image"""

    # ✅ Skip if already in GCS (FAST)
    if image_name in existing_images:
        return True, None

    blob_name = f"{prod_id}/{image_name}"
    image_url = BASE_CDN_URL + image_name + "?im=Resize,width=750"

    success, content, error = await download_with_retries(session, image_url)

    if success:
        upload_to_gcs(blob_name, content)
    else:
        writer.writerow([prod_id, image_name, error])

    return success, error


async def download_product(session, prod_id, writer):
    print(f"\nProcessing Product: {prod_id}")

    # ✅ Get existing files once (FAST resume)
    existing_images = get_existing_images(prod_id)

    tasks = []

    for i in range(1, 11):
        image_name = f"{prod_id}s.jpg" if i == 1 else f"{prod_id}s{i}.jpg"

        # Skip existing
        if image_name in existing_images:
            continue

        tasks.append(
            process_image(session, prod_id, image_name, existing_images, writer)
        )

    if not tasks:
        print(f"All images already exist in GCS for {prod_id} ✔")
        return

    results = await asyncio.gather(*tasks)

    # Stop logic (optional insight)
    for success, error in results:
        if error == "HTTP 404":
            break


# ================= Main =================

async def main():
    # CSV setup
    file_exists = os.path.isfile(FAILED_LOG_FILE)
    csvfile = open(FAILED_LOG_FILE, mode="a", newline="", encoding="utf-8")
    writer = csv.writer(csvfile)

    if not file_exists:
        writer.writerow(["product_id", "image_name", "error"])

    # Load product IDs
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        products_list = json.load(f)

    product_ids = [
        p["product_id"]
        for p in products_list
        if p.get("product_id")
    ]

    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0"}
    ) as session:

        tasks = [
            download_product(session, pid, writer)
            for pid in product_ids
        ]

        for f in tqdm(asyncio.as_completed(tasks),
                      total=len(tasks),
                      desc="Downloading products"):
            await f

    csvfile.close()
    print("\n✅ All uploads completed")


if __name__ == "__main__":
    asyncio.run(main())


# pip install aiohttp tqdm google-cloud-storage
