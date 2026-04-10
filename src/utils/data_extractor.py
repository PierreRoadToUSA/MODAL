import argparse
import os
import pathlib
import time
from typing import Iterable

import requests

BASE_URL = "https://api.semanticscholar.org/datasets/v1"
DEFAULT_DATASET = "s2orc"
DEFAULT_TIMEOUT = 60
DEFAULT_RETRIES = 5
CHUNK_SIZE = 1024 * 1024  # 1MB


def api_get(url: str, headers: dict, retries: int = DEFAULT_RETRIES) -> dict:
    for attempt in range(1, retries + 1):
        response = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
        if response.status_code == 429 and attempt < retries:
            # API key can still be throttled: simple exponential backoff.
            sleep_s = 2 ** attempt
            print(f"Rate limited (429). Retry in {sleep_s}s...")
            time.sleep(sleep_s)
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError(f"Failed GET {url} after {retries} attempts")


def filename_from_url(url: str) -> str:
    return url.split("?")[0].rstrip("/").split("/")[-1]


def stream_download(url: str, destination: pathlib.Path, overwrite: bool) -> None:
    if destination.exists() and not overwrite:
        print(f"Skip existing file: {destination.name}")
        return

    with requests.get(url, stream=True, timeout=DEFAULT_TIMEOUT) as response:
        response.raise_for_status()
        tmp_path = destination.with_suffix(destination.suffix + ".part")
        with open(tmp_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if chunk:
                    handle.write(chunk)
        tmp_path.replace(destination)
    print(f"Downloaded: {destination.name}")


def get_release_id(headers: dict, release: str | None) -> str:
    if release:
        return release
    data = api_get(f"{BASE_URL}/release/latest", headers=headers)
    return data["release_id"]


def get_dataset_urls(headers: dict, release_id: str, dataset: str) -> Iterable[str]:
    data = api_get(f"{BASE_URL}/release/{release_id}/dataset/{dataset}", headers=headers)
    return data["files"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Semantic Scholar S2AG dataset shards (e.g. s2orc)."
    )
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Dataset name (default: s2orc)")
    parser.add_argument("--release", default=None, help="Release id (default: latest)")
    parser.add_argument(
        "--out",
        default="data/s2orc",
        help="Output directory where .gz files are saved (default: data/s2orc)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    api_key = "2y5uBuUCdd49YFT5Jsuhv5LGG2Wfh5CJaLIZfFz5"
    if not api_key:
        raise RuntimeError("Missing API key. Export S2_API_KEY before running.")

    headers = {"x-api-key": api_key}
    release_id = get_release_id(headers=headers, release=args.release)
    print(f"Release: {release_id}")

    output_dir = pathlib.Path(args.out)
    output_dir.mkdir(parents=True, exist_ok=True)

    urls = list(get_dataset_urls(headers=headers, release_id=release_id, dataset=args.dataset))
    print(f"Found {len(urls)} shard(s) for dataset '{args.dataset}'")

    for idx, url in enumerate(urls, start=1):
        filename = filename_from_url(url)
        target = output_dir / filename
        print(f"[{idx}/{len(urls)}] {filename}")
        stream_download(url=url, destination=target, overwrite=args.overwrite)

    print("All downloads completed.")


if __name__ == "__main__":
    main()