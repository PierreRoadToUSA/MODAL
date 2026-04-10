import json
import pathlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from requests import RequestException

GRAPH_BASE_URL = "https://api.semanticscholar.org/graph/v1"
DEFAULT_OUT_DIR = "data/papers"
DEFAULT_TIMEOUT = 60
START_YEAR = 1900
YEAR_BUCKET_SIZE = 5
DEFAULT_MAX_WORKERS = 3
MAX_RETRIES = 10
MIN_QUERY_INTERVAL_SECONDS = 1.1
PAPER_BATCH_SIZE = 10000

_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST_TS = 0.0


def _wait_for_query_slot(min_interval_seconds: float = MIN_QUERY_INTERVAL_SECONDS) -> None:
    global _LAST_REQUEST_TS
    with _REQUEST_LOCK:
        now = time.monotonic()
        elapsed = now - _LAST_REQUEST_TS
        wait_time = max(0.0, min_interval_seconds - elapsed)
        if wait_time > 0:
            time.sleep(wait_time)
        _LAST_REQUEST_TS = time.monotonic()


def _get_with_retry(headers: dict, params: dict) -> requests.Response:
    response: requests.Response | None = None
    last_exception: RequestException | None = None
    for attempt in range(MAX_RETRIES + 1):
        _wait_for_query_slot()
        try:
            response = requests.get(
                f"{GRAPH_BASE_URL}/paper/search/bulk",
                headers=headers,
                params=params,
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestException as exc:
            last_exception = exc
            if attempt >= MAX_RETRIES:
                raise
            delay = max(MIN_QUERY_INTERVAL_SECONDS, 2**attempt)
            print(f"Network error. Waiting {delay:.1f}s before retry... ({exc})")
            time.sleep(delay)
            continue

        if response.status_code == 429:
            retry_after_raw = response.headers.get("Retry-After")
            retry_after = 0
            if retry_after_raw and retry_after_raw.isdigit():
                retry_after = int(retry_after_raw)
            delay = max(MIN_QUERY_INTERVAL_SECONDS, retry_after, 2**attempt)
            if attempt >= MAX_RETRIES:
                response.raise_for_status()
            print(f"Rate limited (429). Waiting {delay:.1f}s before retry...")
            time.sleep(delay)
            continue

        if 500 <= response.status_code <= 599:
            delay = max(MIN_QUERY_INTERVAL_SECONDS, 2**attempt)
            if attempt >= MAX_RETRIES:
                response.raise_for_status()
            print(
                f"Server error ({response.status_code}). "
                f"Waiting {delay:.1f}s before retry..."
            )
            time.sleep(delay)
            continue

        break

    if response is None:
        if last_exception is not None:
            raise last_exception
        raise RuntimeError("No response from Semantic Scholar API.")
    response.raise_for_status()
    return response


def _post_with_retry(url: str, headers: dict, payload: dict, params: dict) -> requests.Response:
    response: requests.Response | None = None
    last_exception: RequestException | None = None
    for attempt in range(MAX_RETRIES + 1):
        _wait_for_query_slot()
        try:
            response = requests.post(
                url,
                headers=headers,
                params=params,
                json=payload,
                timeout=DEFAULT_TIMEOUT,
            )
        except RequestException as exc:
            last_exception = exc
            if attempt >= MAX_RETRIES:
                raise
            delay = max(MIN_QUERY_INTERVAL_SECONDS, 2**attempt)
            print(f"Network error. Waiting {delay:.1f}s before retry... ({exc})")
            time.sleep(delay)
            continue

        if response.status_code == 429:
            retry_after_raw = response.headers.get("Retry-After")
            retry_after = 0
            if retry_after_raw and retry_after_raw.isdigit():
                retry_after = int(retry_after_raw)
            delay = max(MIN_QUERY_INTERVAL_SECONDS, retry_after, 2**attempt)
            if attempt >= MAX_RETRIES:
                response.raise_for_status()
            print(f"Rate limited (429). Waiting {delay:.1f}s before retry...")
            time.sleep(delay)
            continue

        if 500 <= response.status_code <= 599:
            delay = max(MIN_QUERY_INTERVAL_SECONDS, 2**attempt)
            if attempt >= MAX_RETRIES:
                response.raise_for_status()
            print(
                f"Server error ({response.status_code}). "
                f"Waiting {delay:.1f}s before retry..."
            )
            time.sleep(delay)
            continue

        break

    if response is None:
        if last_exception is not None:
            raise last_exception
        raise RuntimeError("No response from Semantic Scholar API.")
    response.raise_for_status()
    return response


def _chunked(sequence: list[str], size: int):
    for index in range(0, len(sequence), size):
        yield sequence[index : index + size]


def _extract_targets(items: list[dict], field_name: str) -> list[dict]:
    targets: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        nested = item.get(field_name)
        if isinstance(nested, dict):
            targets.append(nested)
    return targets


def _enrich_with_citations_and_references(headers: dict, papers: list[dict]) -> list[dict]:
    paper_ids = [paper.get("paperId") for paper in papers if paper.get("paperId")]
    if not paper_ids:
        return papers

    fields = (
        "paperId,"
        "references.paperId,references.title,references.year,"
        "citations.paperId,citations.title,citations.year"
    )
    details_by_id: dict[str, dict] = {}
    url = f"{GRAPH_BASE_URL}/paper/batch"

    total_batches = (len(paper_ids) + PAPER_BATCH_SIZE - 1) // PAPER_BATCH_SIZE
    for batch_index, ids_batch in enumerate(_chunked(paper_ids, PAPER_BATCH_SIZE), start=1):
        response = _post_with_retry(
            url=url,
            headers=headers,
            params={"fields": fields},
            payload={"ids": ids_batch},
        )
        payload = response.json()
        if not isinstance(payload, list):
            continue

        for paper_data in payload:
            if not isinstance(paper_data, dict):
                continue
            paper_id = paper_data.get("paperId")
            if not paper_id:
                continue
            references = _extract_targets(paper_data.get("references") or [], "citedPaper")
            citations = _extract_targets(paper_data.get("citations") or [], "citingPaper")
            details_by_id[paper_id] = {
                "references": references,
                "citations": citations,
            }
        print(f"Enrichment progress: {batch_index}/{total_batches} batch(es).")

    for paper in papers:
        paper_id = paper.get("paperId")
        if not paper_id:
            continue
        details = details_by_id.get(paper_id)
        if not details:
            paper["references"] = []
            paper["citations"] = []
            continue
        paper["references"] = details.get("references", [])
        paper["citations"] = details.get("citations", [])

    return papers


def search_ml_papers_for_year_range(
    headers: dict, year_range: str, batch_size: int = 10000
) -> list[dict]:
    papers: list[dict] = []
    token: str | None = None
    query = "machine learning"
    # `/paper/search/bulk` only supports basic paper fields.
    fields = (
        "paperId,title,abstract,year,venue,url,fieldsOfStudy,s2FieldsOfStudy,"
        "citationCount,influentialCitationCount,authors"
    )

    while True:
        params = {
            "query": query,
            "fields": fields,
            "limit": batch_size,
            "year": year_range,
        }
        if token:
            params["token"] = token

        try:
            response = _get_with_retry(headers=headers, params=params)
        except RequestException as exc:
            print(
                f"[{year_range}] Request failed after retries: {exc}. "
                f"Keeping {len(papers)} papers collected so far for this range."
            )
            break
        data = response.json()

        batch = data.get("data") or []
        if not batch:
            break
        papers.extend(batch)

        token = data.get("token")
        if not token:
            break

        print(f"[{year_range}] Collected {len(papers)} papers...")

    print(f"[{year_range}] Done with {len(papers)} papers.")
    return papers


def build_year_ranges(start_year: int) -> list[str]:
    ranges: list[str] = []
    current_year = datetime.now().year
    year = start_year
    while year <= current_year:
        end_year = min(year + YEAR_BUCKET_SIZE - 1, current_year)
        ranges.append(f"{year}-{end_year}")
        year = end_year + 1
    return ranges


def dedupe_papers(papers: list[dict]) -> list[dict]:
    seen_ids: set[str] = set()
    unique: list[dict] = []
    for paper in papers:
        paper_id = paper.get("paperId")
        if not paper_id:
            continue
        if paper_id in seen_ids:
            continue
        seen_ids.add(paper_id)
        unique.append(paper)
    return unique


def search_ml_papers(headers: dict, max_workers: int = DEFAULT_MAX_WORKERS) -> list[dict]:
    year_ranges = build_year_ranges(start_year=START_YEAR)
    all_papers: list[dict] = []

    print(
        f"Searching ML papers across {len(year_ranges)} year ranges with {max_workers} threads..."
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                search_ml_papers_for_year_range,
                headers,
                year_range,
            ): year_range
            for year_range in year_ranges
        }
        for future in as_completed(futures):
            year_range = futures[future]
            try:
                papers_for_range = future.result()
            except Exception as exc:
                print(f"Failed {year_range}: {exc}")
                continue
            all_papers.extend(papers_for_range)
            print(
                f"Finished {year_range}: +{len(papers_for_range)} papers "
                f"(raw total {len(all_papers)})"
            )

    unique_papers = dedupe_papers(all_papers)
    print(f"Deduplicated total: {len(unique_papers)} papers.")
    return unique_papers


def save_jsonl(records: list[dict], destination: pathlib.Path) -> None:
    with destination.open("wt", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    out_dir = DEFAULT_OUT_DIR

    api_key = "2y5uBuUCdd49YFT5Jsuhv5LGG2Wfh5CJaLIZfFz5"
    if not api_key:
        raise RuntimeError("Missing API key. Export S2_API_KEY before running.")

    headers = {"x-api-key": api_key}

    output_dir = pathlib.Path(out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / "machine_learning_papers.jsonl"

    print("Searching all available ML papers...")
    papers = search_ml_papers(headers=headers)
    print("Enriching papers with references and citations...")
    papers = _enrich_with_citations_and_references(headers=headers, papers=papers)
    save_jsonl(records=papers, destination=target)
    print(f"Saved {len(papers)} papers to {target}")


if __name__ == "__main__":
    main()