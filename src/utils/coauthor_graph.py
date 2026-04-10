import argparse
import csv
import gzip
import json
from collections import Counter
from itertools import combinations
from pathlib import Path


def _open_text_auto(path: Path, mode: str = "rt"):
    if path.suffix == ".gz":
        return gzip.open(path, mode, encoding="utf-8", newline="")
    return path.open(mode, encoding="utf-8", newline="")


def _normalize_author(author: dict) -> tuple[str, str] | None:
    author_id = str(author.get("authorId") or "").strip()
    name = str(author.get("name") or "").strip()
    if author_id:
        return (f"id:{author_id}", name or author_id)
    if not name:
        return None
    normalized_name = " ".join(name.lower().split())
    return (f"name:{normalized_name}", name)


def _read_papers(path: Path):
    with _open_text_auto(path, "rt") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def build_coauthor_graph(input_path: Path) -> tuple[Counter, Counter, dict]:
    node_paper_count: Counter = Counter()
    edge_weights: Counter = Counter()
    node_labels: dict[str, str] = {}

    papers_seen = 0
    papers_with_coauthors = 0
    malformed_author_entries = 0

    for paper in _read_papers(input_path):
        papers_seen += 1
        authors = paper.get("authors") or []
        normalized_authors: list[tuple[str, str]] = []
        seen_in_paper: set[str] = set()

        for author in authors:
            if not isinstance(author, dict):
                malformed_author_entries += 1
                continue
            normalized = _normalize_author(author)
            if not normalized:
                malformed_author_entries += 1
                continue

            author_key, display_name = normalized
            if author_key in seen_in_paper:
                continue
            seen_in_paper.add(author_key)
            normalized_authors.append((author_key, display_name))
            node_labels.setdefault(author_key, display_name)
            node_paper_count[author_key] += 1

        if len(normalized_authors) < 2:
            continue

        papers_with_coauthors += 1
        keys = [item[0] for item in normalized_authors]
        for a, b in combinations(sorted(keys), 2):
            edge_weights[(a, b)] += 1

    metadata = {
        "papers_seen": papers_seen,
        "papers_with_coauthors": papers_with_coauthors,
        "malformed_author_entries": malformed_author_entries,
    }
    return node_paper_count, edge_weights, {**node_labels, **metadata}


def _split_graph_output(output_dir: Path) -> tuple[Path, Path]:
    edges_path = output_dir / "coauthor_edges.csv"
    stats_path = output_dir / "coauthor_stats.json"
    return edges_path, stats_path


def write_edges(
    edges: Counter,
    labels: dict[str, str],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with _open_text_auto(output_path, "wt") as handle:
        writer = csv.writer(handle)
        writer.writerow(["author_a_key", "author_a_name", "author_b_key", "author_b_name", "weight"])
        for (a, b), weight in edges.most_common():
            writer.writerow([a, labels.get(a, ""), b, labels.get(b, ""), weight])


def write_stats(
    node_paper_count: Counter,
    edges: Counter,
    labels: dict[str, str],
    metadata: dict,
    output_path: Path,
) -> None:
    degrees = Counter()
    for (a, b), _weight in edges.items():
        degrees[a] += 1
        degrees[b] += 1

    top_by_papers = [
        {
            "author_key": key,
            "author_name": labels.get(key, ""),
            "paper_count": count,
            "coauthor_count": degrees.get(key, 0),
        }
        for key, count in node_paper_count.most_common(25)
    ]
    top_by_degree = [
        {
            "author_key": key,
            "author_name": labels.get(key, ""),
            "coauthor_count": count,
            "paper_count": node_paper_count.get(key, 0),
        }
        for key, count in degrees.most_common(25)
    ]
    top_edges = [
        {
            "author_a_key": a,
            "author_a_name": labels.get(a, ""),
            "author_b_key": b,
            "author_b_name": labels.get(b, ""),
            "weight": weight,
        }
        for (a, b), weight in edges.most_common(25)
    ]

    payload = {
        "summary": {
            **metadata,
            "unique_authors": len(node_paper_count),
            "unique_edges": len(edges),
        },
        "top_authors_by_papers": top_by_papers,
        "top_authors_by_coauthors": top_by_degree,
        "top_collaborations_by_weight": top_edges,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build an author co-authorship graph from machine_learning_papers JSONL."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/papers/machine_learning_papers.jsonl.gz"),
        help="Input JSONL file (.jsonl or .jsonl.gz).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/graphs"),
        help="Directory where edges and stats are written.",
    )
    args = parser.parse_args()

    node_paper_count, edges, labels_and_metadata = build_coauthor_graph(args.input)
    metadata = {
        "papers_seen": labels_and_metadata.pop("papers_seen"),
        "papers_with_coauthors": labels_and_metadata.pop("papers_with_coauthors"),
        "malformed_author_entries": labels_and_metadata.pop("malformed_author_entries"),
    }
    labels = labels_and_metadata

    edges_path, stats_path = _split_graph_output(args.output_dir)
    write_edges(edges=edges, labels=labels, output_path=edges_path)
    write_stats(
        node_paper_count=node_paper_count,
        edges=edges,
        labels=labels,
        metadata=metadata,
        output_path=stats_path,
    )

    print(f"Done. Wrote {len(edges)} edges to {edges_path}")
    print(f"Wrote graph stats to {stats_path}")


if __name__ == "__main__":
    main()
