#!/usr/bin/env python3
"""Build a small local Lichess puzzle sample from a larger CSV.

This script is intentionally streaming-only so it can safely process a large
CSV without loading the full Lichess database into memory.

Example:
    python scripts/build_lichess_puzzle_sample.py ^
        --input path/to/lichess_db_puzzle.csv ^
        --output lichess_puzzles_sample.csv ^
        --limit 1000 ^
        --min-rating 800 ^
        --max-rating 2000 ^
        --themes fork,pin,sacrifice ^
        --seed 42
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT_DIR / "lichess_puzzles_sample.csv"

REQUIRED_COLUMNS = {"PuzzleId", "FEN", "Moves", "Rating", "Themes"}
PRESERVED_COLUMNS = [
    "PuzzleId",
    "FEN",
    "Moves",
    "Rating",
    "RatingDeviation",
    "Popularity",
    "NbPlays",
    "Themes",
    "GameUrl",
    "OpeningTags",
]


def normalize_token(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def parse_theme_tokens(raw_value: str) -> list[str]:
    value = str(raw_value or "").strip()
    if not value:
        return []
    tokens: list[str] = []
    for chunk in value.replace("|", ",").replace(";", ",").split(","):
        token = normalize_token(chunk)
        if token and token not in tokens:
            tokens.append(token)
    return tokens


def parse_rating(value: str) -> int | None:
    try:
        rating = int(str(value or "").strip())
    except Exception:
        return None
    return rating if rating > 0 else None


def row_is_valid(row: dict[str, str], min_rating: int | None, max_rating: int | None, themes: set[str]) -> bool:
    puzzle_id = str(row.get("PuzzleId", "") or "").strip()
    fen = str(row.get("FEN", "") or "").strip()
    moves = str(row.get("Moves", "") or "").strip()
    rating = parse_rating(row.get("Rating", ""))
    row_themes = parse_theme_tokens(row.get("Themes", ""))
    if not puzzle_id or not fen or not moves or rating is None or not row_themes:
        return False
    if min_rating is not None and rating < min_rating:
        return False
    if max_rating is not None and rating > max_rating:
        return False
    if themes and not set(row_themes).intersection(themes):
        return False
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream a larger Lichess puzzle CSV into a small local sample.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example:\n"
            "  python scripts/build_lichess_puzzle_sample.py --input path/to/lichess_db_puzzle.csv "
            "--limit 1000 --min-rating 800 --max-rating 2000\n"
        ),
    )
    parser.add_argument("--input", required=True, help="Path to the source Lichess CSV.")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Path to write the sample CSV. Default: {DEFAULT_OUTPUT}",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Maximum number of rows to write.")
    parser.add_argument("--min-rating", type=int, default=None, help="Optional minimum rating filter.")
    parser.add_argument("--max-rating", type=int, default=None, help="Optional maximum rating filter.")
    parser.add_argument(
        "--themes",
        default="",
        help="Optional comma-separated theme filter, for example fork,pin,sacrifice.",
    )
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed for stable sampling.")
    return parser


def resolve_path(path_text: str) -> Path:
    return Path(path_text).expanduser().resolve()


def main() -> int:
    args = build_parser().parse_args()
    input_path = resolve_path(args.input)
    output_path = resolve_path(args.output)
    limit = max(int(args.limit or 0), 0)
    theme_filter = {normalize_token(item) for item in str(args.themes or "").split(",") if normalize_token(item)}

    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1
    if limit <= 0:
        print("Limit must be greater than zero.", file=sys.stderr)
        return 1

    rng = random.Random(args.seed)
    rows_scanned = 0
    rows_written = 0
    rows_skipped = 0
    reservoir: list[dict[str, str]] = []
    fieldnames: list[str] = []

    try:
        with input_path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            fieldnames = [str(field or "").strip() for field in (reader.fieldnames or []) if str(field or "").strip()]
            if not fieldnames:
                print("Input CSV is missing a header row.", file=sys.stderr)
                return 1
            for raw_row in reader:
                rows_scanned += 1
                row = {field: raw_row.get(field, "") for field in fieldnames}
                if not row_is_valid(row, args.min_rating, args.max_rating, theme_filter):
                    rows_skipped += 1
                    continue
                rows_written += 1
                if len(reservoir) < limit:
                    reservoir.append(row)
                    continue
                replace_index = rng.randrange(rows_written)
                if replace_index < limit:
                    reservoir[replace_index] = row
    except csv.Error as exc:
        print(f"Malformed CSV: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Failed to read input CSV: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        with temp_path.open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in reservoir:
                writer.writerow({field: row.get(field, "") for field in fieldnames})
        temp_path.replace(output_path)
    except Exception as exc:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
        print(f"Failed to write sample CSV: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    print(f"Rows scanned: {rows_scanned}")
    print(f"Rows written: {len(reservoir)}")
    print(f"Rows skipped: {rows_skipped}")
    if args.min_rating is not None or args.max_rating is not None:
        print(f"Rating filter: {args.min_rating if args.min_rating is not None else 'any'} - {args.max_rating if args.max_rating is not None else 'any'}")
    if theme_filter:
        print(f"Themes filter: {', '.join(sorted(theme_filter))}")
    if args.seed is not None:
        print(f"Seed: {args.seed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
