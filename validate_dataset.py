"""
validate_dataset.py
===================
Inspect and validate the scraped refactoring dataset.
Run after scrape_refactor_dataset.py to check quality.

Usage:
    python validate_dataset.py dataset.jsonl
    python validate_dataset.py dataset.jsonl --sample 5
    python validate_dataset.py dataset.jsonl --dedup --output dataset_clean.jsonl
"""

import ast
import json
import sys
import argparse
import hashlib
from collections import Counter
from pathlib import Path


def load_jsonl(path: str):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def is_valid_python(src: str) -> bool:
    try:
        ast.parse(src)
        return True
    except SyntaxError:
        return False


def pair_hash(pair: dict) -> str:
    """Deduplicate by hashing the (before, after) content."""
    key = pair["before"] + "|||" + pair["after"]
    return hashlib.md5(key.encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Input JSONL file")
    parser.add_argument("--sample", type=int, default=0,
                        help="Print N random samples")
    parser.add_argument("--dedup", action="store_true",
                        help="Remove duplicate pairs")
    parser.add_argument("--output", default=None,
                        help="Write cleaned output to this file")
    args = parser.parse_args()

    pairs = list(load_jsonl(args.input))
    total = len(pairs)
    print(f"\n{'='*50}")
    print(f"Dataset: {args.input}")
    print(f"Total pairs: {total}")
    print(f"{'='*50}\n")

    # --- Refactor type distribution ---
    type_counts = Counter(p["refactor_type"] for p in pairs)
    print("Refactor type distribution:")
    for k, v in type_counts.most_common():
        bar = "█" * (v * 30 // total)
        print(f"  {k:15s} {v:5d}  {bar}")

    # --- Diff size distribution ---
    sizes = [p["diff_lines"] for p in pairs]
    print(f"\nDiff size (lines changed):")
    print(f"  min={min(sizes)}  max={max(sizes)}  "
          f"avg={sum(sizes)/len(sizes):.1f}  median={sorted(sizes)[len(sizes)//2]}")

    # --- Syntax validity check ---
    invalid_before = sum(1 for p in pairs if not is_valid_python(p["before"]))
    invalid_after  = sum(1 for p in pairs if not is_valid_python(p["after"]))
    print(f"\nSyntax validity:")
    print(f"  invalid 'before': {invalid_before}")
    print(f"  invalid 'after':  {invalid_after}")

    # --- Repo diversity ---
    repos = Counter(p["repo"] for p in pairs)
    print(f"\nRepo diversity:")
    print(f"  unique repos: {len(repos)}")
    print(f"  top 5 by pair count:")
    for repo, count in repos.most_common(5):
        print(f"    {repo}: {count}")

    # --- Deduplication ---
    if args.dedup:
        seen = set()
        clean = []
        for p in pairs:
            h = pair_hash(p)
            if h not in seen:
                seen.add(h)
                clean.append(p)
        removed = total - len(clean)
        print(f"\nDeduplication: removed {removed} duplicate pairs "
              f"({len(clean)} remain)")
        pairs = clean

    # --- Sample output ---
    if args.sample > 0:
        import random
        samples = random.sample(pairs, min(args.sample, len(pairs)))
        print(f"\n{'='*50}")
        print(f"SAMPLE PAIRS (n={len(samples)})")
        for i, p in enumerate(samples, 1):
            print(f"\n--- Sample {i} [{p['refactor_type']}] "
                  f"from {p['repo']} ---")
            print(f"  diff_lines: {p['diff_lines']}")
            print("  BEFORE:")
            for line in p["before"].splitlines()[:8]:
                print(f"    {line}")
            if len(p["before"].splitlines()) > 8:
                print("    ...")
            print("  AFTER:")
            for line in p["after"].splitlines()[:8]:
                print(f"    {line}")
            if len(p["after"].splitlines()) > 8:
                print("    ...")

    # --- Write output ---
    if args.output:
        out = Path(args.output)
        with open(out, "w", encoding="utf-8") as f:
            for p in pairs:
                # Only keep syntactically valid pairs
                if is_valid_python(p["before"]) and is_valid_python(p["after"]):
                    f.write(json.dumps(p, ensure_ascii=False) + "\n")
        kept = sum(1 for _ in load_jsonl(args.output))
        print(f"\nWrote {kept} clean pairs to {out}")


if __name__ == "__main__":
    main()
