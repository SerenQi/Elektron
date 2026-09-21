#!/usr/bin/env python3
"""
Backfill embeddings for existing buckets.
为存量桶批量生成 embedding。

Usage:
    OMBRE_BUCKETS_DIR=/data OMBRE_API_KEY=xxx python backfill_embeddings.py [--batch-size 20] [--dry-run]

Each batch calls Gemini embedding API once per bucket.
Free tier: 1500 requests/day, so ~75 batches of 20.
"""

import asyncio
import argparse
import sys

sys.path.insert(0, ".")
from utils import load_config
from bucket_manager import BucketManager
from embedding_engine import EmbeddingEngine


async def backfill(batch_size: int = 20, dry_run: bool = False):
    config = load_config()
    bucket_mgr = BucketManager(config)
    engine = EmbeddingEngine(config)

    if not engine.enabled:
        print("ERROR: Embedding engine not enabled (missing API key?)")
        return

    all_buckets = await bucket_mgr.list_all(include_archive=True)
    print(f"Total buckets: {len(all_buckets)}")

    # Find buckets without embeddings
    missing = []
    for b in all_buckets:
        emb = await engine.get_embedding(b["id"])
        if emb is None:
            missing.append(b)

    print(f"Missing embeddings: {len(missing)}")

    if dry_run:
        for b in missing[:10]:
            print(f"  would embed: {b['id']} ({b['metadata'].get('name', '?')})")
        if len(missing) > 10:
            print(f"  ... and {len(missing) - 10} more")
        return

    total = len(missing)
    success = 0
    failed = 0

    for i in range(0, total, batch_size):
        batch = missing[i : i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size
        print(f"\n--- Batch {batch_num}/{total_batches} ({len(batch)} buckets) ---")

        for b in batch:
            name = b["metadata"].get("name", b["id"])
            content = b.get("content", "")
            if not content or not content.strip():
                print(f"  SKIP (empty): {b['id']} ({name})")
                continue

            try:
                ok = await engine.generate_and_store(b["id"], content)
                if ok:
                    success += 1
                    print(f"  OK: {b['id'][:12]} ({name[:30]})")
                else:
                    failed += 1
                    print(f"  FAIL: {b['id'][:12]} ({name[:30]})")
            except Exception as e:
                failed += 1
                print(f"  ERROR: {b['id'][:12]} ({name[:30]}): {e}")

        if i + batch_size < total:
            print("  Waiting 2s before next batch...")
            await asyncio.sleep(2)

    print(f"\n=== Done: {success} success, {failed} failed, {total - success - failed} skipped ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(backfill(batch_size=args.batch_size, dry_run=args.dry_run))
