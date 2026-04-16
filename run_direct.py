#!/usr/bin/env python3
"""
Direct batch runner — skips the subreddit search endpoint entirely.
Uses a hardcoded curated list of known forex/gold trading subreddits,
goes straight to scraping posts + comments. No search API = no rate-limit chokepoint.

Usage:
    python run_direct.py                        # full run
    python run_direct.py --posts 20             # 20 posts per subreddit
    python run_direct.py --dry-run              # print subreddit list only
    python run_direct.py --reset-state          # wipe dedup state
"""

import argparse, os, json, time, random
from datetime import datetime, UTC
from scraper import (
    get_subreddit_posts, get_post_comments, flatten_comments,
    export_markdown, load_state, save_state, reset_state,
    OUTPUT_DIR, _get_json, _jitter
)

# ── Curated subreddit list (no search needed) ─────────────────────────────────
# Ordered roughly by relevance + subscriber count.
# Add/remove freely — this is the only file to edit to change scope.
SUBREDDITS = [
    # ── Core forex ──────────────────────────────────────────────────────────
    {"name": "Forex",             "subscribers": 524000, "category": "forex"},
    {"name": "Forexstrategy",     "subscribers": 127000, "category": "forex"},
    {"name": "FOREXTRADING",      "subscribers":  29000, "category": "forex"},
    {"name": "Forex_Beginner",    "subscribers":  15000, "category": "forex"},
    {"name": "Forex_Academy",     "subscribers":   5000, "category": "forex"},
    {"name": "FXtrade",           "subscribers":   3000, "category": "forex"},
    {"name": "algotrading",       "subscribers": 150000, "category": "forex"},

    # ── Gold / XAUUSD ─────────────────────────────────────────────────────
    {"name": "GoldForexEdge",         "subscribers":  6000, "category": "gold"},
    {"name": "XAUUSD_Gold_TradeRoom", "subscribers":   400, "category": "gold"},
    {"name": "Gold",                  "subscribers":  50000, "category": "gold"},
    {"name": "Commodities",           "subscribers":  30000, "category": "gold"},
    {"name": "PreciousMetals",        "subscribers":  40000, "category": "gold"},

    # ── Prop firms ────────────────────────────────────────────────────────
    {"name": "Proptrading",       "subscribers":  20000, "category": "prop"},
    {"name": "FundedTrader",      "subscribers":  10000, "category": "prop"},
    {"name": "FTMO",              "subscribers":   8000, "category": "prop"},
    {"name": "TopstepTrader",     "subscribers":   3000, "category": "prop"},

    # ── Brokers & CFDs ────────────────────────────────────────────────────
    {"name": "CFDTrading",        "subscribers":   1400, "category": "brokers"},
    {"name": "trading212",        "subscribers": 183000, "category": "brokers"},
    {"name": "eToro",             "subscribers":  20000, "category": "brokers"},
    {"name": "interactivebrokers","subscribers":  40000, "category": "brokers"},

    # ── Platforms ─────────────────────────────────────────────────────────
    {"name": "metatrader",        "subscribers":  16000, "category": "platforms"},
    {"name": "TradingView",       "subscribers": 100000, "category": "platforms"},
    {"name": "cTrader",           "subscribers":   2000, "category": "platforms"},

    # ── General trading ───────────────────────────────────────────────────
    {"name": "Daytrading",        "subscribers": 5000000, "category": "trading"},
    {"name": "Trading",           "subscribers":  400000, "category": "trading"},
    {"name": "Swingtrading",      "subscribers":  100000, "category": "trading"},
    {"name": "investing",         "subscribers": 2000000, "category": "trading"},
    {"name": "technicalanalysis", "subscribers":  100000, "category": "trading"},
    {"name": "options",           "subscribers":  600000, "category": "trading"},
    {"name": "Wallstreetbets",    "subscribers": 15000000,"category": "trading"},
]


def scrape_subreddit_direct(sub_info: dict, posts_per_sub: int, sort: str,
                             seen_posts: set, output_dir: str) -> dict:
    """Scrape one subreddit directly — no search, straight to posts + comments."""
    name = sub_info["name"]
    print(f"\n  r/{name}  ({sub_info['subscribers']:,} subs  [{sub_info['category']}])")
    print(f"  Fetching {posts_per_sub} posts (sort={sort}) …")

    try:
        posts = get_subreddit_posts(name, limit=posts_per_sub, sort=sort)
    except RuntimeError as e:
        print(f"  ERROR fetching posts: {e}")
        return {"name": name, "posts": [], "error": str(e), **sub_info}

    enriched, skipped = [], 0
    for i, post in enumerate(posts, 1):
        if post["id"] in seen_posts:
            skipped += 1
            continue

        preview = post["title"][:55] + ("…" if len(post["title"]) > 55 else "")
        print(f"  [{i:02d}/{len(posts):02d}] {preview}")
        print(f"         score={post['score']}  comments={post['num_comments']}")

        try:
            comments = get_post_comments(name, post["id"])
            flat = flatten_comments(comments)
            post["comments"] = comments
            post["comments_scraped"] = len(flat)
            print(f"         scraped {len(flat)} comment(s)")
        except RuntimeError as e:
            print(f"         ERROR comments: {e}")
            post["comments"] = []
            post["comments_scraped"] = 0

        enriched.append(post)
        seen_posts.add(post["id"])

    if skipped:
        print(f"  (skipped {skipped} already-seen posts)")

    return {**sub_info, "posts": enriched,
            "posts_scraped": len(enriched), "posts_skipped": skipped}


def run_direct(
    subreddits: list[dict] = None,
    posts_per_sub: int = 20,
    sort: str = "new",
    dry_run: bool = False,
):
    if subreddits is None:
        subreddits = SUBREDDITS

    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    total = len(subreddits)

    print(f"\n{'#' * 64}")
    print(f"  DIRECT BATCH RUN  (no search endpoint)")
    print(f"  Subreddits : {total}")
    print(f"  Posts/sub  : {posts_per_sub}  |  Sort: {sort}")
    print(f"  Max posts  : ~{total * posts_per_sub:,} (before dedup)")
    print(f"  Started    : {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'#' * 64}\n")

    if dry_run:
        print("DRY RUN — subreddits that would be scraped:\n")
        for s in subreddits:
            print(f"  [{s['category']:10s}] r/{s['name']:30s}  {s['subscribers']:>10,} subs")
        print(f"\nTotal: {total} subreddits")
        return

    state      = load_state()
    seen_posts = state["scraped_posts"]   # post-level dedup only — subreddits are ALWAYS revisited

    print(f"  State: {len(seen_posts)} posts already seen (will skip these, scrape everything else)\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    totals = {"subs": 0, "posts": 0, "skipped_posts": 0, "comments": 0, "errors": 0}
    output_file = os.path.join(OUTPUT_DIR, f"reddit_direct_{ts}.json")
    all_results = {
        "run_type": "direct",
        "scraped_at": datetime.now(UTC).isoformat(),
        "parameters": {"posts_per_sub": posts_per_sub, "sort": sort},
        "subreddits": [],
    }

    for idx, sub_info in enumerate(subreddits, 1):
        name = sub_info["name"]
        print(f"{'─' * 64}")
        print(f"  [{idx:02d}/{total}] r/{name}")

        # Subreddits are NEVER skipped — always check for new posts.
        # Only individual post IDs are deduplicated via seen_posts.
        result = scrape_subreddit_direct(sub_info, posts_per_sub, sort, seen_posts, OUTPUT_DIR)

        if "error" not in result:
            totals["subs"]          += 1
            totals["posts"]         += result["posts_scraped"]
            totals["skipped_posts"] += result["posts_skipped"]
            totals["comments"]      += sum(p.get("comments_scraped", 0) for p in result["posts"])
        else:
            totals["errors"] += 1

        all_results["subreddits"].append(result)

        # Save state + checkpoint JSON after every subreddit
        save_state(state)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)

        # Random jitter between subreddits (human-like cadence)
        time.sleep(random.uniform(2.0, 4.0))

    # Final JSON + markdown
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    md_file = output_file.replace(".json", ".md")
    all_results["keywords"] = "direct scrape"
    all_results["stats"] = {
        "new_subreddits": totals["subs"],
        "skipped_subreddits": 0,
        "total_posts": totals["posts"],
        "skipped_posts": totals["skipped_posts"],
        "total_comments": totals["comments"],
    }
    all_results["parameters"]["num_subreddits"] = total
    export_markdown(all_results, md_file)

    print(f"\n{'#' * 64}")
    print(f"  DONE")
    print(f"  Subreddits visited : {totals['subs']}")
    print(f"  New posts scraped  : {totals['posts']}  (skipped {totals['skipped_posts']} already seen)")
    print(f"  Comments scraped   : {totals['comments']}")
    print(f"  Errors             : {totals['errors']}")
    print(f"  Total posts in state: {len(seen_posts)} (cumulative across all runs)")
    print(f"  JSON  : {output_file}")
    print(f"  MD    : {md_file}")
    print(f"{'#' * 64}\n")


def main():
    p = argparse.ArgumentParser(description="Direct Reddit scraper — no search endpoint.")
    p.add_argument("--posts",        type=int, default=20)
    p.add_argument("--sort",         choices=["hot","new","top","rising"], default="new")
    p.add_argument("--reset-state",  action="store_true")
    p.add_argument("--dry-run",      action="store_true")
    args = p.parse_args()

    if args.reset_state:
        reset_state()

    run_direct(posts_per_sub=args.posts, sort=args.sort, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
