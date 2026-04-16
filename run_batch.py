#!/usr/bin/env python3
"""
Batch runner — scrapes across a large keyword list in one go.
Shares a single dedup state so no subreddit or post is ever scraped twice.

Usage:
    python run_batch.py                        # run full keyword list
    python run_batch.py --posts 20 --subreddits 10
    python run_batch.py --reset-state          # wipe state, fresh start
    python run_batch.py --dry-run              # print keywords only, no scraping
"""

import argparse, os, json
from datetime import datetime, UTC
from scraper import scrape, load_state, save_state, reset_state, OUTPUT_DIR

# ── Keyword list (~50 terms → targets 500-1000 posts with dedup) ───────────────
KEYWORDS = [
    # ── Core forex ──────────────────────────────────────────────────────────────
    "forex",
    "forex trading",
    "forex market",
    "forex signals",
    "forex broker",
    "forex strategy",
    "forex price action",
    "forex scalping",
    "forex swing trading",
    "forex day trading",

    # ── Gold / XAUUSD ────────────────────────────────────────────────────────────
    "gold trading",
    "XAUUSD",
    "gold forex",
    "gold price analysis",
    "gold signals",
    "xauusd strategy",
    "gold scalping",

    # ── Currency pairs ────────────────────────────────────────────────────────────
    "EURUSD trading",
    "GBPUSD trading",
    "USDJPY trading",
    "GBPJPY trading",
    "dollar index DXY",

    # ── Trading concepts ──────────────────────────────────────────────────────────
    "ICT trading",
    "smart money concept SMC",
    "supply demand forex",
    "order flow trading",
    "market structure trading",
    "liquidity grab forex",
    "fair value gap FVG",
    "breaker block trading",

    # ── Risk & psychology ─────────────────────────────────────────────────────────
    "forex risk management",
    "trading psychology",
    "weekend gap risk forex",
    "overnight risk trading",
    "drawdown management trading",
    "position sizing forex",

    # ── Platforms & tools ─────────────────────────────────────────────────────────
    "MetaTrader 4 MT4",
    "MetaTrader 5 MT5",
    "cTrader forex",
    "TradingView forex",
    "forex EA expert advisor",

    # ── Brokers & execution ───────────────────────────────────────────────────────
    "forex broker review",
    "ECN broker forex",
    "broker spread slippage",
    "broker withdrawal problems",
    "CFD trading",
    "CFD broker",

    # ── Prop firms ────────────────────────────────────────────────────────────────
    "prop firm trading",
    "FTMO challenge",
    "funded trader",
    "prop firm payout",
    "funded account forex",
    "prop firm rules",
    "metatrader trading",
]


def run_batch(
    keywords: list[str],
    num_subreddits: int = 10,
    posts_per_subreddit: int = 20,
    sort: str = "new",
    dry_run: bool = False,
):
    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    total = len(keywords)

    print(f"\n{'#' * 64}")
    print(f"  BATCH RUN — {total} keywords")
    print(f"  Subreddits/keyword : {num_subreddits}")
    print(f"  Posts/subreddit    : {posts_per_subreddit}")
    print(f"  Sort               : {sort}")
    print(f"  Estimated max posts: ~{total * num_subreddits * posts_per_subreddit:,} (before dedup)")
    print(f"  Started            : {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'#' * 64}\n")

    if dry_run:
        print("DRY RUN — keywords that would be run:\n")
        for i, kw in enumerate(keywords, 1):
            print(f"  {i:02d}. {kw}")
        print(f"\nTotal: {total} keywords")
        return

    state = load_state()
    print(f"  State loaded: {len(state['scraped_subreddits'])} subs, "
          f"{len(state['scraped_posts'])} posts already seen\n")

    batch = {
        "keywords_run": 0,
        "new_subreddits": 0, "skipped_subreddits": 0,
        "total_posts": 0,    "skipped_posts": 0,
        "total_comments": 0,
        "output_files": [],
        "errors": [],
    }

    for idx, kw in enumerate(keywords, 1):
        print(f"\n{'─' * 64}")
        print(f"  [{idx:02d}/{total}] \"{kw}\"")
        print(f"{'─' * 64}")

        safe_kw     = kw.replace(" ", "_")[:30]
        output_file = os.path.join(OUTPUT_DIR, f"reddit_{safe_kw}_{ts}.json")

        try:
            results = scrape(
                keywords=kw,
                num_subreddits=num_subreddits,
                posts_per_subreddit=posts_per_subreddit,
                sort=sort,
                output_file=output_file,
                markdown=True,
                state=state,
            )
        except Exception as e:
            print(f"  [ERROR] keyword \"{kw}\": {e}")
            batch["errors"].append({"keyword": kw, "error": str(e)})
            save_state(state)
            continue

        s = results.get("stats", {})
        batch["keywords_run"]       += 1
        batch["new_subreddits"]     += s.get("new_subreddits", 0)
        batch["skipped_subreddits"] += s.get("skipped_subreddits", 0)
        batch["total_posts"]        += s.get("total_posts", 0)
        batch["skipped_posts"]      += s.get("skipped_posts", 0)
        batch["total_comments"]     += s.get("total_comments", 0)
        batch["output_files"].append(output_file)

        save_state(state)

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{'#' * 64}")
    print(f"  BATCH COMPLETE")
    print(f"  Keywords processed   : {batch['keywords_run']} / {total}")
    print(f"  Unique subreddits    : {batch['new_subreddits']}  "
          f"(skipped {batch['skipped_subreddits']} already seen)")
    print(f"  Posts scraped        : {batch['total_posts']}  "
          f"(skipped {batch['skipped_posts']} already seen)")
    print(f"  Comments scraped     : {batch['total_comments']}")
    print(f"  Errors               : {len(batch['errors'])}")
    print(f"  State totals         : {len(state['scraped_subreddits'])} subs, "
          f"{len(state['scraped_posts'])} posts")
    print(f"\n  Output files in ./{OUTPUT_DIR}/")
    for f in batch["output_files"]:
        base = os.path.basename(f)
        print(f"    {base}")
        print(f"    {base.replace('.json','.md')}")
    if batch["errors"]:
        print(f"\n  Failed keywords:")
        for e in batch["errors"]:
            print(f"    ✗ {e['keyword']}: {e['error']}")
    print(f"{'#' * 64}\n")

    return batch


def main():
    p = argparse.ArgumentParser(description="Batch Reddit scraper — large keyword set.")
    p.add_argument("--subreddits",   type=int, default=10)
    p.add_argument("--posts",        type=int, default=20)
    p.add_argument("--sort",         choices=["hot","new","top","rising"], default="new")
    p.add_argument("--reset-state",  action="store_true")
    p.add_argument("--dry-run",      action="store_true",
                   help="Print keywords list without scraping")
    args = p.parse_args()

    if args.reset_state:
        reset_state()

    run_batch(
        keywords=KEYWORDS,
        num_subreddits=args.subreddits,
        posts_per_subreddit=args.posts,
        sort=args.sort,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
