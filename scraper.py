#!/usr/bin/env python3
"""
Reddit Scraper
Searches for subreddits by keywords, then scrapes posts and all comments.

Usage:
    python scraper.py "forex gold trading" --subreddits 3 --posts 5 --sort hot
    python scraper.py "gold trading" --subreddits 2 --posts 10 --sort top --output results.json
"""

import requests
import json
import time
import argparse
import sys
from datetime import datetime, timezone, UTC
from typing import Optional


HEADERS = {
    "User-Agent": "RedditScraper/1.0 (educational research tool)"
}

REQUEST_DELAY = 1.5  # seconds between requests to respect rate limits


# ─── Reddit API helpers ───────────────────────────────────────────────────────

def _get_json(url: str, params: dict = None, retries: int = 3) -> dict:
    """
    Fetch a Reddit JSON endpoint with retry logic.
    Reddit returns JSON natively when you append .json to any URL,
    or hit the search/listing endpoints directly.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"    [rate-limited] waiting {wait}s …")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            print(f"    [attempt {attempt}/{retries}] {exc}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts")


# ─── Subreddit search ─────────────────────────────────────────────────────────

def search_subreddits(keywords: str, limit: int = 5) -> list[dict]:
    """
    Use Reddit's subreddit search endpoint to find communities
    matching the given keywords, sorted by relevance.
    """
    url = "https://www.reddit.com/subreddits/search.json"
    data = _get_json(url, params={"q": keywords, "limit": limit, "sort": "relevance"})

    results = []
    for child in data["data"]["children"]:
        sub = child["data"]
        results.append({
            "name": sub["display_name"],
            "title": sub.get("title", ""),
            "description": sub.get("public_description", "").strip(),
            "subscribers": sub.get("subscribers") or 0,
            "url": f"https://www.reddit.com{sub['url']}",
        })
    return results


# ─── Post listing ─────────────────────────────────────────────────────────────

def get_subreddit_posts(subreddit: str, limit: int = 10, sort: str = "hot") -> list[dict]:
    """
    Fetch the top-level post listing for a subreddit.
    sort: hot | new | top | rising
    """
    url = f"https://www.reddit.com/r/{subreddit}/{sort}.json"
    data = _get_json(url, params={"limit": limit})

    posts = []
    for child in data["data"]["children"]:
        p = child["data"]
        posts.append({
            "id": p["id"],
            "title": p.get("title", ""),
            "author": p.get("author", "[deleted]"),
            "score": p.get("score", 0),
            "upvote_ratio": p.get("upvote_ratio", 0.0),
            "num_comments": p.get("num_comments", 0),
            "selftext": p.get("selftext", "").strip(),
            "url": p.get("url", ""),
            "permalink": "https://www.reddit.com" + p.get("permalink", ""),
            "created_utc": p.get("created_utc", 0),
            "created_human": datetime.fromtimestamp(
                p.get("created_utc", 0), tz=UTC
            ).strftime("%Y-%m-%d %H:%M UTC"),
            "subreddit": p.get("subreddit", subreddit),
            "flair": p.get("link_flair_text", ""),
        })
    return posts


# ─── Comment scraping ─────────────────────────────────────────────────────────

def _parse_comment_tree(children: list, depth: int = 0) -> list[dict]:
    """
    Recursively walk the Reddit comment tree.
    Each node is either a 't1' (comment) or 'more' (load-more stub, skipped).
    """
    comments = []
    for child in children:
        kind = child.get("kind")
        if kind == "t1":
            c = child["data"]
            replies_raw = c.get("replies")
            reply_children = (
                replies_raw["data"]["children"]
                if isinstance(replies_raw, dict)
                else []
            )
            comments.append({
                "id": c.get("id", ""),
                "author": c.get("author", "[deleted]"),
                "body": c.get("body", "").strip(),
                "score": c.get("score", 0),
                "created_utc": c.get("created_utc", 0),
                "created_human": datetime.fromtimestamp(
                    c.get("created_utc", 0), tz=UTC
                ).strftime("%Y-%m-%d %H:%M UTC"),
                "depth": depth,
                "replies": _parse_comment_tree(reply_children, depth + 1),
            })
        # kind == "more" → load-more stub, ignored
    return comments


def get_post_comments(subreddit: str, post_id: str) -> list[dict]:
    """
    Fetch all comments for a post using the Reddit .json trick:
      https://www.reddit.com/r/<sub>/comments/<id>.json
    Returns a list of top-level comments, each with nested 'replies'.
    """
    url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"
    data = _get_json(url)
    # data[0] = post listing, data[1] = comment listing
    if len(data) < 2:
        return []
    return _parse_comment_tree(data[1]["data"]["children"])


def flatten_comments(comments: list[dict], flat: list = None) -> list[dict]:
    """
    Flatten nested comment tree into a single list (preserves depth field).
    Useful for CSV export or quick counting.
    """
    if flat is None:
        flat = []
    for c in comments:
        flat.append({k: v for k, v in c.items() if k != "replies"})
        flatten_comments(c.get("replies", []), flat)
    return flat


# ─── Markdown export ──────────────────────────────────────────────────────────

def _render_comments_md(comments: list[dict], lines: list[str]):
    """Recursively render the comment tree as indented markdown."""
    indent_map = {0: "###", 1: "####", 2: "#####"}
    arrow_map  = {0: "", 1: "&nbsp;&nbsp;&nbsp;&nbsp;**↳** ", 2: "&nbsp;" * 8 + "**↳↳** "}

    for c in comments:
        d = min(c["depth"], 2)
        prefix = arrow_map.get(d, "&nbsp;" * (d * 4) + "**↳** ")
        heading = indent_map.get(d, "######")

        if d == 0:
            lines.append(f"\n{heading} u/{c['author']} — score: {c['score']} — {c['created_human']}")
        else:
            lines.append(f"\n{prefix}**u/{c['author']}** — score: {c['score']} — {c['created_human']}")

        for line in c["body"].splitlines():
            lines.append(f"> {line}" if line else ">")

        _render_comments_md(c.get("replies", []), lines)
        if d == 0:
            lines.append("\n---")


def export_markdown(results: dict, md_file: str):
    """
    Write a human-readable markdown report of the full scrape results.
    One section per subreddit, one subsection per post, full comment tree.
    """
    lines = [
        f"# Reddit Scrape Report",
        f"",
        f"**Keywords:** {results['keywords']}  ",
        f"**Scraped at:** {results['scraped_at']}  ",
        f"**Sort:** {results['parameters']['sort']}  ",
        f"**Subreddits:** {results['parameters']['num_subreddits']}  ",
        f"**Posts per subreddit:** {results['parameters']['posts_per_subreddit']}  ",
        f"",
        f"---",
        f"",
    ]

    for sub in results["subreddits"]:
        lines += [
            f"## r/{sub['name']}",
            f"",
            f"| Field | Value |",
            f"|---|---|",
            f"| Subscribers | {sub['subscribers']:,} |",
            f"| Description | {sub.get('description','').replace(chr(10),' ')[:120]} |",
            f"| URL | {sub['url']} |",
            f"",
        ]

        for post in sub.get("posts", []):
            flat = flatten_comments(post.get("comments", []))
            top_level = sum(1 for c in flat if c["depth"] == 0)
            unique_authors = len({c["author"] for c in flat})

            lines += [
                f"### {post['title']}",
                f"",
                f"| Field | Value |",
                f"|---|---|",
                f"| Author | u/{post['author']} |",
                f"| Posted | {post['created_human']} |",
                f"| Score | {post['score']} ({int(post.get('upvote_ratio',0)*100)}% upvoted) |",
                f"| Flair | {post.get('flair','—')} |",
                f"| Comments reported | {post['num_comments']} |",
                f"| Comments scraped | {post['comments_scraped']} |",
                f"| Permalink | {post['permalink']} |",
                f"| JSON endpoint | {post['permalink'].rstrip('/')}.json |",
                f"",
            ]

            if post.get("selftext"):
                lines += [
                    f"**Post body:**",
                    f"",
                ]
                for body_line in post["selftext"].splitlines():
                    lines.append(f"> {body_line}" if body_line else ">")
                lines.append("")

            lines += [
                f"**Comment stats:** {post['comments_scraped']} total · "
                f"{top_level} top-level · {unique_authors} unique authors",
                f"",
            ]

            if post.get("comments"):
                _render_comments_md(post["comments"], lines)
            else:
                lines.append("_No comments scraped._")

            lines += ["", "---", ""]

    with open(md_file, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"  Markdown : {md_file}")


# ─── Main scraper orchestration ───────────────────────────────────────────────

def scrape(
    keywords: str,
    num_subreddits: int = 3,
    posts_per_subreddit: int = 5,
    sort: str = "hot",
    output_file: Optional[str] = None,
    markdown: bool = True,
) -> dict:
    """
    Full pipeline:
      1. Search for subreddits matching `keywords`
      2. For each subreddit, fetch `posts_per_subreddit` posts
      3. For each post, fetch all comments (nested)
      4. Persist results to JSON

    Parameters
    ----------
    keywords            : search query (e.g. "forex gold trading")
    num_subreddits      : how many subreddits to include
    posts_per_subreddit : how many posts to scrape per subreddit (max 100)
    sort                : post ordering — hot | new | top | rising
    output_file         : path for JSON output (auto-named if None)
    """
    sep = "=" * 64
    print(f"\n{sep}")
    print(f"  Reddit Scraper")
    print(f"  Keywords  : {keywords}")
    print(f"  Subreddits: {num_subreddits}  |  Posts/sub: {posts_per_subreddit}  |  Sort: {sort}")
    print(f"{sep}\n")

    results = {
        "keywords": keywords,
        "parameters": {
            "num_subreddits": num_subreddits,
            "posts_per_subreddit": posts_per_subreddit,
            "sort": sort,
        },
        "scraped_at": datetime.now(UTC).isoformat(),
        "subreddits": [],
    }

    # ── 1. Find subreddits ────────────────────────────────────────────────────
    print(f"[Step 1] Searching subreddits for: \"{keywords}\" …")
    try:
        subreddits = search_subreddits(keywords, limit=num_subreddits)
    except RuntimeError as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    if not subreddits:
        print("  No subreddits found. Try different keywords.")
        sys.exit(0)

    print(f"  Found {len(subreddits)} subreddit(s):\n")
    for s in subreddits:
        print(f"    r/{s['name']:30s}  {s['subscribers']:>10,} subscribers")
    print()

    time.sleep(REQUEST_DELAY)

    # ── 2 & 3. Posts + comments per subreddit ─────────────────────────────────
    for sub_idx, sub_info in enumerate(subreddits, 1):
        sub_name = sub_info["name"]
        print(f"[Step 2] r/{sub_name} ({sub_idx}/{len(subreddits)})")
        print(f"         Fetching {posts_per_subreddit} posts (sort={sort}) …")

        try:
            posts = get_subreddit_posts(sub_name, limit=posts_per_subreddit, sort=sort)
        except RuntimeError as e:
            print(f"         ERROR fetching posts: {e}")
            results["subreddits"].append({**sub_info, "posts": [], "error": str(e)})
            continue

        time.sleep(REQUEST_DELAY)

        print(f"         Got {len(posts)} posts. Now fetching comments …\n")

        enriched_posts = []
        for p_idx, post in enumerate(posts, 1):
            title_preview = post["title"][:55] + ("…" if len(post["title"]) > 55 else "")
            print(f"         [{p_idx:02d}/{len(posts):02d}] {title_preview}")
            print(f"                  score={post['score']}  comments={post['num_comments']}")

            try:
                comments = get_post_comments(sub_name, post["id"])
                flat = flatten_comments(comments)
                post["comments"] = comments
                post["comments_scraped"] = len(flat)
                print(f"                  scraped {len(flat)} comment(s)")
            except RuntimeError as e:
                print(f"                  ERROR: {e}")
                post["comments"] = []
                post["comments_scraped"] = 0

            enriched_posts.append(post)
            time.sleep(REQUEST_DELAY)

        results["subreddits"].append({**sub_info, "posts": enriched_posts})
        print()

    # ── 4. Save results ───────────────────────────────────────────────────────
    if output_file is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_kw = keywords.replace(" ", "_")[:30]
        output_file = f"reddit_{safe_kw}_{ts}.json"

    with open(output_file, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, ensure_ascii=False)

    if markdown:
        md_file = output_file.rsplit(".", 1)[0] + ".md"
        export_markdown(results, md_file)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_posts = sum(len(s.get("posts", [])) for s in results["subreddits"])
    total_comments = sum(
        sum(p.get("comments_scraped", 0) for p in s.get("posts", []))
        for s in results["subreddits"]
    )

    print(sep)
    print(f"  Done!")
    print(f"  Subreddits : {len(results['subreddits'])}")
    print(f"  Posts      : {total_posts}")
    print(f"  Comments   : {total_comments}")
    print(f"  Output     : {output_file}")
    print(sep + "\n")

    return results


# ─── CLI entry point ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scrape Reddit subreddits, posts, and comments by keyword.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py "forex gold trading"
  python scraper.py "forex gold" --subreddits 5 --posts 10 --sort top
  python scraper.py "gold xauusd" --subreddits 2 --posts 3 --output gold.json
        """,
    )
    parser.add_argument(
        "keywords",
        type=str,
        help='Keywords to search for relevant subreddits (e.g. "forex gold trading")',
    )
    parser.add_argument(
        "--subreddits",
        type=int,
        default=3,
        metavar="N",
        help="Number of subreddits to scrape (default: 3)",
    )
    parser.add_argument(
        "--posts",
        type=int,
        default=5,
        metavar="N",
        help="Number of posts to scrape per subreddit (default: 5, max: 100)",
    )
    parser.add_argument(
        "--sort",
        choices=["hot", "new", "top", "rising"],
        default="hot",
        help="Post sort order (default: hot)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        metavar="FILE",
        help="Output JSON file path (auto-named if omitted)",
    )
    parser.add_argument(
        "--no-markdown",
        action="store_true",
        help="Skip writing the .md report (JSON only)",
    )

    args = parser.parse_args()

    scrape(
        keywords=args.keywords,
        num_subreddits=args.subreddits,
        posts_per_subreddit=args.posts,
        sort=args.sort,
        output_file=args.output,
        markdown=not args.no_markdown,
    )


if __name__ == "__main__":
    main()
