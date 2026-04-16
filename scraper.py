#!/usr/bin/env python3
"""
Reddit Scraper — with proxy rotation + curl_cffi Chrome TLS impersonation
Searches for subreddits by keywords, scrapes posts and nested comments.
Tracks seen subreddits and posts in scrape_state.json (dedup / incremental).

Usage:
    python scraper.py "forex gold trading" --subreddits 10 --posts 20
    python scraper.py "CFD trading" --subreddits 5 --posts 10 --sort top
    python scraper.py --reset-state          # wipe dedup state
"""

import os, sys, json, time, random, argparse, logging
from datetime import datetime, UTC
from typing import Optional

# curl_cffi impersonates a real Chrome TLS fingerprint — avoids fingerprint blocks
try:
    from curl_cffi.requests import Session as CurlSession
    _SESSION = CurlSession(impersonate="chrome")
    _ENGINE  = "curl_cffi"
except ImportError:
    import requests as _requests_fallback
    _SESSION = _requests_fallback.Session()
    _ENGINE  = "requests"

# ── Configuration ──────────────────────────────────────────────────────────────
STATE_FILE  = "scrape_state.json"
OUTPUT_DIR  = "output"
PROXY_FILE  = "proxies.txt"
LOG_FILE    = "scraper.log"

DELAY_MIN          = 2.0   # random jitter range between every request (seconds)
DELAY_MAX          = 5.0
RETRY_DELAY        = 5     # pause before retrying non-rate-limit errors
RATE_LIMIT_BACKOFF = 60    # base seconds when all proxies exhausted (doubles each attempt)
TIMEOUT            = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE)],
)
log = logging.getLogger(__name__)


# ── Proxy rotation (ported from MQL5 scraper) ─────────────────────────────────

def _load_proxies() -> list[str]:
    """
    Load proxies from proxies.txt (one per line) or PROXIES env var.
    Lines starting with '#' are comments and are skipped.
    Format: http://host:port  or  socks5://user:pass@host:port
    """
    proxies: list[str] = []
    if os.path.exists(PROXY_FILE):
        with open(PROXY_FILE) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    proxies.append(line)
        if proxies:
            log.info("Loaded %d proxies from %s", len(proxies), PROXY_FILE)
            return proxies
    env_val = os.environ.get("PROXIES", "")
    if env_val:
        proxies = [p.strip() for p in env_val.split(",") if p.strip()]
        log.info("Loaded %d proxies from PROXIES env var", len(proxies))
    return proxies


class ProxyRotator:
    """
    Round-robin proxy rotator (single-threaded).
    On 429/403: rotate → retry immediately (no attempt consumed).
    Once all proxies tried without success: exponential backoff.
    """
    def __init__(self, proxies: list[str]):
        self._proxies  = list(proxies)
        self._index    = 0
        self._since_ok = 0   # rotations since last successful request

    def current(self) -> str | None:
        return self._proxies[self._index] if self._proxies else None

    def rotate(self) -> None:
        if not self._proxies:
            return
        self._index    = (self._index + 1) % len(self._proxies)
        self._since_ok += 1

    def exhausted(self) -> bool:
        return bool(self._proxies) and self._since_ok >= len(self._proxies)

    def reset(self) -> None:
        self._since_ok = 0

    def __len__(self):
        return len(self._proxies)


_rotator = ProxyRotator(_load_proxies())

if _rotator.current():
    print(f"  [proxy] {len(_rotator)} proxies loaded — rotation active")
else:
    print(f"  [proxy] No proxies loaded — direct requests (add proxies to proxies.txt to rotate)")

print(f"  [http]  engine = {_ENGINE}\n")


# ── Core HTTP fetch ────────────────────────────────────────────────────────────

def _jitter():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def _get_json(url: str, params: dict = None) -> dict:
    """
    Fetch a Reddit JSON endpoint.
    - Random jitter before every attempt (human-like cadence).
    - 429/403 with proxies:  rotate → retry immediately (attempt not incremented).
    - 429/403 no proxies:    Retry-After header respected, then exponential backoff.
    - Other errors:          1 retry after RETRY_DELAY seconds.
    """
    attempt = 0
    while attempt < 2:
        _jitter()
        proxy_url = _rotator.current()
        kwargs: dict = {"headers": HEADERS, "params": params, "timeout": TIMEOUT}
        if proxy_url:
            kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}

        try:
            resp = _SESSION.get(url, **kwargs)

            if resp.status_code == 200:
                _rotator.reset()
                return resp.json()

            if resp.status_code in (429, 403):
                log.warning("HTTP %s %s", resp.status_code, url)

                if proxy_url and not _rotator.exhausted():
                    _rotator.rotate()
                    print(f"    [{resp.status_code}] rotating proxy → {_rotator.current()}")
                    continue   # retry immediately, don't increment attempt

                # No proxies or all proxies exhausted → backoff
                retry_after = int(resp.headers.get("Retry-After", 0))
                wait = retry_after if retry_after else RATE_LIMIT_BACKOFF * (2 ** attempt)
                label = "all proxies exhausted" if proxy_url else "no proxies"
                print(f"    [{resp.status_code}] rate-limited ({label}) — waiting {wait}s …")
                log.warning("Rate-limited %s — waiting %ds", url, wait)
                time.sleep(wait)

            else:
                log.warning("HTTP %s %s (attempt %d)", resp.status_code, url, attempt + 1)
                if attempt == 0:
                    time.sleep(RETRY_DELAY)

        except Exception as exc:
            log.warning("Request error %s: %s (attempt %d)", url, exc, attempt + 1)
            print(f"    [error] {exc}")
            if attempt == 0:
                time.sleep(RETRY_DELAY)

        attempt += 1

    raise RuntimeError(f"Failed to fetch {url} after 2 attempts")


# ── Deduplication state ────────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            s = json.load(f)
        s["scraped_subreddits"] = set(s.get("scraped_subreddits", []))
        s["scraped_posts"]      = set(s.get("scraped_posts", []))
        return s
    return {"scraped_subreddits": set(), "scraped_posts": set(), "runs": []}


def save_state(state: dict):
    out = {
        **state,
        "scraped_subreddits": sorted(state["scraped_subreddits"]),
        "scraped_posts":      sorted(state["scraped_posts"]),
        "last_updated":       datetime.now(UTC).isoformat(),
    }
    with open(STATE_FILE, "w") as f:
        json.dump(out, f, indent=2)


def reset_state():
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print(f"  State reset — {STATE_FILE} deleted.")
    else:
        print("  No state file found.")


# ── Reddit API ─────────────────────────────────────────────────────────────────

def search_subreddits(keywords: str, limit: int = 10) -> list[dict]:
    data = _get_json(
        "https://www.reddit.com/subreddits/search.json",
        params={"q": keywords, "limit": limit, "sort": "relevance"},
    )
    results = []
    for child in data["data"]["children"]:
        s = child["data"]
        results.append({
            "name":        s["display_name"],
            "title":       s.get("title", ""),
            "description": s.get("public_description", "").strip(),
            "subscribers": s.get("subscribers") or 0,
            "url":         f"https://www.reddit.com{s['url']}",
        })
    return results


def get_subreddit_posts(subreddit: str, limit: int = 20, sort: str = "new") -> list[dict]:
    data = _get_json(
        f"https://www.reddit.com/r/{subreddit}/{sort}.json",
        params={"limit": limit},
    )
    posts = []
    for child in data["data"]["children"]:
        p = child["data"]
        posts.append({
            "id":            p["id"],
            "title":         p.get("title", ""),
            "author":        p.get("author", "[deleted]"),
            "score":         p.get("score", 0),
            "upvote_ratio":  p.get("upvote_ratio", 0.0),
            "num_comments":  p.get("num_comments", 0),
            "selftext":      p.get("selftext", "").strip(),
            "url":           p.get("url", ""),
            "permalink":     "https://www.reddit.com" + p.get("permalink", ""),
            "created_utc":   p.get("created_utc", 0),
            "created_human": datetime.fromtimestamp(p.get("created_utc", 0), tz=UTC)
                             .strftime("%Y-%m-%d %H:%M UTC"),
            "subreddit":     p.get("subreddit", subreddit),
            "flair":         p.get("link_flair_text", ""),
        })
    return posts


def _parse_comment_tree(children: list, depth: int = 0) -> list[dict]:
    comments = []
    for child in children:
        if child.get("kind") != "t1":
            continue
        c = child["data"]
        replies_raw    = c.get("replies")
        reply_children = replies_raw["data"]["children"] if isinstance(replies_raw, dict) else []
        comments.append({
            "id":            c.get("id", ""),
            "author":        c.get("author", "[deleted]"),
            "body":          c.get("body", "").strip(),
            "score":         c.get("score", 0),
            "created_utc":   c.get("created_utc", 0),
            "created_human": datetime.fromtimestamp(c.get("created_utc", 0), tz=UTC)
                             .strftime("%Y-%m-%d %H:%M UTC"),
            "depth":         depth,
            "replies":       _parse_comment_tree(reply_children, depth + 1),
        })
    return comments


def get_post_comments(subreddit: str, post_id: str) -> list[dict]:
    """
    Fetch all comments using the Reddit .json trick.
    Handles edge cases: deleted posts, quarantined subs, or any response
    that isn't the expected [post_listing, comment_listing] array.
    """
    data = _get_json(f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json")
    try:
        if not isinstance(data, list) or len(data) < 2:
            return []
        comment_listing = data[1]
        if not isinstance(comment_listing, dict):
            return []
        children = comment_listing.get("data", {}).get("children", [])
        return _parse_comment_tree(children)
    except (KeyError, IndexError, TypeError):
        return []


def flatten_comments(comments: list[dict], flat: list = None) -> list[dict]:
    if flat is None:
        flat = []
    for c in comments:
        flat.append({k: v for k, v in c.items() if k != "replies"})
        flatten_comments(c.get("replies", []), flat)
    return flat


# ── Markdown export ────────────────────────────────────────────────────────────

def _render_comments_md(comments: list[dict], lines: list[str]):
    headings = {0: "###", 1: "####", 2: "#####"}
    arrows   = {0: "", 1: "&nbsp;" * 4 + "**↳** ", 2: "&nbsp;" * 8 + "**↳↳** "}
    for c in comments:
        d = min(c["depth"], 2)
        if d == 0:
            lines.append(f"\n{headings[d]} u/{c['author']} — score: {c['score']} — {c['created_human']}")
        else:
            lines.append(f"\n{arrows[d]}**u/{c['author']}** — score: {c['score']} — {c['created_human']}")
        for line in c["body"].splitlines():
            lines.append(f"> {line}" if line else ">")
        _render_comments_md(c.get("replies", []), lines)
        if d == 0:
            lines.append("\n---")


def export_markdown(results: dict, md_file: str):
    s = results["stats"]
    lines = [
        "# Reddit Scrape Report", "",
        f"**Keywords:** {results['keywords']}  ",
        f"**Scraped at:** {results['scraped_at']}  ",
        f"**Sort:** {results['parameters']['sort']}  ",
        f"**New subreddits:** {s['new_subreddits']}  (skipped {s['skipped_subreddits']} already seen)  ",
        f"**Posts scraped:** {s['total_posts']}  (skipped {s['skipped_posts']} already seen)  ",
        "", "---", "",
    ]
    for sub in results["subreddits"]:
        lines += [
            f"## r/{sub['name']}", "",
            "| Field | Value |", "|---|---|",
            f"| Subscribers | {sub['subscribers']:,} |",
            f"| Description | {sub.get('description','').replace(chr(10),' ')[:120]} |",
            f"| URL | {sub['url']} |", "",
        ]
        for post in sub.get("posts", []):
            flat      = flatten_comments(post.get("comments", []))
            top_lvl   = sum(1 for c in flat if c["depth"] == 0)
            u_authors = len({c["author"] for c in flat})
            lines += [
                f"### {post['title']}", "",
                "| Field | Value |", "|---|---|",
                f"| Author | u/{post['author']} |",
                f"| Posted | {post['created_human']} |",
                f"| Score | {post['score']} ({int(post.get('upvote_ratio',0)*100)}% upvoted) |",
                f"| Flair | {post.get('flair') or '—'} |",
                f"| Comments reported | {post['num_comments']} |",
                f"| Comments scraped | {post['comments_scraped']} |",
                f"| Permalink | {post['permalink']} |",
                f"| JSON endpoint | {post['permalink'].rstrip('/')}.json |", "",
            ]
            if post.get("selftext"):
                lines += ["**Post body:**", ""]
                for ln in post["selftext"].splitlines():
                    lines.append(f"> {ln}" if ln else ">")
                lines.append("")
            lines += [
                f"**Comment stats:** {post['comments_scraped']} total · "
                f"{top_lvl} top-level · {u_authors} unique authors", "",
            ]
            if post.get("comments"):
                _render_comments_md(post["comments"], lines)
            else:
                lines.append("_No comments scraped._")
            lines += ["", "---", ""]

    with open(md_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  Markdown : {md_file}")


# ── Main scraper ───────────────────────────────────────────────────────────────

def scrape(
    keywords: str,
    num_subreddits: int = 10,
    posts_per_subreddit: int = 20,
    sort: str = "new",
    output_file: Optional[str] = None,
    markdown: bool = True,
    state: Optional[dict] = None,
) -> dict:
    sep = "=" * 64
    print(f"\n{sep}")
    print(f"  Keywords  : {keywords}")
    print(f"  Subreddits: {num_subreddits}  |  Posts/sub: {posts_per_subreddit}  |  Sort: {sort}")
    print(f"{sep}\n")

    own_state = state is None
    if own_state:
        state = load_state()

    seen_subs  = state["scraped_subreddits"]
    seen_posts = state["scraped_posts"]

    results = {
        "keywords":   keywords,
        "parameters": {"num_subreddits": num_subreddits,
                       "posts_per_subreddit": posts_per_subreddit, "sort": sort},
        "scraped_at": datetime.now(UTC).isoformat(),
        "stats": {"new_subreddits": 0, "skipped_subreddits": 0,
                  "skipped_posts": 0, "total_posts": 0, "total_comments": 0},
        "subreddits": [],
    }

    # ── 1. Find subreddits ────────────────────────────────────────────────────
    print(f"[Step 1] Searching subreddits …")
    try:
        subreddits = search_subreddits(keywords, limit=num_subreddits)
    except RuntimeError as e:
        print(f"  ERROR: {e}")
        return results

    print(f"  Found {len(subreddits)} subreddit(s)")
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # ── 2 & 3. Posts + comments per subreddit ─────────────────────────────────
    for sub_idx, sub_info in enumerate(subreddits, 1):
        sub_name = sub_info["name"]

        if sub_name in seen_subs:
            print(f"  [skip] r/{sub_name} — already scraped")
            results["stats"]["skipped_subreddits"] += 1
            continue

        print(f"\n[Step 2] r/{sub_name} ({sub_idx}/{len(subreddits)})  {sub_info['subscribers']:,} subs")
        print(f"         Fetching {posts_per_subreddit} posts (sort={sort}) …")

        try:
            posts = get_subreddit_posts(sub_name, limit=posts_per_subreddit, sort=sort)
        except RuntimeError as e:
            print(f"         ERROR: {e}")
            results["subreddits"].append({**sub_info, "posts": [], "error": str(e)})
            continue

        enriched_posts, skipped = [], 0
        for p_idx, post in enumerate(posts, 1):
            if post["id"] in seen_posts:
                skipped += 1
                results["stats"]["skipped_posts"] += 1
                continue

            preview = post["title"][:55] + ("…" if len(post["title"]) > 55 else "")
            print(f"         [{p_idx:02d}/{len(posts):02d}] {preview}")
            print(f"                  score={post['score']}  comments={post['num_comments']}")

            try:
                comments = get_post_comments(sub_name, post["id"])
                flat     = flatten_comments(comments)
                post["comments"]         = comments
                post["comments_scraped"] = len(flat)
                print(f"                  scraped {len(flat)} comment(s)")
            except RuntimeError as e:
                print(f"                  ERROR: {e}")
                post["comments"]         = []
                post["comments_scraped"] = 0

            enriched_posts.append(post)
            seen_posts.add(post["id"])
            results["stats"]["total_comments"] += post["comments_scraped"]

        if skipped:
            print(f"         (skipped {skipped} already-seen posts)")

        results["stats"]["total_posts"]    += len(enriched_posts)
        results["stats"]["new_subreddits"] += 1
        results["subreddits"].append({**sub_info, "posts": enriched_posts})

        seen_subs.add(sub_name)
        if own_state:
            save_state(state)

    # ── 4. Save ───────────────────────────────────────────────────────────────
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if output_file is None:
        ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_kw = keywords.replace(" ", "_")[:30]
        output_file = os.path.join(OUTPUT_DIR, f"reddit_{safe_kw}_{ts}.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n  JSON     : {output_file}")

    if markdown:
        export_markdown(results, output_file.rsplit(".", 1)[0] + ".md")

    st = results["stats"]
    print(sep)
    print(f"  New subs : {st['new_subreddits']}  (skipped {st['skipped_subreddits']})")
    print(f"  Posts    : {st['total_posts']}  (skipped {st['skipped_posts']})")
    print(f"  Comments : {st['total_comments']}")
    print(f"{sep}\n")

    if own_state:
        save_state(state)

    return results


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Reddit scraper with proxy rotation.")
    p.add_argument("keywords", type=str)
    p.add_argument("--subreddits", type=int, default=10)
    p.add_argument("--posts",      type=int, default=20)
    p.add_argument("--sort",       choices=["hot","new","top","rising"], default="new")
    p.add_argument("--output",     type=str, default=None)
    p.add_argument("--no-markdown",  action="store_true")
    p.add_argument("--reset-state",  action="store_true")
    args = p.parse_args()

    if args.reset_state:
        reset_state()
        return

    scrape(keywords=args.keywords, num_subreddits=args.subreddits,
           posts_per_subreddit=args.posts, sort=args.sort,
           output_file=args.output, markdown=not args.no_markdown)


if __name__ == "__main__":
    main()
