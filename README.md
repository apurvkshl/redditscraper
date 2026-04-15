# Reddit Scraper

Scrapes Reddit subreddits, posts, and comments by keyword. Built for trading community research (forex, gold, XAUUSD), but works for any topic.

## How it works

1. Searches Reddit for subreddits matching your keywords
2. Fetches the top N posts from each subreddit
3. Fetches all comments per post using Reddit's `.json` trick — appending `.json` to any Reddit URL returns the full post + nested comment tree with no API key needed
4. Saves results as both **JSON** (structured data) and **Markdown** (human-readable report)

## Usage

```bash
pip install -r requirements.txt

# Basic
python scraper.py "forex gold trading"

# Full control
python scraper.py "forex gold" --subreddits 5 --posts 10 --sort top

# Specific output file
python scraper.py "xauusd gold" --subreddits 2 --posts 20 --sort new --output gold.json

# JSON only, skip markdown
python scraper.py "forex" --no-markdown
```

## Parameters

| Flag | Default | Description |
|---|---|---|
| `keywords` | required | Search query to find subreddits |
| `--subreddits` | 3 | Number of subreddits to scrape |
| `--posts` | 5 | Posts per subreddit (max 100) |
| `--sort` | `hot` | `hot` / `new` / `top` / `rising` |
| `--output` | auto-named | Output JSON file path |
| `--no-markdown` | off | Skip the `.md` report |

## Output

Each run produces two files:

```
reddit_forex_gold_trading_20260415_203700.json   ← structured data
reddit_forex_gold_trading_20260415_203700.md     ← readable report
```

### JSON structure

```
{
  "keywords": "...",
  "parameters": { ... },
  "scraped_at": "...",
  "subreddits": [
    {
      "name": "Forex",
      "subscribers": 524662,
      "posts": [
        {
          "title": "...",
          "author": "...",
          "score": 42,
          "selftext": "...",
          "permalink": "https://reddit.com/...",
          "comments_scraped": 34,
          "comments": [
            {
              "author": "...",
              "body": "...",
              "score": 5,
              "depth": 0,
              "replies": [ ... ]
            }
          ]
        }
      ]
    }
  ]
}
```

### Markdown structure

The `.md` file contains a full human-readable report with:
- Subreddit metadata table
- Per-post metadata table (score, flair, timestamp, permalink, JSON endpoint URL)
- Full nested comment tree with depth indicators (↳, ↳↳)

## The `.json` trick

Any Reddit thread URL becomes machine-readable by appending `.json`:

```
https://www.reddit.com/r/Forex/comments/1smfb93/am_i_cooked/
https://www.reddit.com/r/Forex/comments/1smfb93.json   ← full data, no auth needed
```

## Requirements

- Python 3.11+
- `requests`
