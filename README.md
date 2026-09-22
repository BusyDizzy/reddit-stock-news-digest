# Reddit Trending Stocks → News Digest

A small daily pipeline that answers one question: **the stocks Reddit is talking about most today — what is actually in the news about them?**

It combines three APIs:

1. **[ApeWisdom](https://apewisdom.io)** returns the most-mentioned tickers across Reddit investing communities.
2. **[SerpApi Google News API](https://serpapi.com/google-news-api)** returns the last 24 hours of headlines for each company.
3. **OpenAI** (optional) turns each company's headlines into a 2–3 sentence explanation of why it's trending.
4. **Telegram Bot API** posts the digest to a channel once a day.

Built as a news module for [TickerForge](https://github.com/BusyDizzy), my investing analytics project.

## Example output

```
🔥 Most discussed stocks on Reddit — news digest
22 Sep 2026

1. $META · Meta Platforms
Reddit #1 (▲9) · 429 mentions
<2–3 sentence AI summary of why the stock is discussed today>
Sources: Barchart.com · Seeking Alpha · Pluang

2. $AMD · AMD
Reddit #2 (▲3) · 320 mentions
• ...
```

## How it works

**Ticker selection.** ApeWisdom's `all-stocks` list mixes stocks and ETFs, and it also counts tickers that are ordinary English words (`IT`, `YOU`, `ON`, `ALL`), which inflates their mentions. The script skips ETFs (by name keywords such as *ETF*, *Fund*, *iShares*, plus a list of common ETF tickers), skips common-word tickers, merges share classes of the same company (`GOOG`/`GOOGL`) and keeps the top N by mentions.

**News search.** Each company is queried by name rather than ticker (`"Micron Technology stock when:1d"`), because names are far less ambiguous. `when:1d` is a Google News operator that the SerpApi `google_news` engine passes through. The response is flattened (Google News groups some articles into story clusters), filtered by date, de-duplicated by link and near-identical title, and trimmed to the newest 3 headlines.

**AI summary (optional).** The headlines plus the Reddit attention data (rank and mentions today vs. 24 hours ago) go to an OpenAI model, which writes a short neutral summary of why the stock is being discussed. The prompt restricts the model to facts from the headlines and forbids recommendations. The original articles stay linked as sources. If the key isn't set, `--no-ai` is passed, or a call fails, that company falls back to the plain headline list, so one API error never blocks the post.

**Posting.** Messages use Telegram HTML formatting with all text escaped, link previews disabled, and they are split under the 4096-character limit without breaking a company's block.

## Setup

```bash
git clone <repo> && cd ape-news
pip install -r requirements.txt
cp .env.example .env        # add your keys
python ape_news.py --dry-run   # preview the digest in the terminal
python ape_news.py             # post to Telegram
```

Telegram: create a bot with @BotFather, add it to your channel as an administrator with permission to post, and set `TELEGRAM_CHAT_ID` to `@channel_name`.

## Daily schedule

cron (server time in UTC; 12:30 UTC is before the US market open):

```
30 12 * * 1-5  cd /opt/ape-news && /usr/bin/python3 ape_news.py >> digest.log 2>&1
```

## Search budget

Each run uses one SerpApi search per ticker. Eight tickers on weekdays is roughly 170 searches a month. Adjust `MAX_TICKERS` to your plan. Identical searches repeated within an hour are served from SerpApi's cache, so re-running a dry run doesn't double the cost.

## Options

| Flag / env var | Default | Meaning |
|---|---|---|
| `--tickers` / `MAX_TICKERS` | 8 | Companies in the digest |
| `--per-ticker` / `NEWS_PER_TICKER` | 3 | Headlines per company |
| `--min-mentions` / `MIN_MENTIONS` | 20 | Stop once mentions drop below this |
| `--no-ai` | off | Headlines only, skip the LLM step |
| `OPENAI_MODEL` | none | Model for summaries (required if `OPENAI_API_KEY` is set) |
| `SUMMARY_LANGUAGE` | English | Language of the summaries |
| `--save-json FILE` | none | Save collected data (useful for debugging or feeding another service) |
| `--dry-run` | off | Print instead of posting |

Not investment advice.