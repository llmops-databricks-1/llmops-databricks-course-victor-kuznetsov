"""Web page content scraper (artlake-scrape-pages entry point)."""

from __future__ import annotations

import hashlib
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from loguru import logger
from pydantic import HttpUrl

from artlake.models.event import ProcessingStatus, ScrapedPage

_USER_AGENT = "artlake-scraper/1.0"
_TIMEOUT = 10
_ARTIFACT_EXTENSIONS = frozenset({".pdf", ".jpg", ".jpeg", ".png", ".webp", ".gif"})
_ARTIFACT_IMG_KEYWORDS = frozenset(
    {"poster", "flyer", "brochure", "open-call", "opencall"}
)


def fingerprint(url: str) -> str:
    """sha256 hex digest of the URL string — matches sha2(url, 256) in Spark."""
    return hashlib.sha256(url.encode()).hexdigest()


def is_allowed_by_robots(url: str, timeout: int = _TIMEOUT) -> bool:
    """Return True if robots.txt permits fetching *url* with our user-agent."""
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception:
        return True  # unreadable robots.txt → assume allowed
    return bool(rp.can_fetch(_USER_AGENT, url))


def fetch_llms_txt(url: str, timeout: int = _TIMEOUT) -> str | None:
    """Try /<domain>/llms.txt — return content if found, else None."""
    parsed = urlparse(url)
    llms_url = f"{parsed.scheme}://{parsed.netloc}/llms.txt"
    try:
        resp = requests.get(
            llms_url,
            headers={"User-Agent": _USER_AGENT},
            timeout=timeout,
        )
        if resp.status_code == 200 and resp.text.strip():
            logger.debug("llms.txt found at {}", llms_url)
            return str(resp.text)
    except requests.RequestException:
        pass
    return None


def fetch_html(url: str, timeout: int = _TIMEOUT) -> str | None:
    """Fetch raw HTML from *url*; return None on any error."""
    try:
        resp = requests.get(
            url,
            headers={"User-Agent": _USER_AGENT},
            timeout=timeout,
        )
        resp.raise_for_status()
        return str(resp.text)
    except requests.RequestException as exc:
        logger.warning("Failed to fetch {}: {}", url, exc)
        return None


def extract_from_html(html: str, base_url: str) -> tuple[str, str, list[str]]:
    """Parse *html* and return (title, raw_text, artifact_urls)."""
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    raw_text = soup.get_text(separator=" ", strip=True)

    parsed_base = urlparse(base_url)
    base_origin = f"{parsed_base.scheme}://{parsed_base.netloc}"
    artifact_urls: list[str] = []

    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"]
        if not href.startswith("http"):
            href = f"{base_origin}/{href.lstrip('/')}"
        if any(href.lower().split("?")[0].endswith(ext) for ext in _ARTIFACT_EXTENSIONS):
            artifact_urls.append(href)

    for img_tag in soup.find_all("img"):
        alt = (img_tag.get("alt") or "").lower()
        src: str = img_tag.get("src") or ""
        if any(kw in alt for kw in _ARTIFACT_IMG_KEYWORDS):
            if not src.startswith("http"):
                src = f"{base_origin}/{src.lstrip('/')}"
            if src:
                artifact_urls.append(src)

    return title, raw_text, list(dict.fromkeys(artifact_urls))


def scrape_url(url: str, timeout: int = _TIMEOUT) -> ScrapedPage:
    """Fetch *url* and return a ScrapedPage (never raises)."""
    fp = fingerprint(url)

    if not is_allowed_by_robots(url, timeout=timeout):
        logger.info("robots.txt disallows: {}", url)
        return ScrapedPage(
            fingerprint=fp,
            url=HttpUrl(url),
            title="",
            raw_text="",
            artifact_urls=[],
            processing_status=ProcessingStatus.FAILED,
            error="robots.txt disallows this URL",
        )

    llms_content = fetch_llms_txt(url, timeout=timeout)
    if llms_content:
        return ScrapedPage(
            fingerprint=fp,
            url=HttpUrl(url),
            title="",
            raw_text=llms_content,
            artifact_urls=[],
            processing_status=ProcessingStatus.NEW,
        )

    html = fetch_html(url, timeout=timeout)
    if html is None:
        return ScrapedPage(
            fingerprint=fp,
            url=HttpUrl(url),
            title="",
            raw_text="",
            artifact_urls=[],
            processing_status=ProcessingStatus.FAILED,
            error="failed to fetch page",
        )

    title, raw_text, artifact_urls = extract_from_html(html, url)
    logger.info(
        "Scraped {} — {} chars, {} artifacts", url, len(raw_text), len(artifact_urls)
    )
    return ScrapedPage(
        fingerprint=fp,
        url=HttpUrl(url),
        title=title,
        raw_text=raw_text,
        artifact_urls=artifact_urls,
        processing_status=ProcessingStatus.NEW,
    )


def run_list(
    seen_urls_table: str,
    scraped_pages_table: str,
    limit: int = 0,
) -> list[str]:
    """Anti-join seen_urls vs scraped_pages on fingerprint; return unseen URLs.

    Also emits the list as a Databricks task value so for_each_task can iterate.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    seen_df = spark.table(seen_urls_table).select("url", "fingerprint")

    if spark.catalog.tableExists(scraped_pages_table):
        scraped_fp = spark.table(scraped_pages_table).select("fingerprint")
        unseen_df = seen_df.join(scraped_fp, on="fingerprint", how="left_anti")
    else:
        logger.info("scraped_pages table does not exist yet — all URLs are unseen")
        unseen_df = seen_df

    if limit > 0:
        unseen_df = unseen_df.limit(limit)

    urls: list[str] = [row["url"] for row in unseen_df.select("url").collect()]
    logger.info("Unseen URLs to scrape: {}", len(urls))

    try:
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key="urls", value=urls)
        logger.info("Task value 'urls' set with {} entries", len(urls))
    except ImportError:
        logger.warning(
            "dbutils not available — skipping task value set (non-Databricks env)"
        )

    return urls


def run_scrape(url: str, scraped_pages_table: str, env: str = "dev") -> None:
    """Scrape a single URL and write the result to *scraped_pages_table*."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    page = scrape_url(url)

    row = page.model_dump()
    row["url"] = str(row["url"])

    df = spark.createDataFrame([row])

    parts = scraped_pages_table.split(".")
    if len(parts) == 3:
        catalog, schema, _ = parts
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(scraped_pages_table)
    )
    logger.info(
        "Wrote scraped page {} [{}] to {}",
        url,
        page.processing_status,
        scraped_pages_table,
    )


def main() -> None:
    """Entry point for artlake-scrape-pages wheel task."""
    import argparse

    parser = argparse.ArgumentParser(description="ArtLake page scraper")
    parser.add_argument(
        "--mode",
        choices=["list", "scrape"],
        required=True,
        help="'list' emits unseen URLs as a task value; 'scrape' fetches one URL",
    )
    parser.add_argument("--url", help="URL to scrape (required for --mode scrape)")
    parser.add_argument(
        "--seen-urls-table",
        default="artlake.staging.seen_urls",
        help="Fully-qualified seen_urls Delta table",
    )
    parser.add_argument(
        "--scraped-pages-table",
        default="artlake.staging.scraped_pages",
        help="Fully-qualified scraped_pages Delta table",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max URLs to emit in list mode (0 = no limit)",
    )
    parser.add_argument(
        "--env",
        default="dev",
        help="Deployment environment (dev/tst/acc/prd)",
    )
    args = parser.parse_args()

    if args.mode == "list":
        run_list(
            seen_urls_table=args.seen_urls_table,
            scraped_pages_table=args.scraped_pages_table,
            limit=args.limit,
        )
    else:
        if not args.url:
            parser.error("--url is required for --mode scrape")
        run_scrape(
            url=args.url,
            scraped_pages_table=args.scraped_pages_table,
            env=args.env,
        )
