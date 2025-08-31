import os
import asyncio
import re
import logging
from typing import Set, Tuple

from db import init_db, save_business_batch, get_dsn, close_db, load_business_keys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ANSI escape codes for colored output
GREEN_ON_BLACK = "\033[32;40m"
RESET = "\033[0m"
from playwright.async_api import async_playwright


async def collect_current_listings(page, query: str, seen: Set[Tuple[str, str]], conn):
    """Collect visible listings from the results column."""
    new_entries: list[Tuple[str, str]] = []
    to_save: list[tuple] = []
    listings = await page.locator("//a[contains(@href, 'https://www.google.com/maps/place')]").all()
    logger.debug("Scanning %d sidebar listings", len(listings))
    for listing in listings:
        href = await listing.get_attribute("href")
        if not href:
            continue
        text = await listing.inner_text()
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        if not lines:
            continue
        name = lines[0]
        address = lines[1] if len(lines) > 1 else ""
        key = (name.strip().lower(), address.strip().lower())
        if key in seen:
            logger.debug("Already saved: %s | %s", name, address)
            continue
        seen.add(key)
        logger.info("%sSaving new listing: %s | %s%s", GREEN_ON_BLACK, name, address, RESET)
        new_entries.append(key)
        lat = lon = None
        match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", href)
        if match:
            lat = float(match.group(1))
            lon = float(match.group(2))
        to_save.append(
            (
                name,
                address,
                "",
                "",
                None,
                query,
                lat,
                lon,
            )
        )
    if to_save:
        with conn:
            save_business_batch(conn, to_save)
    return new_entries


async def show_toast(page, message: str) -> None:
    """Display a small toast message inside the browser page."""
    script = """
    (msg) => {
      const t = document.createElement('div');
      t.textContent = msg;
      t.style.cssText = 'position:fixed;top:10px;right:10px;z-index:9999;'
        + 'background:#333;color:#fff;padding:5px 10px;border-radius:4px;';
      document.body.appendChild(t);
      setTimeout(() => t.remove(), 3000);
    }
    """
    await page.evaluate(script, message)


async def monitor_map(query: str, dsn: str | None, *, headless: bool = False, interval: float = 2.0):
    conn = init_db(get_dsn(dsn))
    seen: Set[Tuple[str, str]] = load_business_keys(conn)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        # Ignore certificate errors in development environments
        page = await browser.new_page(ignore_https_errors=True)
        await page.goto("https://www.google.com/maps", timeout=60000)
        await page.fill("//input[@id='searchboxinput']", query)
        await page.keyboard.press("Enter")
        # wait for at least one sidebar result before continuing
        try:
            await page.wait_for_selector(
                "//a[contains(@href, 'https://www.google.com/maps/place')]",
                timeout=15000,
            )
        except Exception:
            logger.warning("Timed out waiting for initial results")
        await page.wait_for_timeout(1000)
        try:
            checkbox = page.get_by_role("checkbox", name="Update results when map moves")
            if await checkbox.count() and (await checkbox.get_attribute("aria-checked")) != "true":
                await checkbox.click()
                await page.wait_for_timeout(1000)
        except Exception:
            pass
        await page.mouse.click(400, 300)
        while True:
            try:
                await page.wait_for_selector(
                    "//a[contains(@href, 'https://www.google.com/maps/place')]",
                    timeout=10000,
                )
            except Exception:
                logger.warning("No sidebar results detected")
            new_entries = await collect_current_listings(page, query, seen, conn)
            if new_entries:
                logger.debug(
                    "Found %d new result%s (total %d)",
                    len(new_entries),
                    "s" if len(new_entries) != 1 else "",
                    len(seen),
                )
            else:
                logger.debug("No new results found (total %d)", len(seen))
            for name, _ in new_entries:
                await show_toast(page, f"Saved: {name}")
            await page.wait_for_timeout(int(interval * 1000))

        await browser.close()
    close_db(conn)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Monitor Google Maps results while you move the map manually"
    )
    parser.add_argument("query")
    parser.add_argument("dsn", nargs="?", help="Postgres DSN")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between checks")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument(
        "--store",
        choices=["postgres", "cassandra", "sqlite", "csv"],
        help="Storage backend",
    )
    args = parser.parse_args()

    if args.store:
        os.environ["MAPS_STORAGE"] = args.store

    asyncio.run(
        monitor_map(
            args.query,
            get_dsn(args.dsn),
            headless=args.headless,
            interval=args.interval,
        )
    )
