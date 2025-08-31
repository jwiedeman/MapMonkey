import os
import asyncio
import random
import re
import logging
from typing import Sequence, Set, Tuple

# Cache geocoding results to avoid repeated requests
_geocode_cache: dict[str, tuple[float, float]] = {}
from playwright.async_api import async_playwright
from db import init_db, save_business_batch, get_dsn, close_db, load_business_keys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ANSI escape codes for colored output
GREEN_ON_BLACK = "\033[32;40m"
RESET = "\033[0m"


async def scrape_at_location(
    page,
    query: str,
    total: int,
    lat: float,
    lon: float,
    seen: Set[Tuple[str, str]],
    conn,
):
    await page.goto(f"https://www.google.com/maps/@{lat},{lon},17z", timeout=60000)
    await page.fill("//input[@id='searchboxinput']", query)
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(5000)
    await page.hover("//a[contains(@href, 'https://www.google.com/maps/place')]")

    counted = 0
    listings = []
    while True:
        await page.mouse.wheel(0, 10000)
        await page.wait_for_timeout(2000)
        current = await page.locator("//a[contains(@href, 'https://www.google.com/maps/place')]").count()
        if current >= total:
            listings = await page.locator("//a[contains(@href, 'https://www.google.com/maps/place')]").all()
            listings = listings[:total]
            break
        if current == counted:
            listings = await page.locator("//a[contains(@href, 'https://www.google.com/maps/place')]").all()
            break
        counted = current

    to_save: list[tuple] = []
    for listing in listings:
        await listing.click()
        await page.wait_for_timeout(3000)

        name = await page.locator('h1.DUwDvf.lfPIob').inner_text() if await page.locator('h1.DUwDvf.lfPIob').count() else ""

        address = ""
        if await page.locator('//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]').count():
            elements = await page.locator('//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]').all()
            if elements:
                address = await elements[0].inner_text()

        website = ""
        if await page.locator('//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]').count():
            elements = await page.locator('//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]').all()
            if elements:
                website = await elements[0].inner_text()

        phone = ""
        if await page.locator('//button[contains(@data-item-id, "phone")]//div[contains(@class, "fontBodyMedium")]').count():
            elements = await page.locator('//button[contains(@data-item-id, "phone")]//div[contains(@class, "fontBodyMedium")]').all()
            if elements:
                phone = await elements[0].inner_text()

        reviews_average = None
        if await page.locator('//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]').count():
            text = await page.locator('//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]').get_attribute('aria-label')
            if text:
                try:
                    reviews_average = float(text.split()[0].replace(',', '.'))
                except ValueError:
                    reviews_average = None

        lat_val = lon_val = None
        url = page.url
        match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", url)
        if match:
            lat_val = float(match.group(1))
            lon_val = float(match.group(2))

        key = (name.strip().lower(), address.strip().lower())
        if key in seen:
            logger.debug("Already saved: %s | %s", name, address)
            continue

        seen.add(key)
        logger.info("%sSaving new listing: %s | %s%s", GREEN_ON_BLACK, name, address, RESET)
        to_save.append(
            (
                name,
                address,
                website,
                phone,
                reviews_average,
                query,
                lat_val,
                lon_val,
            )
        )
    if to_save:
        with conn:
            save_business_batch(conn, to_save)


async def scrape_city_grid(
    city: str,
    query: str,
    steps: int,
    spacing: float,
    total: int,
    dsn: str | None,
    *,
    headless: bool = False,
    min_delay: float = 15.0,
    max_delay: float = 60.0,
    launch_args: Sequence[str] | None = None,
    page=None,
):
    """Scrape a city's grid using an existing Playwright page if provided."""

    db_conn = init_db(get_dsn(dsn))
    seen: Set[Tuple[str, str]] = load_business_keys(db_conn)

    async def geocode_city(page):
        cached = _geocode_cache.get(city)
        if cached:
            return cached
        await page.goto("https://www.google.com/maps", timeout=60000)
        await page.fill("//input[@id='searchboxinput']", city)
        await page.keyboard.press("Enter")
        try:
            await page.wait_for_selector(
                "//a[contains(@href, 'https://www.google.com/maps/place')]",
                timeout=15000,
            )
        except Exception:
            logger.warning("Timed out waiting for city results for %s", city)
        await page.wait_for_timeout(1000)
        match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", page.url)
        if not match:
            raise ValueError(f"Could not find coordinates for city: {city}")
        lat = float(match.group(1))
        lon = float(match.group(2))
        _geocode_cache[city] = (lat, lon)
        return lat, lon

    manage_browser = page is None

    async def run(pw_page):
        lat_center, lon_center = await geocode_city(pw_page)

        coords = [
            (i, j)
            for i in range(-steps, steps + 1)
            for j in range(-steps, steps + 1)
        ]
        random.shuffle(coords)
        for i, j in coords:
            lat = lat_center + i * spacing
            lon = lon_center + j * spacing
            await scrape_at_location(
                pw_page, query, total, lat, lon, seen, db_conn
            )
            delay = random.uniform(min_delay, max_delay)
            await pw_page.wait_for_timeout(int(delay * 1000))

    if manage_browser:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=headless, args=list(launch_args or [])
            )
            page = await browser.new_page()
            await run(page)
            await browser.close()
    else:
        await run(page)

    close_db(db_conn)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape a city grid from Google Maps")
    parser.add_argument("city")
    parser.add_argument("steps", type=int)
    parser.add_argument("spacing_deg", type=float)
    parser.add_argument("per_grid_total", type=int)
    parser.add_argument("dsn", nargs="?", help="Postgres DSN")
    parser.add_argument("--query", help="Single search term")
    parser.add_argument("--terms", help="Comma separated list of search terms")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--min-delay", type=float, default=15.0, help="Minimum delay between grid steps in seconds")
    parser.add_argument("--max-delay", type=float, default=60.0, help="Maximum delay between grid steps in seconds")
    parser.add_argument("--store", choices=["postgres", "cassandra", "sqlite", "csv"], help="Storage backend")
    args = parser.parse_args()

    if args.store:
        os.environ["MAPS_STORAGE"] = args.store

    queries = []
    if args.terms:
        queries.extend([t.strip() for t in args.terms.split(',') if t.strip()])
    if args.query:
        queries.insert(0, args.query)
    if not queries:
        parser.error("Provide a query or --terms")

    async def main():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=args.headless)
            page = await browser.new_page()
            for term in queries:
                search = f"\"{args.city}\" {term}".strip()
                await scrape_city_grid(
                    args.city,
                    search,
                    args.steps,
                    args.spacing_deg,
                    args.per_grid_total,
                    args.dsn,
                    min_delay=args.min_delay,
                    max_delay=args.max_delay,
                    page=page,
                )
            await browser.close()

    asyncio.run(main())
