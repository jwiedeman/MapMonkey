import os
import asyncio
import random
import re
import logging
import json
from typing import Sequence, Set, Tuple

from playwright.async_api import async_playwright
from db import init_db, save_business, get_dsn, close_db, load_business_keys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ANSI escape codes for colored output
GREEN_ON_BLACK = "\033[32;40m"
RESET = "\033[0m"

# Cache geocoding results to avoid repeated requests
_geocode_cache: dict[str, tuple[float, float]] = {}


def _update_batch_state(fill: int, total: int, state_file: str = "run_state.json") -> None:
    """Update batch progress in the run_state.json file."""
    try:
        with open(state_file) as f:
            state = json.load(f)
    except Exception:
        state = {}
    state["batch_fill"] = fill
    state["batch_total"] = total
    tmp = state_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, state_file)


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

    _update_batch_state(0, total)
    saved_count = 0
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

        values = (
            name,
            address,
            website,
            phone,
            reviews_average,
            query,
            lat_val,
            lon_val,
        )
        try:
            save_business(conn, values)
        except Exception as exc:
            logger.error("Error saving listing %s | %s: %s", name, address, exc)
            continue
        seen.add(key)
        saved_count += 1
        logger.info("%sSaving new listing: %s | %s%s", GREEN_ON_BLACK, name, address, RESET)
        _update_batch_state(saved_count, total)
    _update_batch_state(0, total)


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

    async def geocode_city(pw_page):
        cached = _geocode_cache.get(city)
        if cached:
            return cached
        await pw_page.goto("https://www.google.com/maps", timeout=60000)
        await pw_page.fill("//input[@id='searchboxinput']", city)
        await pw_page.keyboard.press("Enter")
        try:
            await pw_page.wait_for_selector(
                "//a[contains(@href, 'https://www.google.com/maps/place')]",
                timeout=15000,
            )
        except Exception:
            logger.warning("Timed out waiting for city results for %s", city)
        await pw_page.wait_for_timeout(1000)
        match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", pw_page.url)
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
