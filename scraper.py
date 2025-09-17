import asyncio
import logging
import random
import re
from typing import Any, Callable, Dict, Optional, Sequence

from playwright.async_api import Page, async_playwright

from storage_manager import BusinessRecord, BusinessStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GREEN_ON_BLACK = "\033[32;40m"
RESET = "\033[0m"

_geocode_cache: dict[str, tuple[float, float]] = {}


async def _notify(callback: Optional[Callable], *args, **kwargs) -> None:
    if callback is None:
        return
    result = callback(*args, **kwargs)
    if asyncio.iscoroutine(result):
        await result


async def _geocode_city(page: Page, city: str) -> tuple[float, float]:
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


async def scrape_at_location(
    page: Page,
    query: str,
    total: int,
    lat: float,
    lon: float,
    *,
    store: BusinessStore,
    context: Dict[str, Any],
    progress_cb: Optional[Callable] = None,
    heartbeat_cb: Optional[Callable] = None,
    event_cb: Optional[Callable] = None,
    business_cb: Optional[Callable] = None,
    batch_size: int = 10,
) -> None:
    await _notify(event_cb, "info", f"Scraping {query} at {lat:.5f},{lon:.5f}", context=context)

    await page.goto(f"https://www.google.com/maps/@{lat},{lon},15z", timeout=60000)
    await page.fill("//input[@id='searchboxinput']", query)
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(2000)
    await _notify(progress_cb, 0, total)

    results_locator = page.locator("//a[contains(@href, 'https://www.google.com/maps/place')]")
    previous_count = -1
    stagnation_loops = 0
    max_loops = 20
    while True:
        await _notify(heartbeat_cb)
        try:
            current_count = await results_locator.count()
        except Exception:
            current_count = 0
        if current_count >= total:
            break
        if current_count == previous_count:
            stagnation_loops += 1
        else:
            stagnation_loops = 0
        if stagnation_loops > 5 or max_loops <= 0:
            break
        previous_count = current_count
        max_loops -= 1
        await page.mouse.wheel(0, 2000)
        try:
            await page.wait_for_function(
                "(expected) => document.querySelectorAll(\"a[href^='https://www.google.com/maps/place']\").length >= expected",
                arg=current_count + 1,
                timeout=2000,
            )
        except Exception:
            await page.wait_for_timeout(750)

    listings = []
    try:
        listings = await results_locator.all()
    except Exception as exc:
        await _notify(event_cb, "error", f"Failed to enumerate listings: {exc}", context=context)

    listings = listings[:total]
    saved_count = 0
    batch: list[BusinessRecord] = []

    for listing in listings:
        await _notify(heartbeat_cb)
        try:
            await listing.click()
            await page.wait_for_timeout(1500)
        except Exception as exc:
            await _notify(event_cb, "warning", f"Failed to open listing: {exc}", context=context)
            continue

        name_locator = page.locator("h1.DUwDvf.lfPIob")
        name = await name_locator.inner_text() if await name_locator.count() else ""

        address_locator = page.locator(
            "//button[@data-item-id='address']//div[contains(@class, 'fontBodyMedium')]"
        )
        address = ""
        if await address_locator.count():
            try:
                address = await address_locator.nth(0).inner_text()
            except Exception:
                address = ""

        website_locator = page.locator(
            "//a[@data-item-id='authority']//div[contains(@class, 'fontBodyMedium')]"
        )
        website = ""
        if await website_locator.count():
            try:
                website = await website_locator.nth(0).inner_text()
            except Exception:
                website = ""

        phone_locator = page.locator(
            "//button[contains(@data-item-id, 'phone')]//div[contains(@class, 'fontBodyMedium')]"
        )
        phone = ""
        if await phone_locator.count():
            try:
                phone = await phone_locator.nth(0).inner_text()
            except Exception:
                phone = ""

        reviews_average = None
        reviews_locator = page.locator(
            "//div[@jsaction='pane.reviewChart.moreReviews']//div[@role='img']"
        )
        if await reviews_locator.count():
            text = await reviews_locator.get_attribute("aria-label")
            if text:
                try:
                    reviews_average = float(text.split()[0].replace(",", "."))
                except ValueError:
                    reviews_average = None

        match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", page.url)
        lat_val = float(match.group(1)) if match else None
        lon_val = float(match.group(2)) if match else None

        record = BusinessRecord(
            name=name,
            address=address,
            website=website,
            phone=phone,
            reviews_average=reviews_average,
            query=query,
            latitude=lat_val,
            longitude=lon_val,
        )

        batch.append(record)
        if len(batch) >= batch_size:
            try:
                inserted = store.save_new(batch)
            except Exception as exc:
                await _notify(event_cb, "error", f"Failed to persist batch: {exc}", context=context)
                batch.clear()
                continue
            batch.clear()
            if inserted:
                saved_count += len(inserted)
                for item in inserted:
                    logger.info(
                        "%sSaving new listing: %s | %s%s",
                        GREEN_ON_BLACK,
                        item.get("name", ""),
                        item.get("address", ""),
                        RESET,
                    )
                await _notify(progress_cb, min(saved_count, total), total)
                await _notify(business_cb, inserted, context)

    if batch:
        try:
            inserted = store.save_new(batch)
        except Exception as exc:
            await _notify(event_cb, "error", f"Failed to persist final batch: {exc}", context=context)
            inserted = []
        if inserted:
            saved_count += len(inserted)
            for item in inserted:
                logger.info(
                    "%sSaving new listing: %s | %s%s",
                    GREEN_ON_BLACK,
                    item.get("name", ""),
                    item.get("address", ""),
                    RESET,
                )
            await _notify(business_cb, inserted, context)

    await _notify(progress_cb, min(saved_count, total), total)
    await _notify(heartbeat_cb)


async def scrape_city_grid(
    city: str,
    query: str,
    steps: int,
    spacing: float,
    total: int,
    dsn: Optional[str],
    *,
    headless: bool = False,
    min_delay: float = 15.0,
    max_delay: float = 60.0,
    launch_args: Optional[Sequence[str]] = None,
    page: Optional[Page] = None,
    store: Optional[BusinessStore] = None,
    context: Optional[Dict[str, Any]] = None,
    progress_cb: Optional[Callable] = None,
    heartbeat_cb: Optional[Callable] = None,
    event_cb: Optional[Callable] = None,
    business_cb: Optional[Callable] = None,
) -> None:
    """Scrape a city's grid using an existing Playwright page and store."""

    context = context or {"city": city, "query": query}
    manage_store = store is None
    if store is None:
        store = BusinessStore(dsn)

    async def run(active_page: Page) -> None:
        lat_center, lon_center = await _geocode_city(active_page, city)
        coords = [
            (i, j)
            for i in range(-steps, steps + 1)
            for j in range(-steps, steps + 1)
        ]
        random.shuffle(coords)
        for i, j in coords:
            lat = lat_center + i * spacing
            lon = lon_center + j * spacing
            cell_context = dict(context)
            cell_context.update({"grid": {"i": i, "j": j}, "latitude": lat, "longitude": lon})
            await scrape_at_location(
                active_page,
                query,
                total,
                lat,
                lon,
                store=store,
                context=cell_context,
                progress_cb=progress_cb,
                heartbeat_cb=heartbeat_cb,
                event_cb=event_cb,
                business_cb=business_cb,
            )
            await _notify(progress_cb, 0, total)
            delay = random.uniform(min_delay, max_delay)
            await _notify(event_cb, "info", f"Cooling down for {delay:.1f}s", context=cell_context)
            await active_page.wait_for_timeout(int(delay * 1000))

    if page is None:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless, args=list(launch_args or []))
            active_page = await browser.new_page()
            try:
                await run(active_page)
            finally:
                await browser.close()
    else:
        await run(page)

    if manage_store:
        store.close()
