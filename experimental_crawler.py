import random
import re
from typing import Any, Dict, Optional, Sequence

from playwright.async_api import Page, async_playwright

from scraper import _geocode_city  # type: ignore
from storage_manager import BusinessRecord, BusinessStore


async def _scroll_results(page: Page, target: int) -> None:
    cards_locator = page.locator("div[role='feed'] div[role='article']")
    last_count = -1
    idle_rounds = 0
    while True:
        try:
            count = await cards_locator.count()
        except Exception:
            count = 0
        if count >= target:
            break
        if count == last_count:
            idle_rounds += 1
        else:
            idle_rounds = 0
        if idle_rounds > 6:
            break
        last_count = count
        await page.evaluate(
            """
            () => {
              const feed = document.querySelector("div[role='feed']");
              if (feed) feed.scrollBy(0, feed.clientHeight * 0.9);
            }
            """
        )
        try:
            await page.wait_for_function(
                "(expected) => document.querySelectorAll(`div[role='feed'] div[role='article']`).length >= expected",
                arg=count + 1,
                timeout=2500,
            )
        except Exception:
            await page.wait_for_timeout(800)


async def _extract_cards(page: Page, limit: int) -> list[Dict[str, Any]]:
    cards = await page.locator("div[role='feed'] div[role='article']").all()
    results: list[Dict[str, Any]] = []
    for card in cards[:limit]:
        data = await card.evaluate(
            """
            (node) => {
              const anchor = node.querySelector('a[href^="https://www.google.com/maps/place"]');
              const title = anchor ? anchor.getAttribute('aria-label') || anchor.textContent || '' : '';
              const addressNode = node.querySelector('[data-item-id="address"] span');
              const websiteNode = node.querySelector('[data-item-id="authority"] span');
              const phoneNode = node.querySelector('[data-item-id*="phone"] span');
              const ratingNode = node.querySelector('[aria-label$="stars"]');
              const href = anchor ? anchor.getAttribute('href') || '' : '';
              return {
                title,
                address: addressNode ? addressNode.textContent || '' : '',
                website: websiteNode ? websiteNode.textContent || '' : '',
                phone: phoneNode ? phoneNode.textContent || '' : '',
                rating: ratingNode ? ratingNode.getAttribute('aria-label') || '' : '',
                href,
              };
            }
            """
        )
        if data:
            results.append(data)
    return results


def _parse_lat_lon(href: str) -> tuple[Optional[float], Optional[float]]:
    if not href:
        return None, None
    match = re.search(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)", href)
    if match:
        return float(match.group(1)), float(match.group(2))
    return None, None


async def experimental_scrape_city_grid(
    city: str,
    term: str,
    *,
    steps: int,
    spacing: float,
    per_grid_total: int,
    dsn: Optional[str],
    min_delay: float = 10.0,
    max_delay: float = 30.0,
    launch_args: Optional[Sequence[str]] = None,
    store: Optional[BusinessStore] = None,
) -> None:
    query = f"\"{city}\" {term}".strip()
    own_store = store is None
    if own_store:
        store = BusinessStore(dsn)

    async def run(page: Page) -> None:
        lat_center, lon_center = await _geocode_city(page, city)
        coords = [
            (i, j)
            for i in range(-steps, steps + 1)
            for j in range(-steps, steps + 1)
        ]
        random.shuffle(coords)

        for i, j in coords:
            lat = lat_center + i * spacing
            lon = lon_center + j * spacing
            await page.goto(f"https://www.google.com/maps/@{lat},{lon},14z", timeout=60000)
            await page.fill("//input[@id='searchboxinput']", query)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(2000)
            await _scroll_results(page, per_grid_total)
            cards = await _extract_cards(page, per_grid_total)
            batch = []
            for card in cards:
                lat_val, lon_val = _parse_lat_lon(card.get("href", ""))
                rating_text = card.get("rating") or ""
                try:
                    rating_value = float(rating_text.split()[0].replace(",", ".")) if rating_text else None
                except Exception:
                    rating_value = None
                record = BusinessRecord(
                    name=card.get("title", ""),
                    address=card.get("address", ""),
                    website=card.get("website", ""),
                    phone=card.get("phone", ""),
                    reviews_average=rating_value,
                    query=query,
                    latitude=lat_val,
                    longitude=lon_val,
                )
                batch.append(record)
            if batch:
                store.save_new(batch)
            delay = random.uniform(min_delay, max_delay)
            await page.wait_for_timeout(int(delay * 1000))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=list(launch_args or []))
        page = await browser.new_page()
        try:
            await run(page)
        finally:
            await browser.close()

    if own_store:
        store.close()
