import argparse
import asyncio
import csv
import os
import random
import re
from typing import Any, Dict, Iterable, Optional, Sequence

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
    headless: bool = True,
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
        browser = await p.chromium.launch(headless=headless, args=list(launch_args or []))
        page = await browser.new_page()
        try:
            await run(page)
        finally:
            await browser.close()

    if own_store:
        store.close()


def load_list(path: str) -> list[str]:
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        return [
            ", ".join(part.strip() for part in row if part.strip())
            for row in reader
            if any(part.strip() for part in row)
        ]


def resolve_values(value: Optional[str], path: str, label: str) -> list[str]:
    if value:
        return [value]
    try:
        values = load_list(path)
    except FileNotFoundError as exc:  # pragma: no cover - CLI convenience
        raise SystemExit(f"{label} file not found: {path}") from exc
    if not values:
        raise SystemExit(f"No {label.lower()} found in {path}")
    return values


def build_launch_args(
    base_args: Iterable[str],
    *,
    headless: bool,
    screen_width: int,
    screen_height: int,
) -> list[str]:
    args = list(base_args)
    if not headless and not any(arg.startswith("--window-size=") for arg in args):
        args.append(f"--window-size={screen_width},{screen_height}")
    return args


async def run_cli(args: argparse.Namespace) -> None:
    if args.max_delay < args.min_delay:
        raise SystemExit("--max-delay must be greater than or equal to --min-delay")

    cities = resolve_values(args.city, args.cities_file, "Cities")
    terms = resolve_values(args.term, args.terms_file, "Terms")

    launch_args = build_launch_args(
        args.launch_args,
        headless=args.headless,
        screen_width=args.screen_width,
        screen_height=args.screen_height,
    )

    store = BusinessStore(args.dsn)
    try:
        for city in cities:
            for term in terms:
                print(f"[mapmonkey] Scraping {city!r} for term {term!r}")
                await experimental_scrape_city_grid(
                    city,
                    term,
                    steps=args.steps,
                    spacing=args.spacing_deg,
                    per_grid_total=args.per_grid_total,
                    dsn=args.dsn,
                    min_delay=args.min_delay,
                    max_delay=args.max_delay,
                    launch_args=launch_args,
                    store=store,
                    headless=args.headless,
                )
    finally:
        store.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the experimental Google Maps crawler",
    )
    parser.add_argument("--city", help="Single city to scrape (overrides --cities-file)")
    parser.add_argument(
        "--cities-file",
        default="cities.csv",
        help="CSV file containing cities (default: %(default)s)",
    )
    parser.add_argument("--term", help="Single search term (overrides --terms-file)")
    parser.add_argument(
        "--terms-file",
        default="terms.csv",
        help="CSV file containing search terms (default: %(default)s)",
    )
    parser.add_argument("--steps", type=int, default=0, help="Grid radius in each direction")
    parser.add_argument(
        "--spacing-deg",
        type=float,
        default=0.02,
        help="Distance in degrees between neighbouring grid cells",
    )
    parser.add_argument(
        "--per-grid-total",
        type=int,
        default=50,
        help="Maximum number of cards to collect per grid coordinate",
    )
    parser.add_argument("--dsn", help="Database DSN or path (defaults to environment configuration)")
    parser.add_argument(
        "--min-delay",
        type=float,
        default=10.0,
        help="Minimum seconds to wait between grid cells",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=30.0,
        help="Maximum seconds to wait between grid cells",
    )
    parser.add_argument(
        "--launch-arg",
        dest="launch_args",
        action="append",
        default=[],
        help="Additional Chromium launch argument (repeatable)",
    )
    parser.add_argument(
        "--store",
        choices=["postgres", "cassandra", "sqlite", "csv"],
        help="Storage backend to use (overrides MAPS_STORAGE)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chromium in headless mode (hidden window)",
    )
    parser.add_argument(
        "--screen-width",
        type=int,
        default=1920,
        help="Browser window width when not running headless",
    )
    parser.add_argument(
        "--screen-height",
        type=int,
        default=1080,
        help="Browser window height when not running headless",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    if cli_args.store:
        os.environ["MAPS_STORAGE"] = cli_args.store
    asyncio.run(run_cli(cli_args))
