import argparse
import asyncio
import json
import os
from db import get_dsn
from grid_worker import scrape_city_grid
from playwright.async_api import async_playwright



def load_state(path: str) -> dict:
    """Load the run state from JSON, providing defaults for new fields."""
    if os.path.exists(path):
        with open(path) as f:
            state = json.load(f)
    else:
        state = {"city_index": 0, "term_index": 0}

    # Ensure new keys exist so older state files can be upgraded seamlessly
    state.setdefault("total_cities", 0)
    state.setdefault("total_terms", 0)
    state.setdefault("overall_progress", 0)
    state.setdefault("overall_total", 0)
    state.setdefault("workers", {})
    return state


def save_state(path: str, state: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, path)


async def run_city(city: str, terms: list[str], state: dict, args) -> None:
    """Scrape all search terms for a single city using multiple browsers."""
    launch_args = [
        f"--window-size={args.screen_width},{args.screen_height}",
        "--window-position=0,0",
    ]
    async with async_playwright() as p:
        queue: asyncio.Queue[str] = asyncio.Queue()
        for term in terms[state["term_index"]:]:

            queue.put_nowait(term)

        browsers = []
        pages = []
        for _ in range(args.concurrency):
            browser = await p.chromium.launch(
                headless=args.headless, args=launch_args
            )
            page = await browser.new_page()
            browsers.append(browser)
            pages.append(page)

        lock = asyncio.Lock()

        async def worker(worker_id: int, page):
            nonlocal state

            while True:
                try:
                    term = queue.get_nowait()
                except asyncio.QueueEmpty:
                    # Mark worker as idle when no more work is available
                    async with lock:
                        state["workers"].pop(str(worker_id), None)
                        save_state(args.state_file, state)
                    break

                # Record which term this worker is processing
                async with lock:
                    state["workers"][str(worker_id)] = {"city": city, "term": term}
                    save_state(args.state_file, state)

                search = f"{city} {term}".strip()
                try:
                    await scrape_city_grid(
                        city,
                        search,
                        args.steps,
                        args.spacing_deg,
                        args.per_grid_total,
                        args.dsn,
                        min_delay=args.min_delay,
                        max_delay=args.max_delay,
                        page=page,
                    )
                except Exception as e:
                    print(f"Error processing term '{term}' in city '{city}': {e}")
                finally:
                    async with lock:
                        state["term_index"] += 1
                        state["overall_progress"] = (
                            state["city_index"] * state["total_terms"]
                            + state["term_index"]
                        )
                        state["workers"].pop(str(worker_id), None)
                        save_state(args.state_file, state)

        await asyncio.gather(
            *(worker(i, page) for i, page in enumerate(pages))
        )

        for browser in browsers:
            await browser.close()

async def main(args) -> None:
    args.dsn = get_dsn(args.dsn)
    terms = [t.strip() for t in args.terms.split(',') if t.strip()]
    cities = [args.city]
    if args.cities:
        cities.extend([c.strip() for c in args.cities.split(',') if c.strip()])


    state = load_state(args.state_file)
    # Update totals and overall counters in the state file
    state["total_cities"] = len(cities)
    state["total_terms"] = len(terms)
    state["overall_total"] = state["total_cities"] * state["total_terms"]
    state["overall_progress"] = (
        state["city_index"] * state["total_terms"] + state["term_index"]
    )
    state.setdefault("workers", {})
    save_state(args.state_file, state)

    for idx in range(state["city_index"], len(cities)):
        city = cities[idx]
        try:
            await run_city(city, terms, state, args)
        except Exception as e:
            print(f"Error processing city '{city}': {e}")
            break
        state["term_index"] = 0
        state["city_index"] = idx + 1
        state["overall_progress"] = (
            state["city_index"] * state["total_terms"] + state["term_index"]
        )
        save_state(args.state_file, state)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Run Google Maps searches across multiple terms for each city, "
            "using multiple browsers concurrently"
        ),
    )
    parser.add_argument("city", help="City name to search around")
    parser.add_argument("--cities", help="Comma separated list of additional cities")
    parser.add_argument("--terms", required=True, help="Comma separated search terms")
    parser.add_argument("--steps", type=int, default=0, help="Grid radius in steps (0 for single location)")
    parser.add_argument("--spacing-deg", type=float, default=0.02)
    parser.add_argument("--per-grid-total", type=int, default=50)
    parser.add_argument("--dsn", help="Database DSN or path (depends on storage)")
    parser.add_argument("--screen-width", type=int, default=1920)
    parser.add_argument("--screen-height", type=int, default=1080)
    parser.add_argument("--store", choices=["postgres", "cassandra", "sqlite", "csv"], help="Storage backend")
    parser.add_argument("--headless", action="store_true", help="Run browsers headless")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Number of simultaneous browser windows",
    )
    parser.add_argument(
        "--min-delay", type=float, default=15.0, help="Minimum delay between grid steps"
    )
    parser.add_argument(
        "--max-delay", type=float, default=60.0, help="Maximum delay between grid steps"
    )
    parser.add_argument(
        "--state-file",
        default="run_state.json",
        help="Path to JSON file tracking progress for resuming",
    )
    args = parser.parse_args()

    if args.store:
        os.environ["MAPS_STORAGE"] = args.store

    asyncio.run(main(args))
