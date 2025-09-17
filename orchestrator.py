import argparse
import asyncio
import csv
import os
import random
from typing import Optional

from playwright.async_api import async_playwright
try:
    from prometheus_client import Counter, Gauge, start_http_server

    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    PROMETHEUS_AVAILABLE = False

    class _NoopMetric:
        def __init__(self, *args, **kwargs) -> None:  # noqa: D401 - stub
            """Fallback metric that ignores all operations."""

        def inc(self, *args, **kwargs) -> None:  # noqa: D401 - stub
            """Increment no-op."""

        def dec(self, *args, **kwargs) -> None:  # noqa: D401 - stub
            """Decrement no-op."""

        def set(self, *args, **kwargs) -> None:  # noqa: D401 - stub
            """Set no-op."""

    class Counter(_NoopMetric):
        pass

    class Gauge(_NoopMetric):
        pass

    def start_http_server(*args, **kwargs):  # type: ignore[override]
        raise RuntimeError(
            "Prometheus metrics requested but prometheus_client is not installed."
        )

from db import get_dsn
from obfuscation import BrowserIdentity, create_identity_pool
from scraper import scrape_city_grid
from state_manager import StateManager, load_state
from storage_manager import BusinessStore


TERMS_PROCESSED = Counter(
    "mapmonkey_terms_processed_total",
    "Number of search terms fully processed",
)
BUSINESSES_SAVED = Counter(
    "mapmonkey_businesses_saved_total",
    "Number of unique businesses stored",
)
ACTIVE_WORKERS = Gauge(
    "mapmonkey_active_workers",
    "Number of workers actively scraping",
)


def load_list(path: str) -> list[str]:
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        return [
            ", ".join(part.strip() for part in row if part.strip())
            for row in reader
            if any(part.strip() for part in row)
        ]


async def run_city(city: str, terms: list[str], state_mgr: StateManager, args) -> None:
    default_launch_args = [
        f"--window-size={args.screen_width},{args.screen_height}",
        "--window-position=0,0",
    ]

    async with async_playwright() as p:
        queue: asyncio.Queue[str] = asyncio.Queue()
        start_index = state_mgr.state.get("term_index", 0)
        for term in terms[start_index:]:
            queue.put_nowait(term)

        browser_contexts = []
        pages = []
        for _ in range(args.concurrency):
            identity: Optional[BrowserIdentity] = None
            launch_args = list(default_launch_args)
            if args.obfuscate:
                identity = args.identity_pool.sample(args.identity_rng)
                width, height = identity.window_size()
                launch_args = [f"--window-size={width},{height}"]
                if not args.headless:
                    offset_x = args.identity_rng.randint(0, 300)
                    offset_y = args.identity_rng.randint(0, 300)
                    launch_args.append(f"--window-position={offset_x},{offset_y}")

            browser = await p.chromium.launch(headless=args.headless, args=launch_args)
            context_kwargs = identity.to_context_kwargs() if identity else {}
            context = await browser.new_context(**context_kwargs)
            if identity:
                await context.add_init_script(identity.init_script())
            page = await context.new_page()
            browser_contexts.append((browser, context))
            pages.append(page)

        async def worker(worker_id: int, page):
            store = BusinessStore(args.dsn)
            try:
                while True:
                    try:
                        term = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        await state_mgr.clear_worker(worker_id)
                        await state_mgr.clear_batch(worker_id)
                        break

                    await state_mgr.assign_worker(worker_id, city, term)
                    ACTIVE_WORKERS.inc()
                    search = f"\"{city}\" {term}".strip()
                    context = {"city": city, "term": term, "query": search}
                    async def on_progress(fill: int, total: int) -> None:
                        await state_mgr.update_batch(worker_id, fill, total)

                    async def on_heartbeat() -> None:
                        await state_mgr.worker_heartbeat(worker_id)

                    async def on_event(level: str, message: str, context: Optional[dict] = None) -> None:
                        payload = dict(context or {})
                        payload.setdefault("city", city)
                        payload.setdefault("term", term)
                        payload.setdefault("query", search)
                        await state_mgr.record_event(level, message, worker_id=worker_id, context=payload)

                    async def on_business(records, ctx):
                        if not records:
                            return
                        merged_context = dict(context)
                        merged_context.update(ctx or {})
                        BUSINESSES_SAVED.inc(len(records))
                        await state_mgr.record_business_batch(worker_id, merged_context, records)

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
                            store=store,
                            context=context,
                            progress_cb=on_progress,
                            heartbeat_cb=on_heartbeat,
                            event_cb=on_event,
                            business_cb=on_business,
                        )
                    except Exception as exc:  # noqa: BLE001
                        await on_event("error", f"Error processing term: {exc}")
                    finally:
                        await state_mgr.clear_batch(worker_id)
                        await state_mgr.clear_worker(worker_id)
                        await state_mgr.increment_term()
                        TERMS_PROCESSED.inc()
                        ACTIVE_WORKERS.dec()

            finally:
                store.close()

        await asyncio.gather(*(worker(i, page) for i, page in enumerate(pages)))

        for browser, context in browser_contexts:
            await context.close()
            await browser.close()


async def main(args) -> None:
    args.dsn = get_dsn(args.dsn)
    cities = load_list(args.cities_file)
    terms = load_list(args.terms_file)

    state = load_state(args.state_file)
    state["total_cities"] = len(cities)
    state["total_terms"] = len(terms)
    state["overall_total"] = state["total_cities"] * state["total_terms"]
    state["overall_progress"] = (
        state.get("city_index", 0) * state["total_terms"] + state.get("term_index", 0)
    )

    state_mgr = StateManager(args.state_file, state, flush_interval=args.flush_interval)
    await state_mgr.flush(force=True)

    for idx in range(state.get("city_index", 0), len(cities)):
        city = cities[idx]
        await state_mgr.start_city(idx, city)
        try:
            await run_city(city, terms, state_mgr, args)
        except Exception as exc:  # noqa: BLE001
            await state_mgr.record_event("error", f"Error processing city '{city}': {exc}")
            break
        await state_mgr.next_city(idx + 1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Google Maps searches across multiple terms for each city",
    )
    parser.add_argument("--cities-file", default="cities.csv")
    parser.add_argument("--terms-file", default="terms.csv")
    parser.add_argument("--steps", type=int, default=0)
    parser.add_argument("--spacing-deg", type=float, default=0.02)
    parser.add_argument("--per-grid-total", type=int, default=50)
    parser.add_argument("--dsn", help="Database DSN or path")
    parser.add_argument("--screen-width", type=int, default=1920)
    parser.add_argument("--screen-height", type=int, default=1080)
    parser.add_argument("--store", choices=["postgres", "cassandra", "sqlite", "csv"], help="Storage backend")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--obfuscate", action="store_true")
    parser.add_argument("--profile-file")
    parser.add_argument("--profile-seed", type=int)
    parser.add_argument("--min-delay", type=float, default=15.0)
    parser.add_argument("--max-delay", type=float, default=60.0)
    parser.add_argument("--state-file", default="run_state.json")
    parser.add_argument("--metrics-port", type=int, help="Expose Prometheus metrics on this port")
    parser.add_argument("--flush-interval", type=float, default=1.0, help="State flush interval in seconds")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.store:
        os.environ["MAPS_STORAGE"] = args.store

    args.identity_pool = create_identity_pool(args.profile_file)
    if args.profile_seed is None:
        args.identity_rng = random.SystemRandom()
    else:
        args.identity_rng = random.Random(args.profile_seed)

    if args.metrics_port:
        if not PROMETHEUS_AVAILABLE:
            raise SystemExit(
                "--metrics-port requires prometheus_client. Install it via 'pip install prometheus-client'."
            )
        start_http_server(args.metrics_port)

    asyncio.run(main(args))
