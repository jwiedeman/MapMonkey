import argparse
import asyncio
import csv
import os
import random
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Optional

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


@dataclass
class WorkerSlot:
    browser: Any
    context: Any
    page: Any
    task: Optional[asyncio.Task] = None
    current_term: Optional[str] = None
    last_heartbeat: float = field(default_factory=time.monotonic)


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

        worker_slots: dict[int, WorkerSlot] = {}
        active_tasks: set[asyncio.Task] = set()
        shutting_down = False

        async def worker(worker_id: int, slot: WorkerSlot) -> None:
            page = slot.page
            store = BusinessStore(args.dsn)
            try:
                while True:
                    try:
                        term = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        await state_mgr.clear_worker(worker_id)
                        await state_mgr.clear_batch(worker_id)
                        slot.current_term = None
                        slot.last_heartbeat = time.monotonic()
                        break

                    slot.current_term = term
                    slot.last_heartbeat = time.monotonic()
                    await state_mgr.assign_worker(worker_id, city, term)
                    ACTIVE_WORKERS.inc()
                    search = f"\"{city}\" {term}".strip()
                    context = {"city": city, "term": term, "query": search}
                    async def on_progress(fill: int, total: int) -> None:
                        await state_mgr.update_batch(worker_id, fill, total)

                    async def on_heartbeat() -> None:
                        slot.last_heartbeat = time.monotonic()
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

                    term_completed = False
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
                        term_completed = True
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        await on_event("error", f"Error processing term: {exc}")
                        term_completed = True
                    finally:
                        await state_mgr.clear_batch(worker_id)
                        await state_mgr.clear_worker(worker_id)
                        if term_completed:
                            await state_mgr.increment_term()
                            TERMS_PROCESSED.inc()
                        ACTIVE_WORKERS.dec()
                        slot.current_term = None
                        slot.last_heartbeat = time.monotonic()

            finally:
                store.close()

        async def start_worker(worker_id: int, *, reason: Optional[str] = None) -> None:
            if shutting_down:
                return

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

            slot = WorkerSlot(browser=browser, context=context, page=page)
            worker_slots[worker_id] = slot

            async def run_worker() -> None:
                try:
                    await worker(worker_id, slot)
                finally:
                    with suppress(Exception):
                        await context.close()
                    with suppress(Exception):
                        await browser.close()
                    if worker_slots.get(worker_id) is slot:
                        worker_slots.pop(worker_id, None)

            task = asyncio.create_task(run_worker())
            slot.task = task
            active_tasks.add(task)

            def _cleanup(t: asyncio.Task) -> None:
                active_tasks.discard(t)

            task.add_done_callback(_cleanup)

        async def restart_worker(worker_id: int, reason: str) -> None:
            if shutting_down:
                return

            slot = worker_slots.get(worker_id)
            if slot is None:
                if queue.empty():
                    return
                await start_worker(worker_id, reason=reason)
                return

            term = slot.current_term
            context_payload = {"city": city, "reason": reason}
            if term is not None:
                search = f"\"{city}\" {term}".strip()
                context_payload.update({"term": term, "query": search})
            task = slot.task
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            if term is not None:
                await queue.put(term)
            slot.current_term = None
            await state_mgr.record_event(
                "warning",
                f"Restarting worker {worker_id}: {reason}",
                worker_id=worker_id,
                context=context_payload,
            )

            await start_worker(worker_id, reason=reason)

        async def monitor_workers() -> None:
            interval = max(args.worker_check_interval, 1.0)
            while True:
                if queue.empty() and all(slot.current_term is None for slot in worker_slots.values()):
                    return

                if args.worker_timeout > 0:
                    now = time.monotonic()
                    for worker_id, slot in list(worker_slots.items()):
                        if slot.current_term is None:
                            continue
                        if now - slot.last_heartbeat <= args.worker_timeout:
                            continue
                        elapsed = now - slot.last_heartbeat
                        await restart_worker(worker_id, f"no heartbeat for {elapsed:.1f}s")

                await asyncio.sleep(interval)

        monitor_task: Optional[asyncio.Task] = None
        try:
            for worker_id in range(args.concurrency):
                await start_worker(worker_id)

            monitor_task = asyncio.create_task(monitor_workers())
            await monitor_task
        finally:
            shutting_down = True
            if monitor_task is not None and not monitor_task.done():
                monitor_task.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor_task

            for slot in list(worker_slots.values()):
                task = slot.task
                if task is not None and not task.done():
                    task.cancel()

            for task in list(active_tasks):
                with suppress(asyncio.CancelledError):
                    await task


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
    parser.add_argument(
        "--worker-timeout",
        type=float,
        default=240.0,
        help="Seconds without a heartbeat before restarting a worker (0 disables restarts)",
    )
    parser.add_argument(
        "--worker-check-interval",
        type=float,
        default=30.0,
        help="How often to check worker health in seconds",
    )
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
