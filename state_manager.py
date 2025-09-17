import asyncio
import json
import os
import time
from collections import deque
from typing import Any, Dict, Iterable, List, MutableMapping, Optional


class StateManager:
    """Coordinate shared run_state.json updates with throttling."""

    def __init__(
        self,
        path: str,
        state: MutableMapping[str, Any],
        *,
        flush_interval: float = 1.0,
        max_events: int = 100,
        max_recent: int = 50,
    ) -> None:
        self.path = path
        self.state = state
        self.flush_interval = flush_interval
        self.max_events = max_events
        self.max_recent = max_recent
        self._lock = asyncio.Lock()
        self._dirty = False
        self._last_flush = 0.0

        # Ensure newer keys exist so older state files can be upgraded lazily.
        self.state.setdefault("workers", {})
        self.state.setdefault("alerts", [])
        self.state.setdefault("events", [])
        self.state.setdefault("recent_businesses", [])
        self.state.setdefault(
            "metrics",
            {
                "businesses_saved": 0,
                "per_city": {},
                "per_query": {},
                "per_worker": {},
            },
        )
        self.state.setdefault("batch", {"fill": 0, "total": 0, "worker": None})
        self.state.setdefault("current_city", None)

    async def _maybe_flush_locked(self, force: bool = False) -> None:
        now = time.monotonic()
        if not self._dirty and not force:
            return
        if not force and now - self._last_flush < self.flush_interval:
            return
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)
        self._dirty = False
        self._last_flush = now

    async def flush(self, *, force: bool = False) -> None:
        async with self._lock:
            await self._maybe_flush_locked(force=force)

    async def assign_worker(self, worker_id: int, city: str, term: str) -> None:
        async with self._lock:
            workers = self.state.setdefault("workers", {})
            workers[str(worker_id)] = {
                "city": city,
                "term": term,
                "assigned_at": time.time(),
                "heartbeat": time.time(),
            }
            self._dirty = True
            await self._maybe_flush_locked()

    async def clear_worker(self, worker_id: int) -> None:
        async with self._lock:
            workers = self.state.setdefault("workers", {})
            workers.pop(str(worker_id), None)
            self._dirty = True
            await self._maybe_flush_locked()

    async def worker_heartbeat(self, worker_id: int) -> None:
        async with self._lock:
            worker = self.state.setdefault("workers", {}).get(str(worker_id))
            if worker is None:
                return
            worker["heartbeat"] = time.time()
            self._dirty = True
            await self._maybe_flush_locked()

    async def update_batch(self, worker_id: int, fill: int, total: int) -> None:
        async with self._lock:
            self.state.setdefault("batch", {})
            self.state["batch"].update(
                {
                    "fill": fill,
                    "total": total,
                    "worker": str(worker_id),
                    "updated_at": time.time(),
                }
            )
            # Treat batch updates as heartbeats as well.
            worker = self.state.setdefault("workers", {}).get(str(worker_id))
            if worker is not None:
                worker["heartbeat"] = time.time()
            self._dirty = True
            await self._maybe_flush_locked()

    async def clear_batch(self, worker_id: int) -> None:
        async with self._lock:
            batch = self.state.setdefault("batch", {})
            if batch.get("worker") == str(worker_id):
                batch.update({"fill": 0, "total": 0, "worker": None})
                self._dirty = True
                await self._maybe_flush_locked()

    async def increment_term(self) -> None:
        async with self._lock:
            self.state["term_index"] = self.state.get("term_index", 0) + 1
            total_terms = self.state.get("total_terms", 0)
            self.state["overall_progress"] = (
                self.state.get("city_index", 0) * total_terms
                + self.state.get("term_index", 0)
            )
            self._dirty = True
            await self._maybe_flush_locked()

    async def next_city(self, city_index: int) -> None:
        async with self._lock:
            self.state["city_index"] = city_index
            self.state["term_index"] = 0
            self.state["current_city"] = None
            total_terms = self.state.get("total_terms", 0)
            self.state["overall_progress"] = city_index * total_terms
            self._dirty = True
            await self._maybe_flush_locked(force=True)

    async def start_city(self, city_index: int, city_name: str) -> None:
        async with self._lock:
            self.state["city_index"] = city_index
            self.state["current_city"] = city_name
            self._dirty = True
            await self._maybe_flush_locked()

    async def record_event(
        self,
        level: str,
        message: str,
        *,
        worker_id: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = {
            "level": level,
            "message": message,
            "timestamp": time.time(),
        }
        if worker_id is not None:
            payload["worker"] = str(worker_id)
        if context:
            payload.update(context)

        async with self._lock:
            events: List[Dict[str, Any]] = self.state.setdefault("events", [])
            events.append(payload)
            if len(events) > self.max_events:
                del events[: len(events) - self.max_events]
            if level.lower() in {"error", "warning"}:
                alerts: List[Dict[str, Any]] = self.state.setdefault("alerts", [])
                alerts.append(payload)
                if len(alerts) > self.max_events:
                    del alerts[: len(alerts) - self.max_events]
            self._dirty = True
            await self._maybe_flush_locked()

    async def record_business_batch(
        self,
        worker_id: int,
        context: Dict[str, Any],
        records: Iterable[Dict[str, Any]],
    ) -> None:
        records_list = list(records)
        if not records_list:
            return

        city = context.get("city")
        query = context.get("query")
        term = context.get("term")

        async with self._lock:
            metrics = self.state.setdefault("metrics", {})
            metrics.setdefault("businesses_saved", 0)
            metrics["businesses_saved"] += len(records_list)

            per_city: Dict[str, int] = metrics.setdefault("per_city", {})
            if city:
                per_city[city] = per_city.get(city, 0) + len(records_list)

            per_query: Dict[str, int] = metrics.setdefault("per_query", {})
            if query:
                per_query[query] = per_query.get(query, 0) + len(records_list)

            per_worker: Dict[str, int] = metrics.setdefault("per_worker", {})
            per_worker[str(worker_id)] = per_worker.get(str(worker_id), 0) + len(
                records_list
            )

            recent: deque = deque(
                self.state.setdefault("recent_businesses", []), maxlen=self.max_recent
            )
            for record in records_list:
                entry = dict(record)
                entry.setdefault("saved_at", time.time())
                if city and "city" not in entry:
                    entry["city"] = city
                if term and "term" not in entry:
                    entry["term"] = term
                if query and "query" not in entry:
                    entry["query"] = query
                recent.append(entry)
            self.state["recent_businesses"] = list(recent)

            self._dirty = True
            await self._maybe_flush_locked()


def load_state(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            state = json.load(f)
    else:
        state = {"city_index": 0, "term_index": 0}
    state.setdefault("total_cities", 0)
    state.setdefault("total_terms", 0)
    state.setdefault("overall_progress", 0)
    state.setdefault("overall_total", 0)
    state.setdefault("workers", {})
    state.setdefault("batch", {"fill": 0, "total": 0, "worker": None})
    state.setdefault("alerts", [])
    state.setdefault("events", [])
    state.setdefault("recent_businesses", [])
    state.setdefault(
        "metrics",
        {
            "businesses_saved": 0,
            "per_city": {},
            "per_query": {},
            "per_worker": {},
        },
    )
    state.setdefault("current_city", None)
    return state
