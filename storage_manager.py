from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional

from db import (
    close_db,
    get_dsn,
    get_storage,
    init_db,
    load_business_keys,
    save_business_batch,
)


@dataclass
class BusinessRecord:
    name: str
    address: str
    website: str
    phone: str
    reviews_average: Optional[float]
    query: str
    latitude: Optional[float]
    longitude: Optional[float]

    def as_tuple(self) -> tuple:
        return (
            self.name,
            self.address,
            self.website,
            self.phone,
            self.reviews_average,
            self.query,
            self.latitude,
            self.longitude,
        )

    def as_dict(self) -> Dict[str, Optional[float]]:
        data = asdict(self)
        return data


class BusinessStore:
    """Maintain a connection and dedupe cache for business inserts."""

    def __init__(self, dsn: Optional[str], *, storage: Optional[str] = None) -> None:
        self.storage = get_storage(storage)
        resolved_dsn = get_dsn(dsn)
        self.conn = init_db(resolved_dsn, storage=self.storage)
        # Preload dedupe keys only for storage engines where it is cheap.
        self._preload_complete = self.storage in {"sqlite", "csv"}
        self._seen_keys = (
            load_business_keys(self.conn, storage=self.storage)
            if self._preload_complete
            else set()
        )

    def filter_new(self, records: Iterable[BusinessRecord]) -> List[BusinessRecord]:
        fresh: List[BusinessRecord] = []
        for record in records:
            key = (record.name.strip().lower(), record.address.strip().lower())
            if not record.name.strip() or not record.address.strip():
                continue
            if key in self._seen_keys:
                continue
            if not self._preload_complete and self._exists_in_store(record):
                self._seen_keys.add(key)
                continue
            self._seen_keys.add(key)
            fresh.append(record)
        return fresh

    def save_new(self, records: Iterable[BusinessRecord]) -> List[Dict]:
        fresh_records = self.filter_new(records)
        if not fresh_records:
            return []
        tuples = [r.as_tuple() for r in fresh_records]
        save_business_batch(self.conn, tuples, storage=self.storage)
        return [r.as_dict() for r in fresh_records]

    def close(self) -> None:
        close_db(self.conn, storage=self.storage)

    def _exists_in_store(self, record: BusinessRecord) -> bool:
        if self.storage == "postgres":
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM businesses WHERE name = %s AND address = %s LIMIT 1",
                    (record.name, record.address),
                )
                return cur.fetchone() is not None
        if self.storage == "cassandra":
            rows = self.conn.execute(
                "SELECT name FROM businesses WHERE name = %s AND address = %s LIMIT 1",
                (record.name, record.address),
            )
            return any(rows)
        return False
