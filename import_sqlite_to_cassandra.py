import sqlite3
from pathlib import Path
from db import init_db, save_business, close_db


def import_sqlite_files(directory: Path) -> None:
    cassandra_conn = init_db(None, storage="cassandra")
    for db_path in directory.glob("*.db"):
        sqlite_conn = sqlite3.connect(db_path)
        cur = sqlite_conn.execute(
            "SELECT name, address, website, phone, reviews_average, query, latitude, longitude FROM businesses"
        )
        for row in cur:
            save_business(cassandra_conn, row, storage="cassandra")
        sqlite_conn.close()
    close_db(cassandra_conn, storage="cassandra")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Import all SQLite .db files in a folder into the Cassandra maps.businesses table"
    )
    parser.add_argument(
        "path", nargs="?", default=".", help="Folder to search for SQLite files"
    )
    args = parser.parse_args()
    import_sqlite_files(Path(args.path))



