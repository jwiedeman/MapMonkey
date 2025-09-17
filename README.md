# Google Maps Scraper Worker

This worker scrapes business listings from Google Maps using Playwright.
Results can be written to Postgres, Cassandra, a local SQLite file or a
CSV depending on the configured storage backend. Each record notes the search
term and the GPS coordinates where it was collected.

## Usage

Install Python 3.11+ and the dependencies. It's recommended to use a virtual
environment so the Cassandra driver is installed for the correct interpreter.
The driver currently supports up to Python 3.12.

```bash
python3.12 -m venv venv  # or python3.11
source venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and modify values as needed for your environment.
Sample `cities.csv` and `terms.csv` files with a few entries are included in the
repository. Replace their contents with your full datasets (one value per line)
before running the scraper.

The scraper now defaults to Cassandra. Choose between `cassandra`, `postgres`,
`sqlite` or `csv` using the `MAPS_STORAGE` environment variable or the `--store`
option. When using Postgres set the connection string with the `POSTGRES_DSN`
environment variable. The Cassandra driver is required when the storage mode is
set to `cassandra`.

When using Cassandra you can configure connection parameters with the following
environment variables:

- `CASSANDRA_CONTACT_POINTS` – comma separated list of hosts
- `CASSANDRA_PORT` – port number (default `9042`)
- `CASSANDRA_KEYSPACE` – keyspace name (default `maps`)
- `CASSANDRA_LOCAL_DATA_CENTER` – data center name (default `datacenter1`)

## Running searches

`orchestrator.py` focuses on one city at a time but can open several browser
windows to divide the search terms among them. All terms for the first city are
processed before moving on to the next, making it easier to resume or skip
problematic cities. Progress is stored in `run_state.json` so interrupted runs
can pick up where they left off. Errors on individual terms are logged and the
remaining terms continue so a single failure doesn't abort a city.

A lightweight dashboard (`dashboard.html`) and API server (`monitor_server.py`)
are included to monitor a running scrape. The server reads `run_state.json`
and the active datastore (SQLite, Postgres, Cassandra or CSV) to expose compact
JSON endpoints that power the dashboard without repeatedly downloading the full
database. Start it from the repository root and browse to the reported URL:

```bash
python monitor_server.py --port 8080
```

The refreshed dashboard highlights worker heartbeats, stuck-worker alerts,
batch progress, per-city/per-query leaderboards and recent inserts without
requiring any browser extensions.

Provide city and term lists in CSV files (one value per line) and use
`--concurrency` to control the number of concurrent windows. The scraper no
longer has a practical upper bound—run dozens of workers if your hardware can
keep up. By default the script reads from `cities.csv` and `terms.csv` in the
repository root.

```bash
python orchestrator.py --cities-file cities.csv --terms-file terms.csv --steps 0 --concurrency 3
```

Windows open in non‑headless mode so you can watch progress. Use `--headless`
to run the browsers without a visible window. Each browser works through a
subset of the terms for the current city until all have completed, after which
the next city begins. Specify `--state-file` to store the run state at a
different path or delete the file to start from the beginning.

### Monitoring and metrics

Expose Prometheus metrics with `--metrics-port <port>`; counters for processed
terms, saved businesses and the current number of active workers become
available under `/metrics`. The monitoring server described above also surfaces
the same state information over REST for custom tooling.

### Experimental crawler

`experimental_crawler.py` contains an alternate implementation that collects
listings directly from the left-hand results pane (avoiding per-card page
navigations) and experiments with adaptive scrolling heuristics. It reuses the
new storage pipeline so results still flow through the same deduplication
logic. Treat it as an opt-in playground when iterating on scraping strategies.

The module now exposes a lightweight CLI so you can launch experiments the same
way as the primary orchestrator. Provide a single city/term directly or point it
at the familiar CSV lists:

```bash
python experimental_crawler.py --city "Seattle, WA" --term coffee --steps 1 --spacing-deg 0.02 --per-grid-total 40
```

Use `--cities-file` / `--terms-file` to iterate through full lists. The crawler
works through each city/term combination sequentially. A visible browser window
opens by default; add `--headless` to run without a UI. Pass `--store` if you
prefer to override the storage backend without setting environment variables.

```bash
python experimental_crawler.py --cities-file cities.csv --terms-file terms.csv --steps 2 --spacing-deg 0.025 --per-grid-total 60
```

#### Parameters at a glance

- `city` / `term` – the geographic focus and search keyword. The query sent to
  Google Maps becomes `"{city}" {term}` (quotes only surround the city).
- `steps` – how many grid cells to probe in each direction from the city
  centre. A value of `1` scrapes a 3×3 grid, `2` a 5×5 grid, and so on.
- `spacing` – distance in degrees between neighbouring grid cells.
- `per_grid_total` – maximum number of business cards to collect for each grid
  coordinate before moving on.
- `dsn` – connection string or path for the configured storage backend. When
  omitted the same environment variables as the main scraper apply.
- `min_delay` / `max_delay` – randomised pause (seconds) between grid cells.
- `launch_args` – optional additional Chromium launch flags (for example
  proxies).
- `headless` – hide the browser window (`--headless` flag).

All storage backends supported by the primary scraper are available here as
well. Pick one via the `MAPS_STORAGE` environment variable (defaults to
`sqlite`) and provide any DSNs using the same variables (`POSTGRES_DSN`,
`SQLITE_PATH`, `CSV_PATH`, etc.). Alternatively pass `--store` to the CLI to set
the backend for a single run.

#### Example invocations

Collect a small sample for a single city/term pair and write it to the default
SQLite database:

```bash
MAPS_STORAGE=sqlite python experimental_crawler.py --city "Seattle, WA" --term coffee --steps 1 --spacing-deg 0.02 --per-grid-total 40
```

Store directly to Postgres by switching the backend and providing a DSN. This
example also widens the grid:

```bash
MAPS_STORAGE=postgres POSTGRES_DSN="dbname=maps user=postgres host=localhost" \
python experimental_crawler.py --city "Austin, TX" --term roofing --steps 2 --spacing-deg 0.03 --per-grid-total 60
```

Additional Chromium arguments such as proxies or feature flags can be supplied
with `--launch-arg`. The following run demonstrates routing through a SOCKS5
proxy, overriding the random delay window and hiding the UI:

```bash
python experimental_crawler.py --city "Berlin, Germany" --term veterinarian --steps 0 --spacing-deg 0.015 --per-grid-total 80 --dsn custom_results.csv --min-delay 5 --max-delay 10 --launch-arg "--proxy-server=socks5://127.0.0.1:1080" --headless
```

The `BusinessStore` deduplication logic is shared with the main scraper, so you
can safely mix experimental and production runs within the same datastore.

### Browser identity rotation

Large numbers of identical browser windows are an easy signal for anti-bot
systems. Enable `--obfuscate` to give every worker a unique user agent,
viewport, locale and timezone. This helps spread work across more than a dozen
concurrent workers without seeing throttling. Fingerprints are generated from a
pool of realistic desktop configurations, and a small stealth script removes
common automation flags such as `navigator.webdriver`.

Optional switches provide further control:

- `--profile-file <path>` – load fingerprints from a JSON or plain text file.
  Plain text files should contain one user agent per line. JSON files must
  contain a list of objects or strings. Example object:

  ```json
  [
    {
      "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.86 Safari/537.36",
      "viewport": {"width": 1920, "height": 1080},
      "locale": "en-US",
      "timezone": "America/New_York"
    }
  ]
  ```

- `--profile-seed <int>` – reuse the same fingerprint sequence for reproducible
  runs when `--obfuscate` is set.

## Local Postgres setup

The workers expect a running Postgres instance.
Ensure the PostgreSQL command line tools (`initdb`, `pg_ctl` and `createdb`)
are installed and available on your `PATH`. On macOS install them with
Homebrew (`brew install postgresql`) and on Debian/Ubuntu use
`sudo apt-get install postgresql`. If the commands aren't on your `PATH` after
installing with Homebrew, add `/usr/local/opt/postgresql/bin` (or
`/opt/homebrew/opt/postgresql/bin` on Apple Silicon) to the `PATH`.

Run `start_postgres.sh` in this folder to initialise and launch the database.
The script automatically checks the common Homebrew locations above when
locating the Postgres tools. It creates a data directory under `pgdata/` on the
first run and starts the server on port `5432` (or `$PGPORT` if set).

```bash
./start_postgres.sh
```

Once the server is running the default DSN `dbname=maps user=postgres host=localhost password=postgres`
will connect successfully. You can also set a custom connection string via the
`POSTGRES_DSN` environment variable when invoking the workers.

## Exporting to Excel

`export_to_excel.py` can convert a Postgres database to an Excel file:

```bash
python export_to_excel.py "dbname=maps user=postgres host=localhost password=postgres" results.xlsx
```

## Importing existing SQLite databases

`import_sqlite_to_cassandra.py` copies any `*.db` files in this folder into the
Cassandra `maps.businesses` table. Run it once after collecting data locally:

```bash
python import_sqlite_to_cassandra.py
```

## Docker Swarm

This image can also run as a service in Docker Swarm after being built and pushed to your registry.

```bash
docker service create --name <service-name> --env-file .env <image>:latest
```

Alternatively include the service in a stack file and deploy with `docker stack deploy`.
