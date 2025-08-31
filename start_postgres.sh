#!/bin/bash
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
PGDATA="$DIR/pgdata"
LOGFILE="$PGDATA/postgres.log"
PGPORT=${PGPORT:-5432}
mkdir -p "$PGDATA"

# Ensure Postgres command line tools are installed
for cmd in initdb pg_ctl createdb; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    # Try common Homebrew locations if command is missing
    for prefix in /usr/local/opt/postgresql /opt/homebrew/opt/postgresql; do
      if [ -x "$prefix/bin/$cmd" ]; then
        PATH="$prefix/bin:$PATH"
        export PATH
        break
      fi
    done
  fi
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: '$cmd' not found. Please install PostgreSQL and ensure it's on your PATH." >&2
    exit 1
  fi
done

if [ ! -f "$PGDATA/PG_VERSION" ]; then
  echo "Initializing database at $PGDATA"
  PWFILE="$PGDATA/pwfile"
  echo "postgres" > "$PWFILE"
  initdb -D "$PGDATA" -U postgres -A password --pwfile="$PWFILE"
  rm "$PWFILE"
fi

pg_ctl -D "$PGDATA" -o "-p $PGPORT" -l "$LOGFILE" start
createdb -h localhost -p "$PGPORT" -U postgres maps 2>/dev/null || true
echo "Postgres running on port $PGPORT with data dir $PGDATA"
