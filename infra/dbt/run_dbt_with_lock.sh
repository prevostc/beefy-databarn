#!/bin/sh
# Run dbt under an exclusive file lock so only one dbt run executes at a time.
# Prevents scheduler runs and manual "make dbt run" from clashing on DB/artifacts.
# Usage: run_dbt_with_lock.sh <dbt_subcommand> [args...]
#   e.g. run_dbt_with_lock.sh run
#   e.g. run_dbt_with_lock.sh run --select my_model
#   e.g. run_dbt_with_lock.sh test
#
# Uses flock -x [ -w timeout ] file command [args] (util-linux; no -- separator).

LOCKFILE="${DBT_RUN_LOCKFILE:-/app/dbt/.dbt_run.lock}"
# Max wait in seconds (default 15min); 0 = wait forever
LOCK_TIMEOUT="${DBT_RUN_LOCK_TIMEOUT:-30}"

# Ensure lock file directory exists (e.g. if DBT_RUN_LOCKFILE points elsewhere)
mkdir -p "$(dirname "$LOCKFILE")"

cd /app/dbt
export DBT_PROFILES_DIR=/app/dbt
export DBT_PROJECT_DIR=/app/dbt

# flock syntax: flock [options] file command [arguments] (no -- separator in util-linux)
if [ "$LOCK_TIMEOUT" -gt 0 ]; then
  echo "flock -x -w $LOCK_TIMEOUT $LOCKFILE uv run dbt $@"
  flock -x -w "$LOCK_TIMEOUT" "$LOCKFILE" uv run dbt "$@"
else
  echo "flock -x $LOCKFILE uv run dbt $@"
  flock -x "$LOCKFILE" uv run dbt "$@"
fi
