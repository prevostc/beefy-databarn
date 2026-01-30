#!/bin/sh
# Run dbt under an exclusive file lock so only one dbt run executes at a time.
# Prevents scheduler runs and manual "make dbt run" from clashing on DB/artifacts.
# Usage: run_dbt_with_lock.sh <dbt_subcommand> [args...]
#   e.g. run_dbt_with_lock.sh run
#   e.g. run_dbt_with_lock.sh run --select my_model
#   e.g. run_dbt_with_lock.sh test
#
# Uses fd-based flock (subshell) so it works with both GNU flock and BusyBox flock;
# the "flock ... -- cmd" form can fail with "failed to execute --" on some systems.

LOCKFILE="${DBT_RUN_LOCKFILE:-/app/dbt/.dbt_run.lock}"
# Max wait in seconds (default 15min); 0 = wait forever
LOCK_TIMEOUT="${DBT_RUN_LOCK_TIMEOUT:-900}"

# Ensure lock file directory exists (e.g. if DBT_RUN_LOCKFILE points elsewhere)
mkdir -p "$(dirname "$LOCKFILE")"

cd /app/dbt
export DBT_PROFILES_DIR=/app/dbt
export DBT_PROJECT_DIR=/app/dbt

if [ "$LOCK_TIMEOUT" -gt 0 ]; then
  (
    flock -x -w "$LOCK_TIMEOUT" 200
    exec uv run dbt "$@"
  ) 200>"$LOCKFILE"
else
  (
    flock -x 200
    exec uv run dbt "$@"
  ) 200>"$LOCKFILE"
fi
