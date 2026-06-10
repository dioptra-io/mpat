#!/bin/bash
set -uo pipefail

# Fetches Iris results for each day between May 27 and June 9, 2026.
# For each day, fetches ipv6 (index 0) then zeph (indices 0–3).
# On failure, logs the error and continues to the next command.
#
# Usage:
#   nohup ./fetch_iris_may.sh >> fetch_may.log 2>&1 &
#   ./fetch_iris_may.sh --dry-run

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
	DRY_RUN=true
	echo "[dry-run] no commands will be executed"
fi

days=(
	"2026-05-27"
	"2026-05-28"
	"2026-05-29"
	"2026-05-30"
	"2026-05-31"
	"2026-06-01"
	"2026-06-02"
	"2026-06-03"
	"2026-06-04"
	"2026-06-05"
	"2026-06-06"
	"2026-06-07"
	"2026-06-08"
	"2026-06-09"
)

run() {
	local kind="$1"
	local index="$2"
	local date="$3"
	local table="iris_${kind}_${index}__resultslite__${date//-/}"

	local cmd="mp fetch iris-results $table --date $date --kind $kind --index $index --lite=true --policy fail"

	if [[ "$DRY_RUN" == "true" ]]; then
		echo "[dry-run] $cmd"
		return
	fi

	echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] starting: $table"
	if mp fetch iris-results "$table" \
		--date "$date" \
		--kind "$kind" \
		--index "$index" \
		--lite=true \
		--policy fail; then
		echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] done: $table"
	else
		echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] failed: $table (continuing)"
	fi
}

for date in "${days[@]}"; do
	run "ipv6" 0 "$date"
	for index in 0 1 2 3; do
		run "zeph" "$index" "$date"
	done
done
