#!/bin/bash
set -uo pipefail

# Fetches RIPE BGP prefixes for each day in May 2026.
# For each day, fetches dawn, day, and night snapshots using the tier-1 ASN list.
# On failure, logs the error and continues to the next command.
#
# Usage:
#   nohup ./fetch_ripe_may.sh >> fetch_ripe_may.log 2>&1 &
#   ./fetch_ripe_may.sh --dry-run

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
	DRY_RUN=true
	echo "[dry-run] no commands will be executed"
fi

days=(
	"2026-05-01"
	"2026-05-02"
	"2026-05-03"
	"2026-05-04"
	"2026-05-05"
	"2026-05-06"
	"2026-05-07"
	"2026-05-08"
	"2026-05-09"
	"2026-05-10"
	"2026-05-11"
	"2026-05-12"
	"2026-05-13"
	"2026-05-14"
	"2026-05-15"
	"2026-05-16"
	"2026-05-17"
	"2026-05-18"
	"2026-05-19"
	"2026-05-20"
	"2026-05-21"
	"2026-05-22"
	"2026-05-23"
	"2026-05-24"
	"2026-05-25"
	"2026-05-26"
	"2026-05-27"
	"2026-05-28"
	"2026-05-29"
	"2026-05-30"
	"2026-05-31"
)

run() {
	local snapshot="$1"
	local date="$2"
	local table="ripeprefixes_tier1_${snapshot}__${date//-/}"
	local cmd="mp fetch ripe-prefixes $table --tier1 --date $date --snapshot $snapshot --policy fail"

	if [[ "$DRY_RUN" == "true" ]]; then
		echo "[dry-run] $cmd"
		return
	fi

	echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] starting: $table"
	if mp fetch ripe-prefixes "$table" \
		--tier1 \
		--date "$date" \
		--snapshot "$snapshot" \
		--policy fail; then
		echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] done: $table"
	else
		echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] failed: $table (continuing)"
	fi
}

for date in "${days[@]}"; do
	run "dawn" "$date"
	run "day" "$date"
	run "night" "$date"
done
