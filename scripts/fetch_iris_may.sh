#!/bin/bash
set -uo pipefail

# Fetches Iris results for each day in May 2026.
# For each day, fetches ipv6 first, then zeph.
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
	"2026-05-01" "2026-05-02"
	"2026-05-02" "2026-05-03"
	"2026-05-03" "2026-05-04"
	"2026-05-04" "2026-05-05"
	"2026-05-05" "2026-05-06"
	"2026-05-06" "2026-05-07"
	"2026-05-07" "2026-05-08"
	"2026-05-08" "2026-05-09"
	"2026-05-09" "2026-05-10"
	"2026-05-10" "2026-05-11"
	"2026-05-11" "2026-05-12"
	"2026-05-12" "2026-05-13"
	"2026-05-13" "2026-05-14"
	"2026-05-14" "2026-05-15"
	"2026-05-15" "2026-05-16"
	"2026-05-16" "2026-05-17"
	"2026-05-17" "2026-05-18"
	"2026-05-18" "2026-05-19"
	"2026-05-19" "2026-05-20"
	"2026-05-20" "2026-05-21"
	"2026-05-21" "2026-05-22"
	"2026-05-22" "2026-05-23"
	"2026-05-23" "2026-05-24"
	"2026-05-24" "2026-05-25"
	"2026-05-25" "2026-05-26"
	"2026-05-26" "2026-05-27"
	"2026-05-27" "2026-05-28"
	"2026-05-28" "2026-05-29"
	"2026-05-29" "2026-05-30"
	"2026-05-30" "2026-05-31"
	"2026-05-31" "2026-06-01"
)

run() {
	local tag="$1"
	local date="$2"
	local next_date="$3"
	local table="iris${tag}__resultslite__${date//-/}"

	local cmd="mp fetch iris-results $table --from ${date}T00:00:00Z --to ${next_date}T00:00:00Z --tag $tag --lite=true --policy fail"

	if [[ "$DRY_RUN" == "true" ]]; then
		echo "[dry-run] $cmd"
		return
	fi

	echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] starting: $table"
	if mp fetch iris-results "$table" \
		--from "${date}T00:00:00Z" \
		--to "${next_date}T00:00:00Z" \
		--tag "$tag" \
		--lite=true \
		--policy fail; then
		echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] done: $table"
	else
		echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] failed: $table (continuing)"
	fi
}

for ((i = 0; i < ${#days[@]}; i += 2)); do
	date="${days[$i]}"
	next_date="${days[$i + 1]}"
	run "ipv6" "$date" "$next_date"
	run "zeph" "$date" "$next_date"
done
