#!/usr/bin/env bash

set -euo pipefail

DATES=("2025-06-02" "2025-06-03" "2025-06-04" "2025-06-05" "2025-06-06" "2025-06-07" "2025-06-08")
DRYRUN=false

mpat_command() {
    if $DRYRUN; then
        echo "[dry-run] mpat upload $*"
        sleep 1
    else
        mpat "$@"
    fi
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

upload_for_date() {
    local date="$1"
    local datestr="${date//-/}"

    mpat_command upload iris-results "$date" "iris4__${datestr}" &
    local iris_pid=$!

    mpat_command upload ark-results "$date" "ark4__${datestr}" &
    local ark_pid=$!

    log "Started uploading tables iris4__${datestr} (pid: ${iris_pid}) and ark4__${datestr} (pid: ${ark_pid})"

    wait "$iris_pid"
    local iris_status=$?
    if [[ $iris_status -ne 0 ]]; then
        log "iris4__${datestr} failed with code ${iris_status}" >&2
    fi

    wait "$ark_pid"
    local ark_status=$?
    if [[ $ark_status -ne 0 ]]; then
        log "ark4__${datestr} failed with code ${ark_status}" >&2
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --dry-run)
                DRYRUN=true
                shift
                ;;
            *)
                echo "Unknown argument: $1" >&2
                exit 1
                ;;
        esac
    done
}

main() {
    parse_args "$@"

    if ! command -v mpat &> /dev/null; then
        echo "Error: 'mpat' command not found." >&2
        return 1
    fi

    for date in "${DATES[@]}"; do
        upload_for_date "$date"
    done

    wait
    log "All uploads completed"
}

main "$@"