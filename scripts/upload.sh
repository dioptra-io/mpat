#!/usr/bin/env bash

set -euo pipefail

DATES=("2025-06-02" "2025-06-03" "2025-06-04" "2025-06-05" "2025-06-06" "2025-06-07")

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

upload_for_date() {
    local date="$1"
    local datestr="${date//-/}"

    log "Starting to upload for $date"

    if mpat upload iris-results "$date" "iris4__${datestr}"; then
        log "Started iris upload for $date"
    else
        log "Failed to start iris upload for $date" >&2
    fi

    if mpat upload ark-results "$date" "ark4__${datestr}"; then
        log "Started ark upload for $date"
    else
        log "Failed to start ark upload for $date" >&2
    fi
}

main() {
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
