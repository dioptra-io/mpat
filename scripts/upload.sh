#!/usr/bin/env bash

set -euo pipefail

DATES=()
DATEFILE=""
DRYRUN=false
MODE="iris"

mpat_command() {
    if $DRYRUN; then
        log "[dry-run] mpat $*"
        sleep 1
    else
        mpat "$@" || true
    fi
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

upload_for_date() {
    local date="$1"
    local datestr="${date//-/}"

    log "Uploading ${MODE}4__${datestr}..."
    mpat_command upload "${MODE}-results" "$date" "${MODE}4__${datestr}"
    local command_status=$?

    if [[ $command_status -ne 0 ]]; then
        log "${MODE}4__${datestr} failed with code ${command_status}" >&2
    fi
}

parse_args() {
    DRYRUN=false

    for arg in "$@"; do
        case "$arg" in
            -h|--help)
                print_help
                exit 0
                ;;
        esac
        case "$arg" in
            --dry-run)
                DRYRUN=true
                ;;
        esac
    done

    local positional=()
    for arg in "$@"; do
        if [[ "$arg" != -* ]]; then
            positional+=("$arg")
        fi
    done

    if [[ ${#positional[@]} -lt 2 ]]; then
        log "Error: not enough arguments" >&2
        exit 1
    fi

    if [[ "$MODE" != "ark" && "$MODE" != "iris" ]]; then
        log "Error: invalid mode '$MODE'. Expected 'ark' or 'iris'." >&2
        exit 1
    fi
    
    MODE="${positional[0]}"
    DATEFILE="${positional[1]}"
}

read_dates() {
    while IFS= read -r line; do
        [[ -n "$line" ]] && DATES+=("$line")
    done < "$DATEFILE"
}

check_mpat_command() {
    if ! command -v mpat &> /dev/null; then
        log "Error: 'mpat' command not found." >&2
        return 1
    fi
}

print_help() {
    cat <<EOF
Usage: $0 <ark|iris> <datefile> [--dry-run]

Arguments:
  ark|iris       Required. Mode to use.
  datefile       Required. Path to file with one date per line.
  --dry-run      Optional. Simulates commands instead of running them.
  -h, --help     Show this help message.

Example:
  $0 iris ./dates.txt --dry-run
EOF
}

main() {
    parse_args "$@"
    read_dates

    for date in "${DATES[@]}"; do
        upload_for_date "$date"
    done

    log "All uploads completed"
}

main "$@"