#!/usr/bin/env bash

set -euo pipefail

TABLES=()
tablesfile=""
jsonlfile=""
DRYRUN=false

mpat_command() {
    if $DRYRUN; then
        log "[dry-run] mpat $*"
        sleep 1
    else
        mpat "$@" --silent >> "${jsonlfile}" || true
    fi
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

process_for_table() {
    local stem="$1"

    log "Processing ${stem}"
    mpat_command metrics "${stem}"
    local command_status=$?

    if [[ $command_status -ne 0 ]]; then
        log "${stem} failed with code ${command_status}" >&2
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
    
    tablesfile="${positional[0]}"
    jsonlfile="${positional[1]}"
}

read_dates() {
    while IFS= read -r line; do
        [[ -n "$line" ]] && TABLES+=("$line")
    done < "$tablesfile"
}

check_mpat_command() {
    if ! command -v mpat &> /dev/null; then
        log "Error: 'mpat' command not found." >&2
        return 1
    fi
}

print_help() {
    cat <<EOF
Usage: $0 <stemfile> <jsonlfile> [--dry-run]

Arguments:
  stemfile                                  Required. Path to file with one stem per line.
  jsonlfile                                 Required. Path to the output file format jsonl.
  --dry-run                                 Optional. Simulates commands instead of running them.
  -h, --help                                Show this help message.

Example:
  $0 stems.txt output.jsonl --dry-run
EOF
}

main() {
    parse_args "$@"
    read_dates

    for stem in "${TABLES[@]}"; do
        process_for_table "$stem"
    done

    log "All processes completed"
}

main "$@"
