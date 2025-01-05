#!/bin/bash

set -eu
shellcheck -x "$0"

source .env

main () {
    local input
	local i
    local current_num_next_hops
    local total_num_next_hops=0

	if [[ $# -ne 1 ]]; then
		echo "$0: missing argument"
		return 1
	fi
	input="$1"

	i=0
	while read -r table; do
        current_num_next_hops=$(./consumerctl cnh "$table" -w 10 -p "$DEV_CH_PASSWD" -u "$DEV_CH_USER")
        
        total_num_next_hops=$((total_num_next_hops + current_num_next_hops))

        echo "${table}" "${current_num_next_hops}" "${total_num_next_hops}"

		i=$((i + 1))
	done < "$input"
}

main "$@"
