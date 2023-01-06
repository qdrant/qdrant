#!/usr/bin/env bash

set -euo pipefail

# macOS ships with a 15 years old Bash, so you will have to install a more recent version!

function help {
	# This file is indented with tabs for the sole purpose of having an indented here-doc...

	cat <<-EOF
	search-points.sh
	        simple wrapper to run 'search-points' criterion benchmark as a "standalone" binary
	        and propagate configurable parameters into the benchmark
	        (by abusing environment-variables)

	--help
	        Display this help message
	--dry-run
	        Print commands required to run the benchmark without executing anything
	--criterion-help
	        Display criterion help message

	--uri URI
	        Qdrant URI
	--request-timeout|--timeout REQUEST_TIMEOUT
	        Request timeout for Qdrant client (in seconds; float-point value)
	--connection-timeout CONNECTION_TIMEOUT
	        Connection timeout for Qdrant client (in seconds; float-point value)
	--api-key API_KEY
	        API key for Qdrant client
	--collection-name|--collection COLLECTION_NAME
	        Name of the collection to query
	--limit LIMIT
	        Number of points to query
	--seed SEED
	        Random seed for deterministic runs
	EOF
}


function main {
	declare -a criterion_args

	while (( $# ))
	do
		declare arg="$1"

		case "$arg" in
			--help)
				help
				exit
			;;

			--dry-run)
				declare dry_run=1
				shift
			;;

			--criterion-help)
				criterion_args+=( --help )
				shift
			;;

			*)
				if is-option "$arg"
				then
					export-value "$@"
					shift 2
				else
					criterion_args+=( "$arg" )
					shift
				fi
			;;
		esac
	done

	criterion "${criterion_args[@]}"
}


declare -A OPTIONS=(
	[--uri]=URI
	[--request-timeout]=REQUEST_TIMEOUT
	[--timeout]=REQUEST_TIMEOUT
	[--connection-timeout]=CONNECTION_TIMEOUT
	[--api-key]=API_KEY
	[--collection-name]=COLLECTION_NAME
	[--collection]=COLLECTION_NAME
	[--limit]=LIMIT
	[--seed]=SEED
)

function is-option {
	declare option="$1"

	[[ -v OPTIONS[$option] ]]
}

function export-value {
	declare option="$1"
	declare args=( "${@:1}" )

	is-option "$option"

	if ! (( ${#args[@]} ))
	then
		error-missing-value "$option"
	fi

	declare value="${args[1]}"

	if ! is-value "$value" && [[ "$option" != --api-key ]]
	then
		error-missing-value "$option"
	fi

	export "${OPTIONS[$option]}"="$value"
}

function is-value {
	declare value="$1"

	[[ $value != --* ]]
}

function error-missing-value {
	declare option="$1"

	echo "ERROR: Missing value for $option option" >&2
	return 1
}


function criterion {
	declare parent_dir="$(dirname "$(realpath -L "${BASH_SOURCE[${#BASH_SOURCE[@]} - 1]}")")"

	dry-run cd "$parent_dir"
	dry-run $(is-dry-run && env) cargo run --release -- "$@" --bench
	dry-run cd - >/dev/null
}


function dry-run {
	${dry_run:+echo} "$@"
}

function is-dry-run {
	[[ -v dry_run ]]
}

function env {
	declare -A env
	declare var

	for option in "${!OPTIONS[@]}"
	do
		var="${OPTIONS[$option]}"
		env[$var]=1
	done

	declare filter=''

	for var in "${!env[@]}"
	do
		if [[ -n $filter ]]
		then
			filter+='|'
		fi

		filter+="$var"
	done

	export -p | sed -E 's|declare -x ([^[:space:]]+=)|\1|g' | grep -E "$filter"
}


if ! (return 0 &>/dev/null)
then
	main "$@"
fi
