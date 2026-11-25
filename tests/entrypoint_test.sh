#!/usr/bin/env bash
# Check argument forwarding in the Docker entrypoint without starting Qdrant.

set -eu

test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
cp "$(dirname "$0")/../tools/entrypoint.sh" "$test_dir/"
cd "$test_dir"

cat > qdrant <<'EOF'
#!/usr/bin/env bash
if [[ -n ${QDRANT__STORAGE__RECOVERY_MODE:-} ]]; then
    mode=recovery
    status=23
else
    mode=normal
    status=$NORMAL_STATUS
fi

printf '%s\n' "$mode"
for arg in "$@"; do
    printf '<%s>\n' "$arg"
done
exit "$status"
EOF
chmod +x qdrant

unset QDRANT_ALLOW_RECOVERY_MODE QDRANT_INIT_FILE_PATH QDRANT__STORAGE__RECOVERY_MODE

check_launch() {
    local name=$1 expected_status=$2 expected_output=$3
    shift 3

    local status=0
    ./entrypoint.sh "$@" > actual || status=$?
    if [[ $status != "$expected_status" ]]; then
        printf 'FAIL %s: expected exit %s, got %s\n' "$name" "$expected_status" "$status"
        return 1
    fi
    if ! diff -u <(printf '%s\n' "$expected_output") actual; then
        printf 'FAIL %s: unexpected launch output\n' "$name"
        return 1
    fi
    printf 'PASS %s\n' "$name"
}

failed=0
NORMAL_STATUS=0 check_launch normal_config_path_with_spaces 0 $'normal\n<--config-path>\n<test data/config.yaml>' \
    --config-path 'test data/config.yaml' || failed=1
NORMAL_STATUS=137 QDRANT_ALLOW_RECOVERY_MODE=true check_launch recovery_config_path_with_spaces 23 \
    $'normal\n<--config-path>\n<test data/config.yaml>\nrecovery\n<--config-path>\n<test data/config.yaml>' \
    --config-path 'test data/config.yaml' || failed=1

touch matching.yaml
NORMAL_STATUS=0 check_launch literal_wildcard_and_empty_argument 0 $'normal\n<*.yaml>\n<>' \
    '*.yaml' '' || failed=1
NORMAL_STATUS=0 check_launch no_arguments 0 'normal' || failed=1
NORMAL_STATUS=137 check_launch recovery_disabled 137 $'normal\n<--config-path>\n<config/config.yaml>' \
    --config-path config/config.yaml || failed=1
NORMAL_STATUS=1 QDRANT_ALLOW_RECOVERY_MODE=true check_launch non_137_exit 1 'normal' || failed=1

touch 'initialized marker'
NORMAL_STATUS=137 QDRANT_ALLOW_RECOVERY_MODE=true QDRANT_INIT_FILE_PATH='initialized marker' \
    check_launch already_initialized 137 'normal' || failed=1

exit "$failed"
