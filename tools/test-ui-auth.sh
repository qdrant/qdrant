#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:6333}"
COOKIE_JAR="$(mktemp)"
trap 'rm -f "$COOKIE_JAR"' EXIT

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; exit 1; }

echo "Testing Qdrant Web UI auth at $BASE_URL"

# 1) Dashboard should redirect to login without session
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/dashboard")
LOCATION=$(curl -s -D - -o /dev/null "$BASE_URL/dashboard" | awk 'tolower($1)=="location:" {print $2}' | tr -d '\r')
[[ "$STATUS" == "302" ]] || fail "expected 302 on /dashboard, got $STATUS"
[[ "$LOCATION" == /dashboard/login* ]] || fail "expected redirect to login, got $LOCATION"
pass "unauthenticated /dashboard redirects to login"

# 2) Login page is reachable
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/dashboard/login")
[[ "$STATUS" == "200" ]] || fail "expected 200 on /dashboard/login, got $STATUS"
pass "login page is public"

# 3) Invalid login rejected
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H 'Content-Type: application/json' \
  -d '{"username":"tester","password":"wrong"}' \
  "$BASE_URL/dashboard/auth/login")
[[ "$STATUS" == "401" ]] || fail "expected 401 for bad password, got $STATUS"
pass "invalid login rejected"

# 4) Valid login sets session cookie
LOGIN_BODY=$(curl -s -c "$COOKIE_JAR" -w "\n%{http_code}" \
  -H 'Content-Type: application/json' \
  -d '{"username":"tester","password":"s3cret-pass"}' \
  "$BASE_URL/dashboard/auth/login")
STATUS=$(echo "$LOGIN_BODY" | tail -n1)
BODY=$(echo "$LOGIN_BODY" | sed '$d')
[[ "$STATUS" == "200" ]] || fail "expected 200 for valid login, got $STATUS"
echo "$BODY" | grep -q '"status":"ok"' || fail "login response missing status ok"
grep -q 'qdrant_ui_session' "$COOKIE_JAR" || fail "session cookie not set"
pass "valid login succeeds and sets cookie"

# 5) Dashboard accessible with session
STATUS=$(curl -s -b "$COOKIE_JAR" -o /tmp/dashboard-body.html -w "%{http_code}" "$BASE_URL/dashboard")
[[ "$STATUS" == "200" ]] || fail "expected 200 on /dashboard with session, got $STATUS"
grep -q 'dashboard-ok' /tmp/dashboard-body.html || fail "dashboard body not served"
pass "authenticated dashboard loads static UI"

# 6) Dashboard includes logout control
grep -q 'Log out' /tmp/dashboard-body.html || fail "dashboard missing logout control"
pass "dashboard includes logout control"

# 7) Logout clears session
STATUS=$(curl -s -b "$COOKIE_JAR" -c "$COOKIE_JAR" -o /dev/null -w "%{http_code}" \
  -X POST "$BASE_URL/dashboard/auth/logout")
[[ "$STATUS" == "200" ]] || fail "expected 200 on logout, got $STATUS"
STATUS=$(curl -s -b "$COOKIE_JAR" -o /dev/null -w "%{http_code}" "$BASE_URL/dashboard")
[[ "$STATUS" == "302" ]] || fail "expected 302 on /dashboard after logout, got $STATUS"
pass "logout clears dashboard session"

# 8) API still open without API key (expected default)
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/collections")
[[ "$STATUS" == "200" ]] || fail "expected open API /collections, got $STATUS"
pass "REST API remains accessible without API key (default behavior)"

echo "All UI auth smoke tests passed."
