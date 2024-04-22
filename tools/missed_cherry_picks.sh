#!/usr/bin/env bash

# List all commits that have not been cherry-picked from dev into master yet
#
# Example usage:
# ./missed_cherry_picks.sh

set -euo pipefail

# Ignore all commits upto and including this commit hash on dev
IGNORE_UPTO=a3abcef0f189301292516faaae9e9e2c2fb8ab58

# Fetch latest branch info from remote
git fetch -q origin master
git fetch -q origin dev

# Get remote/local commit hash of dev, user must have up-to-date branch
REMOTE_SHA=$(git log -n 1 --pretty=format:"%H" origin/dev)
LOCAL_SHA=$(git log -n 1 --pretty=format:"%H" dev)
if [[ "$REMOTE_SHA" != "$LOCAL_SHA" ]]; then
    echo Error: your local dev branch must match the current remote
    echo To pull latest dev, use: git checkout dev \&\& git pull --ff-only origin dev
    exit 1
fi

# List all commits yet-to-be-cherry-picked
git cherry -v origin/master origin/dev $IGNORE_UPTO \
| grep --color=never '^+ ' \
| cut -d' ' -f2-
