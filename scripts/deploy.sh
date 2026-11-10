#!/usr/bin/env bash
set -euo pipefail
if [[ $# != 1 ]]; then
  echo 'usage: scripts/deploy.sh <saved-machine-label>' >&2
  exit 2
fi
cd "$(dirname "$0")/.."
# Always rebuild: Go's build cache keeps this fast, and timestamp checks miss
# go.mod, go.sum and Makefile changes.
make dist
binary="$PWD/dist/herd-msg-linux-amd64"
target="$("$binary" deploy-target "$1")"
ssh_options=(-o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=8)
# Every remote command is interpreted by a shell. Quote paths as data.
quote() { printf "'%s'" "${1//\'/\'\"\'\"\'}"; }
staging="$(ssh "${ssh_options[@]}" -- "$target" 'mkdir -p "$HOME/.local/share" && mktemp -d "$HOME/.local/share/.herd-msg-deploy.XXXXXX"')"
if [[ "$staging" != /* || "$staging" == *$'\n'* || "$staging" != */.herd-msg-deploy.* ]]; then
  echo 'remote staging did not return one absolute owned path' >&2
  exit 1
fi
cleanup() { ssh "${ssh_options[@]}" -- "$target" "rm -rf -- $(quote "$staging")"; }
trap cleanup EXIT
# The folder is a reviewable package; setup installs its embedded copy atomically.
tar -cf - -C "$PWD" herdr-plugin.toml .claude-plugin claude-plugin skills -C "$PWD/dist" herd-msg-linux-amd64 |
  ssh "${ssh_options[@]}" -- "$target" "tar -xf - -C $(quote "$staging")"
ssh "${ssh_options[@]}" -- "$target" "env -u CODEX_THREAD_ID -u CLAUDE_CODE_SESSION_ID $(quote "$staging/herd-msg-linux-amd64") setup --json"
echo "Installed herd-msg on saved machine: $1"
ssh "${ssh_options[@]}" -- "$target" '"$HOME/.local/bin/herd-msg" status --json'
echo 'Run herd-msg doctor manually for operator shell checks.'
