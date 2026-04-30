#!/usr/bin/env zsh
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/lakontratw-stack/ernie-morning-brief.git}"
BRANCH="${BRANCH:-main}"
WORKDIR="${WORKDIR:-/tmp/ernie-morning-brief-deploy}"
SOURCE_DIR="$(cd "$(dirname "$0")" && pwd)"

rm -rf "$WORKDIR"
git clone "$REPO_URL" "$WORKDIR"
cd "$WORKDIR"
git checkout "$BRANCH"

rsync -a --delete \
  --exclude '.git/' \
  --exclude '.env' \
  --exclude '.venv/' \
  --exclude 'lyra_line.db' \
  "$SOURCE_DIR"/ "$WORKDIR"/

git add .
git commit -m "Add Lyra LINE OA integration" || {
  echo "No changes to commit."
  exit 0
}
git push origin "$BRANCH"

echo "Pushed to $REPO_URL on branch $BRANCH."
echo "Render should auto-deploy from this commit."
