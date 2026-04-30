#!/usr/bin/env zsh
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/lakontratw-stack/ernie-morning-brief.git}"
BRANCH="${BRANCH:-main}"
WORKDIR="${WORKDIR:-/tmp/ernie-morning-brief-deploy}"
SOURCE_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_KEY="${DEPLOY_KEY:-$SOURCE_DIR/.deploy_keys/github_lakontratw_deploy}"

if [[ -f "$DEPLOY_KEY" ]]; then
  SSH_HOST="github.com"
  SSH_PORT="22"
  if [[ "${GITHUB_SSH_OVER_HTTPS:-1}" == "1" ]]; then
    SSH_HOST="ssh.github.com"
    SSH_PORT="443"
  fi
  export GIT_SSH_COMMAND="ssh -i '$DEPLOY_KEY' -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o HostName=$SSH_HOST -o Port=$SSH_PORT"
  if [[ "$REPO_URL" == https://github.com/* ]]; then
    REPO_URL="git@github.com:${REPO_URL#https://github.com/}"
    REPO_URL="${REPO_URL%.git}.git"
  fi
fi

rm -rf "$WORKDIR"
git clone "$REPO_URL" "$WORKDIR"
cd "$WORKDIR"
git checkout "$BRANCH"

rsync -a --delete \
  --exclude '.git/' \
  --exclude '.deploy_keys/' \
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
