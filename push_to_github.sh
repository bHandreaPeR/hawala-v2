#!/bin/bash
# ── Hawala v2 — One-command GitHub push ──────────────────────────
# Run this from your Mac Terminal whenever Claude asks you to push:
#   bash "/Users/subhransubaboo/Claude Projects/Hawala v2/Hawala v2/push_to_github.sh"

WORKSPACE="/Users/subhransubaboo/Claude Projects/Hawala v2/Hawala v2"
REPO="https://github.com/subhransubaboo/hawala-v2.git"
TOKEN=$(cat "$WORKSPACE/.github_token" 2>/dev/null)

if [ -z "$TOKEN" ]; then
  echo "❌ Token not found at $WORKSPACE/.github_token"
  exit 1
fi

TMPDIR=$(mktemp -d)
cp -r "$WORKSPACE"/*.py "$WORKSPACE"/*.ipynb "$WORKSPACE"/README.md \
      "$WORKSPACE"/requirements.txt "$TMPDIR/" 2>/dev/null

cd "$TMPDIR"
git init -b main -q
git config user.email "subhraba01@gmail.com"
git config user.name "Subhransu"
git add .
git commit -q -m "sync: $(date '+%Y-%m-%d %H:%M')"
git remote add origin "https://subhransubaboo:${TOKEN}@${REPO#https://}"
git push origin main --force -q

echo "✅ Pushed to github.com/subhransubaboo/hawala-v2"
rm -rf "$TMPDIR"
