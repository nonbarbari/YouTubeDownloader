# scripts/push.sh
#!/usr/bin/env bash
set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📤 PUSHING TO GITHUB"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

git config user.name "github-actions[bot]"
git config user.email "bot@github.com"

echo ""
echo "📋 Adding all files..."
git add -A

TOTAL=$(git diff --cached --name-only | wc -l)
echo "   Found $TOTAL files"

if [ "$TOTAL" -eq 0 ]; then
  echo "❌ Nothing to push"
  exit 0
fi

git diff --cached --name-only | while read f; do
  echo "   📎 $f"
done

echo ""
echo "📤 Pushing..."

git commit -m "📦 $(date +%m%d-%H%M) [skip ci]" --quiet

for i in 1 2 3 4 5; do
  echo -n "   🚀 Attempt $i/5..."
  if git pull --rebase --quiet 2>/dev/null && git push --quiet 2>/dev/null; then
    echo " ✅ DONE!"
    exit 0
  fi
  echo " ❌"
  git rebase --abort 2>/dev/null
  [ $i -lt 5 ] && sleep $((i*5))
done

echo "   ❌ Push failed"
exit 1
