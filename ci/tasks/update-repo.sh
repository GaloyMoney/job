#!/bin/bash

set -eu

# ----------- UPDATE REPO -----------
git config --global user.email "bot@galoy.io"
git config --global user.name "CI Bot"

pushd repo

VERSION="$(cat ../version/version)"

cat <<EOF >new_change_log.md
# [job release v${VERSION}](https://github.com/GaloyMoney/job/releases/tag/${VERSION})

$(cat ../artifacts/gh-release-notes.md)

$(cat CHANGELOG.md)
EOF
mv new_change_log.md CHANGELOG.md

for file in $(find . -name Cargo.toml); do
  sed -i'' "s/^version.*/version = \"${VERSION}\"/" ${file}
done

cargo update --workspace

git status
git add .

if [[ "$(git status -s -uno)" != ""  ]]; then
  git commit -m "ci(release): release version $(cat ../version/version)"
fi
