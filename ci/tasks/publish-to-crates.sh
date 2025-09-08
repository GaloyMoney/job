#!/bin/bash

set -e

pushd repo

cat <<EOF | cargo login
${CRATES_API_TOKEN}
EOF

cargo publish -p job --all-features --no-verify
