#!/usr/bin/env bash

set -ex

cd ..

uv run --only-group docs mkdocs build -d ./docs_build/site

curl -sLo ./docs_build/site/t1.js "https://cdn.jsdelivr.net/gh/Swetrix/swetrix-js@latest/dist/swetrix.js"
curl -sLo ./docs_build/site/t1.js.map "https://cdn.jsdelivr.net/gh/Swetrix/swetrix-js@latest/dist/swetrix.js.map"
curl -sLo ./docs_build/site/t2.js "https://cloud.umami.is/script.js"
