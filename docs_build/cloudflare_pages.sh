#!/usr/bin/env bash

set -ex

cd ..

uv run --only-group docs zensical build -d ./docs_build/site

curl -sLo ./docs_build/site/t.js "https://cloud.umami.is/script.js"
