#!/usr/bin/env bash

set -ex

cd ..

uv run --only-group docs mkdocs build
