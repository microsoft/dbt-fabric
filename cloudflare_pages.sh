#!/usr/bin/env bash

set -ex

pip install .[docs]
mkdocs build
