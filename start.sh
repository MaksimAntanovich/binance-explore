#!/usr/bin/env bash

PYTHONPATH="${PYTHONPATH}:$(pwd)" python3 src/entrypoint.py -c resource/config.conf --job SimpleJob --start 2023-04-01 --end 2023-04-07