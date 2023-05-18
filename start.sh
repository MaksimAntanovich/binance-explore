#!/usr/bin/env bash

PYTHONPATH="${PYTHONPATH}:$(pwd)" python3 src/entrypoint.py -c resource/config.conf --job $1 --start $2 --end $3