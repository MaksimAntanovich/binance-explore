#!/bin/bash

PYTHONPATH="${PYTHONPATH}:$(pwd)" python3 src/entrypoint.py -c resource/config.conf --job SimpleJob