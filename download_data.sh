#!/usr/bin/env bash
rm -rf ./data
mkdir data
mkdir data/spot
mkdir data/option
for day  in {01..07}
do
  wget https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2023-06-${day}.zip -P data/spot
  wget https://data.binance.vision/data/option/daily/EOHSummary/BTCUSDT/BTCUSDT-EOHSummary-2023-06-${day}.zip -P data/option
done
