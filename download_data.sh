#!/usr/bin/env bash
rm -rf ./data
mkdir data
for day  in {01..30}
do
  wget https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2023-04-${day}.zip -P data
done
