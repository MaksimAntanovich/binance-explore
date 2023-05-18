# Exploring Binance Public Data with pySpark

## Entrypoints

to download data run `download_data.sh`
```shell
./download_data.sh
```

to launch simple spark job run `start.sh` with `JOBNAME`, `START_DATE` and `END_DATE` parameters
```shell
./start.sh JOBNAME START_DATE END_DATE
```
for example
```shell
./start.sh SimpleJob 2023-04-01 2023-04-02
```