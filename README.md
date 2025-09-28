
# Duka-DL
A fast and simple tool for data download from Dukascopy

Dukascopy is known for its high-quality, tick-level historical market data, which is excellent for backtesting trading strategies. However, downloading this data for long periods is a tedious manual process of downloading, saving, and resampling individual daily files.

**Duka-DL** automates this entire workflow. It is a free, fast, and simple command-line tool that fetches data for each day in your specified range and consolidates it into a single, clean CSV or Parquet file, ready for analysis.

## Installation

Install the package directly from PyPI:

```bash
pip install duka-dl
```

## Usage

Using the tool is straightforward. You can specify a symbol, start date, and end date.

### Download Data for a Specific Date Range

To download data for `EURUSD` from January 1st, 2023 to January 31st, 2023:

```bash
duka-dl EUR-USD -s 01-01-2023 -e 31-01-2023
```

### Download From a Start Date to Present

If you provide only a start date, the tool will automatically download data up to the most recent complete day.

```bash
duka-dl EUR-USD -s 01-06-2024
```

### Download All Available Data

To download all available historical data for a symbol (the tool will find the earliest data available):

```bash
duka-dl EUR-USD --all
```

### Save as Parquet

For better performance and smaller file sizes, you can save the output in Parquet format using the `-p` or `--parquet` flag:

```bash
duka-dl EUR-USD --all --parquet
```

### Download ASK Price

By default, the tool downloads BID prices. To download ASK prices, use the `-m ASK` flag.

```bash
duka-dl GBP-JPY -s 01-01-2024 -m ASK
```

### Help

To see a list of all available commands and options, use the `--help` flag:

```bash
duka-dl --help
```

## License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

