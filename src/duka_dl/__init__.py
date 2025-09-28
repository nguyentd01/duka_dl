import asyncio
from datetime import datetime, timedelta,date
import argparse
import httpx
import polars as pl
from tqdm.asyncio import tqdm
import os
import json
from rich_argparse import RichHelpFormatter

def process_date(start_date: str, end_date: str):
    s = datetime.strptime(start_date, "%d-%m-%Y").date()
    e = datetime.strptime(end_date, "%d-%m-%Y").date()
    out, d = [], s
    one = timedelta(days=1)
    while d <= e:
        out.append(d)
        d += one
    return out

def json_to_polars_day(data: dict, d) -> pl.DataFrame | None:
    base_open  = data.get("open")
    base_high  = data.get("high")
    base_low   = data.get("low")
    base_close = data.get("close")
    t0_ms      = data.get("timestamp")
    opens  = data.get("opens", [])
    highs  = data.get("highs", [])
    lows   = data.get("lows", [])
    closes = data.get("closes", [])
    times  = data.get("times", [])
    times = [i * 60 for i in times]
    vols   = data.get("volumes", [])
    if None in (base_open, base_high, base_low, base_close) or not times or not t0_ms:
        tqdm.write(f"No data for {symbol} on {d.strftime('%d-%m-%Y')}")
        return None
    scale = data.get("multiplier", 1e-5)
    o_abs = (pl.Series(opens,  dtype=pl.Int64) * scale).cum_sum() + float(base_open)
    h_abs = (pl.Series(highs,  dtype=pl.Int64) * scale).cum_sum() + float(base_high)
    l_abs = (pl.Series(lows,   dtype=pl.Int64) * scale).cum_sum() + float(base_low)
    c_abs = (pl.Series(closes, dtype=pl.Int64) * scale).cum_sum() + float(base_close)
    o_abs = o_abs.round(5); h_abs = h_abs.round(5); l_abs = l_abs.round(5); c_abs = c_abs.round(5)
    t0_s  = float(t0_ms) / 1000.0
    t_abs = pl.Series(times, dtype=pl.Int64).cum_sum() + t0_s
    ts    = pl.from_epoch(t_abs, time_unit="s").dt.strftime("%Y-%m-%d %H:%M:%S")
    n = min(len(ts), len(o_abs), len(h_abs), len(l_abs), len(c_abs), len(vols))
    if n == 0:
        return None
    return pl.DataFrame(
        {
            "timestamp": ts[:n],
            "open":      o_abs[:n],
            "high":      h_abs[:n],
            "low":       l_abs[:n],
            "close":     c_abs[:n],
            "volume":    pl.Series(vols[:n], dtype=pl.Float64),
        }
    )

async def fetch_one_day(client: httpx.AsyncClient, symbol: str, d, semaphore, mode):
    url = f"https://jetta.dukascopy.com/v1/candles/minute/{symbol}/{mode}/{d.year}/{d.month}/{d.day}"
    async with semaphore:
        try:
            r = await client.get(url, timeout=10)
            r.raise_for_status()
            return json_to_polars_day(r.json(),d)
        except httpx.RequestError as exc:
            tqdm.write(f"An error occurred while requesting {exc.request.url!r}.")
            return None
        except httpx.HTTPStatusError as exc:
            tqdm.write(f"Error response {exc.response.status_code} while requesting {exc.request.url!r}.")
            return None
        except json.JSONDecodeError:
            tqdm.write(f"Failed to decode JSON from response for {symbol} on {d}.")
            return None

def check_one_day(symbol: str, d):
    url = f"https://jetta.dukascopy.com/v1/candles/minute/{symbol}/BID/{d.year}/{d.month}/{d.day}"
    try:
        r = httpx.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        status = (data.get('open') is not None)
        return status
    except (httpx.RequestError, httpx.HTTPStatusError, json.JSONDecodeError):
        return False



async def main():
    parser = argparse.ArgumentParser(
    prog='duka_dl',
    description='Downloads data from Dukascopy')
    parser = argparse.ArgumentParser(formatter_class=RichHelpFormatter)
    parser.add_help = True
    parser.add_argument('Symbol')
    parser.add_argument('-s', '--start',metavar='',help="Start date")
    parser.add_argument('-e', '--end',metavar='',help="End date")
    parser.add_argument('-f', '--folder',default='.',metavar='',help="Output folder")
    parser.add_argument('-t', '--thread',default=20,metavar='',help="Threads (default: 20)")
    parser.add_argument('-se', '--semaphore',default=50,metavar='',help="Async semaphore (default: 50)")
    parser.add_argument('-all', '--all',action='store_true',help="Download all")
    parser.add_argument('-p','--parquet',action='store_true',help="Save as parquet (default: csv)")
    parser.add_argument('-m','--mode',default='BID',metavar='',help="BID or ASK price (default: BID)")
    args = parser.parse_args()
    mode = str(args.mode).upper()
    folder = args.folder
    sem = asyncio.Semaphore(int(args.semaphore))
    connection = int(args.thread)
    global symbol
    symbol = args.Symbol
    start_date = args.start
    end_date = args.end
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")
    if args.all:
        start = 2000
        end_date = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")
        print(f"Tracking data from 2000")
        for i in range(start, datetime.now().year + 1):
            check_date = date(day=1, month=1, year=i)
            s = check_one_day(symbol, check_date)
            if s == True:
                start_date = check_date.strftime("%d-%m-%Y")
                print(f"Downloading data from {start_date}")
                break

    dates = process_date(str(start_date), str(end_date))
    limits = httpx.Limits(max_connections=connection, max_keepalive_connections=connection)


    async with httpx.AsyncClient(http2=True, limits=limits) as client:
        tasks = [fetch_one_day(client, symbol, d, sem, mode) for d in dates]
        results = await tqdm.gather(*tasks,total=len(tasks))

    dfs = []
    for d, res in zip(dates, results):
        if isinstance(res, Exception) or res is None or res.height == 0:
            continue
        dfs.append((d, res))

    if not dfs:
        print("No data.")
        return

    dfs.sort(key=lambda x: x[0])
    full = pl.concat([df for _, df in dfs], how="vertical_relaxed", rechunk=True).select(
        "timestamp",
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
    )
    print("Writing...")

    file_name = f"{symbol}({start_date} to {end_date})"
    if args.parquet:
        full.write_parquet(f"{folder}/{file_name}.parquet")
        print(f"Wrote {full.height} rows across {len(dfs)} days -> {os.path.normpath(f'{folder}/{file_name}.parquet')}")
    else:
        full.write_csv(f"{folder}/{file_name}.csv")
        print(f"Wrote {full.height} rows across {len(dfs)} days -> {os.path.normpath(f'{folder}/{file_name}.csv')}")

def run():
    asyncio.run(main())

if __name__ == "__main__":
    run()