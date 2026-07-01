import csv

def load_symbols(csv_path: str):
    symbols = []
    seen = set()

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=2):
            exchange = (row.get("exchange") or "").strip().upper()
            symbol = (row.get("symbol") or "").strip().upper()

            if not exchange or not symbol:
                print(
                    f"[INIT][WARN] Skipping invalid row {idx} in {csv_path}: {row}",
                    flush=True,
                )
                continue

            key = f"{exchange}:{symbol}"
            if key in seen:
                continue
            seen.add(key)

            symbols.append({
                "exchange": exchange,
                "symbol": symbol,
                "key": key
            })

    print(f"[INIT] Loaded {len(symbols)} symbols from {csv_path}", flush=True)
    return symbols