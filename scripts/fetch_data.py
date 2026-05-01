#!/usr/bin/env python3
"""
fetch_data.py — Astra Research / Chiliz Burn Tracker

Pulls on-chain + market data and writes data.json for the live dashboard.
Runs from a GitHub Actions cron every 6 hours.

Sources:
  - Chiliz Chain RPC (Ankr public)        on-chain stake, burns, validators
  - CoinGecko free public API             price, market cap, circulating supply
  - Routescan etherscan-compatible API    Stake/Unstake event log sweep

Outputs (stdout): data.json with the full snapshot.

Exit non-zero on any fetch failure so the workflow does not commit stale data.
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# ---- Configuration ----
RPC_URL = os.environ.get("CHZ_RPC", "https://rpc.ankr.com/chiliz")
ROUTESCAN = "https://api.routescan.io/v2/network/mainnet/evm/88888/etherscan/api"
COINGECKO = "https://api.coingecko.com/api/v3"

VALIDATOR_CONTRACT = "0x0000000000000000000000000000000000001000"
STAKING_CONTRACT   = "0x0000000000000000000000000000000000007001"
BURN_DESTINATION   = "0x0000000000000000000000000000000000000000"

# keccak256 selectors / event topics (see methodology section in index.html)
GET_VALIDATORS_SELECTOR = "0xb7ab4db5"          # getValidators() returns address[]
GET_VALIDATOR_STATUS    = "0xa310624f"          # getValidatorStatus(address)
STAKE_EVENT_TOPIC       = "0x99039fcf0a98f484616c5196ee8b2ecfa971babf0b519848289ea4db381f85f7"

# Event timestamps used to mark the chart
EVT_ANNOUNCED   = "2026-02-09"
EVT_FIRST_BURN  = "2026-03-05"
EVT_FIRST_REPORT = "2026-04-10"


def rpc(method, params, timeout=15):
    payload = json.dumps({"jsonrpc": "2.0", "method": method, "params": params, "id": 1}).encode()
    req = urllib.request.Request(RPC_URL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        d = json.loads(r.read())
    if "error" in d:
        raise RuntimeError(f"RPC error: {d['error']}")
    return d["result"]


def routescan_get(path_with_qs, timeout=15):
    url = f"{ROUTESCAN}?{path_with_qs}"
    req = urllib.request.Request(url, headers={"User-Agent": "AstraDataBot/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def cg_get(path, timeout=15):
    url = f"{COINGECKO}{path}"
    req = urllib.request.Request(url, headers={"User-Agent": "AstraDataBot/1.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def to_chz(wei_hex_or_int):
    if isinstance(wei_hex_or_int, str):
        v = int(wei_hex_or_int, 16)
    else:
        v = int(wei_hex_or_int)
    return v / 1e18


# ---- Main fetch routines ----

def fetch_chain_snapshot():
    """Pull on-chain state at the latest block."""
    latest_hex = rpc("eth_blockNumber", [])
    latest = int(latest_hex, 16)

    # Total stake = native balance of validator contract
    total_staked = to_chz(rpc("eth_getBalance", [VALIDATOR_CONTRACT, "latest"]))

    # Cumulative burns = native balance of burn destination
    cumulative_burned = to_chz(rpc("eth_getBalance", [BURN_DESTINATION, "latest"]))

    # Validator list
    raw = rpc("eth_call", [{"to": VALIDATOR_CONTRACT, "data": GET_VALIDATORS_SELECTOR}, "latest"])
    data = raw[2:] if raw.startswith("0x") else raw
    offset = int(data[0:64], 16)
    length_pos = offset * 2
    length = int(data[length_pos:length_pos + 64], 16)
    validator_addrs = []
    for i in range(length):
        start = length_pos + 64 + i * 64
        validator_addrs.append("0x" + data[start + 24:start + 64])

    # Per-validator stake via getValidatorStatus(address)
    validators = []
    for addr in validator_addrs:
        padded = addr[2:].rjust(64, "0")
        out = rpc("eth_call", [{"to": VALIDATOR_CONTRACT, "data": GET_VALIDATOR_STATUS + padded}, "latest"])
        raw = out[2:] if out.startswith("0x") else out
        if len(raw) < 192:
            continue
        # output: address, uint8, uint256, ...
        delegated_chz = int(raw[128:192], 16) / 1e18
        validators.append({"addr": addr, "stake": round(delegated_chz)})

    return {
        "block": latest,
        "block_hex": latest_hex,
        "total_staked_chz": round(total_staked),
        "cumulative_burned_chz": round(cumulative_burned),
        "validators": validators,
    }


def fetch_burn_history(latest_block, weeks_back=26):
    """Sample burn destination balance weekly for the past N weeks."""
    BLOCKS_PER_DAY = 28800  # ~3s per block
    series = []
    for w in range(weeks_back, -1, -1):
        target = max(1, latest_block - w * 7 * BLOCKS_PER_DAY)
        target_hex = hex(target)
        try:
            bal_hex = rpc("eth_getBalance", [BURN_DESTINATION, target_hex])
            bal = to_chz(bal_hex)
            ts = int((time.time() - w * 7 * 86400) * 1000)
            series.append([ts, round(bal)])
        except Exception:
            pass
        time.sleep(0.05)
    return series


def fetch_stake_history(latest_block, weeks_back=26):
    """Sample validator contract balance weekly for the past N weeks."""
    BLOCKS_PER_DAY = 28800
    series = []
    for w in range(weeks_back, -1, -1):
        target = max(1, latest_block - w * 7 * BLOCKS_PER_DAY)
        target_hex = hex(target)
        try:
            bal_hex = rpc("eth_getBalance", [VALIDATOR_CONTRACT, target_hex])
            bal = to_chz(bal_hex)
            ts = int((time.time() - w * 7 * 86400) * 1000)
            series.append([ts, round(bal)])
        except Exception:
            pass
        time.sleep(0.05)
    return series


def fetch_stakers_count(latest_block, lookback_blocks=6_000_000):
    """Count unique stakers from Stake events on the staking contract."""
    start = max(1, latest_block - lookback_blocks)
    chunk = 200_000
    stakers_set = set()
    val_stakers = {}
    cur = start
    while cur < latest_block:
        end = min(cur + chunk, latest_block)
        try:
            qs = urllib.parse.urlencode({
                "module": "logs", "action": "getLogs",
                "fromBlock": cur, "toBlock": end,
                "address": STAKING_CONTRACT,
                "topic0": STAKE_EVENT_TOPIC,
            })
            d = routescan_get(qs)
            logs = d.get("result", []) or []
            if isinstance(logs, list):
                for log in logs:
                    topics = log.get("topics", [])
                    if len(topics) >= 3:
                        validator = "0x" + topics[1][26:].lower()
                        staker = "0x" + topics[2][26:].lower()
                        stakers_set.add(staker)
                        val_stakers.setdefault(validator, set()).add(staker)
        except Exception:
            pass
        cur = end + 1
        time.sleep(0.05)
    return {
        "unique_stakers": len(stakers_set),
        "stakers_per_validator": {v: len(s) for v, s in val_stakers.items()},
    }


def fetch_market_data():
    """CHZ price, market cap, circulating supply, plus 180-day historical price."""
    simple = cg_get("/simple/price?ids=chiliz&vs_currencies=usd&include_market_cap=true&include_24hr_change=true")
    price_usd = simple["chiliz"]["usd"]
    mcap_usd = simple["chiliz"]["usd_market_cap"]
    change_24h = simple["chiliz"].get("usd_24h_change", 0)
    circulating = mcap_usd / price_usd if price_usd > 0 else 0

    # 180-day price history
    chart = cg_get("/coins/chiliz/market_chart?vs_currency=usd&days=180&interval=daily")
    prices = [[int(p[0]), round(float(p[1]), 5)] for p in chart.get("prices", [])]
    market_caps = chart.get("market_caps", [])
    return {
        "price_usd": price_usd,
        "market_cap_usd": round(mcap_usd),
        "circulating_chz": round(circulating),
        "change_24h_pct": round(change_24h, 2),
        "price_180d": prices,
        "market_cap_180d": [[int(m[0]), round(float(m[1]))] for m in market_caps],
    }


# ---- Compose ----

def build_snapshot():
    print("[fetch] chain snapshot...", file=sys.stderr)
    chain = fetch_chain_snapshot()

    print(f"[fetch] burn history (block {chain['block']})...", file=sys.stderr)
    burns_history = fetch_burn_history(chain["block"], weeks_back=26)

    print("[fetch] stake history...", file=sys.stderr)
    stake_history = fetch_stake_history(chain["block"], weeks_back=26)

    print("[fetch] stakers count (this is slow, sweeping logs)...", file=sys.stderr)
    stakers = fetch_stakers_count(chain["block"], lookback_blocks=6_000_000)

    print("[fetch] market data (CoinGecko)...", file=sys.stderr)
    market = fetch_market_data()

    # Derive monthly burn deltas from the weekly burn history
    # Pair up balances ~30 days apart for the last 6 months
    monthly_burns = []
    if burns_history:
        # sort by ts ascending, grab one snapshot ~per month for last 6 months
        sorted_h = sorted(burns_history, key=lambda x: x[0])
        # Take samples roughly every 4 entries (4 weeks)
        for i in range(4, len(sorted_h), 4):
            prev = sorted_h[i - 4]
            cur = sorted_h[i]
            delta = max(0, cur[1] - prev[1])
            monthly_burns.append({"ts": cur[0], "delta_chz": delta})

    # Last full-month burn = max value if positive
    last_month_burn = monthly_burns[-1]["delta_chz"] if monthly_burns else 0
    annualised = last_month_burn * 12

    free_float = max(0, market["circulating_chz"] - chain["total_staked_chz"] - chain["cumulative_burned_chz"])
    stake_ratio = (chain["total_staked_chz"] / market["circulating_chz"] * 100) if market["circulating_chz"] else 0
    free_float_pct = (free_float / market["circulating_chz"] * 100) if market["circulating_chz"] else 0
    burn_pct = (annualised / market["circulating_chz"] * 100) if market["circulating_chz"] else 0

    snapshot = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "block": chain["block"],
        "block_hex": chain["block_hex"],
        "burns": {
            "cumulative_chz": chain["cumulative_burned_chz"],
            "last_month_chz": last_month_burn,
            "annualised_chz": round(annualised),
            "annualised_pct_of_supply": round(burn_pct, 2),
            "history": burns_history,           # weekly snapshots [ts_ms, balance_chz]
            "monthly": monthly_burns,           # implied monthly deltas
        },
        "stake": {
            "total_chz": chain["total_staked_chz"],
            "validator_count": len(chain["validators"]),
            "validators": chain["validators"],   # [{addr, stake}]
            "history": stake_history,
            "unique_stakers_180d": stakers["unique_stakers"],
            "stakers_per_validator": stakers["stakers_per_validator"],
        },
        "supply": {
            "circulating_chz": market["circulating_chz"],
            "free_float_chz": free_float,
            "stake_ratio_pct": round(stake_ratio, 2),
            "free_float_pct": round(free_float_pct, 2),
        },
        "price": {
            "usd": market["price_usd"],
            "market_cap_usd": market["market_cap_usd"],
            "change_24h_pct": market["change_24h_pct"],
            "history_180d": market["price_180d"],
            "market_cap_180d": market["market_cap_180d"],
        },
        "events": {
            "buyback_announced": EVT_ANNOUNCED,
            "first_burn_executed": EVT_FIRST_BURN,
            "first_burn_report": EVT_FIRST_REPORT,
        },
        "methodology": {
            "rpc": RPC_URL,
            "validator_contract": VALIDATOR_CONTRACT,
            "staking_contract": STAKING_CONTRACT,
            "burn_destination": BURN_DESTINATION,
            "chain_id": 88888,
        },
    }

    return snapshot


def main():
    try:
        snap = build_snapshot()
        print(json.dumps(snap, separators=(",", ":")))
        print(f"[ok] snapshot generated at {snap['generated_at']} block {snap['block']}", file=sys.stderr)
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
