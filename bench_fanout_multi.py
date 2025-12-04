#!/usr/bin/env python3
import subprocess
import time
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import matplotlib.pyplot as plt

BASE_URL = "https://hip-return-473713-r7.appspot.com/api/timeline"

PARAMS = [10, 50, 100]   # nombre de followees par user
RUNS = 3
TOTAL_REQUESTS = 1000
CONCURRENCY = 50

OUTPUT_CSV = "fanout.csv"
OUTPUT_PNG = "fanout.png"


# ------------ 1. SEED DES DONNÉES ------------

def seed_fanout():
    print("==> SEED fanout10 (10 followees, ~100 posts/user)")
    subprocess.run([
        "python", "seed.py",
        "--users", "1000",
        "--posts", "100000",
        "--follows-min", "10",
        "--follows-max", "10",
        "--prefix", "fanout10",
    ], check=True)

    print("==> SEED fanout50 (50 followees, ~100 posts/user)")
    subprocess.run([
        "python", "seed.py",
        "--users", "1000",
        "--posts", "100000",
        "--follows-min", "50",
        "--follows-max", "50",
        "--prefix", "fanout50",
    ], check=True)

    print("==> SEED fanout100 (100 followees, ~100 posts/user)")
    subprocess.run([
        "python", "seed.py",
        "--users", "1000",
        "--posts", "100000",
        "--follows-min", "100",
        "--follows-max", "100",
        "--prefix", "fanout100",
    ], check=True)

    print("==> SEED fanout* terminé.")


# ------------ 2. BENCHMARK ------------

def fetch_timeline(user: str):
    url = f"{BASE_URL}?user={user}&limit=20"
    t0 = time.perf_counter()
    try:
        resp = requests.get(url, timeout=10)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        failed = not resp.ok
        return dt_ms, failed
    except Exception:
        dt_ms = (time.perf_counter() - t0) * 1000.0
        return dt_ms, True


def users_for_param(param: int):
    if param == 10:
        prefix = "fanout10"
    elif param == 50:
        prefix = "fanout50"
    elif param == 100:
        prefix = "fanout100"
    else:
        raise ValueError(f"Param inconnu: {param}")
    return [f"{prefix}{i}" for i in range(1, CONCURRENCY + 1)]


def run_for_param(param: int, run_idx: int):
    print(f"=== PARAM={param}, run={run_idx} ===")

    users = users_for_param(param)
    c = len(users)

    base = TOTAL_REQUESTS // c
    rest = TOTAL_REQUESTS % c
    per_user_counts = [base + (1 if i < rest else 0) for i in range(c)]

    latencies = []
    any_failed = False

    def worker(user, count):
        nonlocal any_failed
        user_lats = []
        for _ in range(count):
            dt, failed = fetch_timeline(user)
            user_lats.append(dt)
            if failed:
                any_failed = True
        return user_lats

    with ThreadPoolExecutor(max_workers=c) as executor:
        futures = []
        for user, count in zip(users, per_user_counts):
            if count > 0:
                futures.append(executor.submit(worker, user, count))
        for f in as_completed(futures):
            latencies.extend(f.result())

    if not latencies:
        return float("nan"), 1

    avg_time = sum(latencies) / len(latencies)
    failed_flag = 1 if any_failed else 0
    print(f"    AVG_TIME={avg_time:.3f} ms, FAILED={failed_flag}")
    return avg_time, failed_flag


def make_bar_chart(rows):
    by_param = {}
    for row in rows:
        p = row["param"]
        by_param.setdefault(p, []).append(row["avg"])

    params_sorted = sorted(by_param.keys())
    means, stds = [], []

    for p in params_sorted:
        vals = by_param[p]
        m = sum(vals) / len(vals)
        if len(vals) > 1:
            var = sum((x - m) ** 2 for x in vals) / len(vals)
            s = var ** 0.5
        else:
            s = 0.0
        means.append(m)
        stds.append(s)

    plt.figure()
    plt.bar([str(p) for p in params_sorted], means, yerr=stds)
    plt.xlabel("Nombre de followees par utilisateur (PARAM)")
    plt.ylabel("Temps moyen par requête(ms)")
    plt.title("Performance vs le nombre de followee (C=50)")
    plt.tight_layout()
    plt.savefig(OUTPUT_PNG)
    plt.close()
    print(f"Graphique enregistré dans {OUTPUT_PNG}")


def main():
    # 1) Seed jeux fanout*
    seed_fanout()

    # 2) Mesures
    rows = []
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["PARAM", "AVG_TIME", "RUN", "FAILED"])
        for param in PARAMS:
            for run in range(1, RUNS + 1):
                avg, failed = run_for_param(param, run)
                writer.writerow([param, f"{avg:.3f}", run, failed])
                rows.append({"param": param, "avg": avg, "run": run, "failed": failed})

    print(f"CSV enregistré dans {OUTPUT_CSV}")
    make_bar_chart(rows)


if __name__ == "__main__":
    main()
