#!/usr/bin/env python3
import subprocess
import time
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import matplotlib.pyplot as plt

# URL de base de ton endpoint timeline
BASE_URL = "https://hip-return-473713-r7.oa.r.appspot.com/api/timeline"

# Paramètres de benchmark
CONCURRENCIES = [1, 10, 20, 50, 100, 1000]
RUNS = 3
TOTAL_REQUESTS = 1000

OUTPUT_CSV = "conc.csv"
OUTPUT_PNG = "conc.png"


# ------------ 1. SEED DES DONNÉES ------------

def seed_conc():
    """
    Crée le dataset concurrence : 1000 users, ~50 posts/user, 20 followees.
    Utilise seed.py dans le même dossier.
    """
    print("==> SEED CONC: 1000 users, 50000 posts, follows=20")
    cmd = [
        "python", "seed.py",
        "--users", "1000",
        "--posts", "50000",
        "--follows-min", "20",
        "--follows-max", "20",
        "--prefix", "conc",
    ]
    subprocess.run(cmd, check=True)
    print("==> SEED CONC terminé.")


# ------------ 2. BENCHMARK ------------

def fetch_timeline(user: str):
    """Envoie UNE requête timeline pour un user donné. Retourne (latence_ms, failed)."""
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


def run_for_concurrency(c: int, run_idx: int):
    """Lance TOTAL_REQUESTS requêtes avec c utilisateurs distincts en parallèle."""
    print(f"=== C={c}, run={run_idx} ===")

    # conc1 .. concC
    users = [f"conc{i}" for i in range(1, c + 1)]

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
    plt.xlabel("Nombre d'utilisateurs concurrents (PARAM = C)")
    plt.ylabel("Temps moyen par requête (ms)")
    plt.title("Performance vs concurrence (timeline)")
    plt.tight_layout()
    plt.savefig(OUTPUT_PNG)
    plt.close()
    print(f"Graphique enregistré dans {OUTPUT_PNG}")


def main():
    # 1) Seed
    seed_conc()

    # 2) Mesures
    rows = []
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["PARAM", "AVG_TIME", "RUN", "FAILED"])
        for c in CONCURRENCIES:
            for run in range(1, RUNS + 1):
                avg, failed = run_for_concurrency(c, run)
                writer.writerow([c, f"{avg:.3f}", run, failed])
                rows.append({"param": c, "avg": avg, "run": run, "failed": failed})

    print(f"CSV enregistré dans {OUTPUT_CSV}")
    make_bar_chart(rows)


if __name__ == "__main__":
    main()
