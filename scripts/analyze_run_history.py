import csv
import sys
from datetime import datetime

# Opsional: path histori lain, misalnya data/metrics/run_history_airflow.csv
SOURCE = sys.argv[1] if len(sys.argv) > 1 else "data/input/run_history.csv"

def to_minutes(hhmm):
    if not hhmm:
        return None
    h, m = map(int, hhmm.split(":"))
    return h * 60 + m

with open(SOURCE, newline="", encoding="utf-8") as f:
    runs = list(csv.DictReader(f))

total = len(runs)
failed = sum(1 for r in runs if r["status"].lower() == "failed")
success = [r for r in runs if r["status"].lower() == "success"]
durations = [float(r["duration_seconds"]) for r in success]
latencies = [
    to_minutes(r["data_ready_at"]) - to_minutes(r["scheduled_ready_at"])
    for r in success if r["data_ready_at"]
]

print("=== HISTORICAL PIPELINE HEALTH ===")
print(f"Total runs             : {total}")
print(f"Failed runs            : {failed}")
print(f"Failure rate           : {failed / total * 100:.2f}%")
if durations:
    print(f"Average successful duration: {sum(durations) / len(durations):.1f} seconds")
else:
    print("Average successful duration: - (belum ada run sukses)")
if latencies:
    print(f"Average data latency   : {sum(latencies) / len(latencies):.1f} minutes")
    print(f"Maximum data latency   : {max(latencies):.1f} minutes")
