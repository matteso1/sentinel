# Sentinel Demo: Historical Data Replay
#
# This script replays Madison Metro bus prediction data through Sentinel,
# demonstrating Sentinel as a real message queue handling real-world data.
#
# Usage:
#   1. Start Sentinel server: go run ./cmd/sentinel-server --port 9092
#   2. Run this script: python demo_replay.py
#   3. The script will stream historical bus predictions through Sentinel

import subprocess
import json
import csv
import os
import time
import glob
from datetime import datetime
from pathlib import Path

# Configuration
SENTINEL_HOST = "localhost"
SENTINEL_PORT = 9092
DATA_DIR = Path(r"C:\Users\nilsm\Desktop\VSCODE PROJECTS\resume\madison-bus-eta\backend\collected_data")
TOPIC = "bus-predictions"

def run_sentinel_cli(command, *args):
    """Run sentinel-cli command and return output."""
    cmd = [
        "go", "run", "./cmd/sentinel-cli",
        command,
        "-broker", f"{SENTINEL_HOST}:{SENTINEL_PORT}",
    ] + list(args)
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )
    if result.returncode != 0:
        print(f"    DEBUG: {result.stderr.strip()[:100]}" if result.stderr else "")
    return result.returncode == 0, result.stdout, result.stderr

def produce_message(topic, message):
    """Send a single message to Sentinel."""
    # Escape the message for command line
    escaped = message.replace('"', '\\"')
    success, stdout, stderr = run_sentinel_cli(
        "produce",
        "-topic", topic,
        "-partition", "0",
        "-message", escaped,
        "-count", "1"
    )
    return success

def load_csv_file(filepath):
    """Load a CSV file and return rows as dicts."""
    rows = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
    return rows

def replay_predictions(limit=100, delay_ms=100):
    """
    Replay historical prediction data through Sentinel.
    
    Args:
        limit: Max number of records to replay
        delay_ms: Delay between messages in milliseconds
    """
    print("=" * 60)
    print("SENTINEL DEMO: Historical Data Replay")
    print("=" * 60)
    print()
    print(f"Data source: {DATA_DIR}")
    print(f"Topic: {TOPIC}")
    print(f"Limit: {limit} records")
    print()
    
    # Find prediction CSV files
    csv_files = sorted(glob.glob(str(DATA_DIR / "predictions_*.csv")))
    
    if not csv_files:
        print("ERROR: No prediction CSV files found!")
        print(f"Expected path: {DATA_DIR}")
        return
    
    print(f"Found {len(csv_files)} CSV files")
    print()
    
    # Use a sample of files from different times
    sample_files = [
        csv_files[0],                        # First file
        csv_files[len(csv_files) // 2],      # Middle file
        csv_files[-1]                        # Last file
    ]
    
    records_sent = 0
    start_time = time.time()
    
    for csv_file in sample_files:
        if records_sent >= limit:
            break
            
        filename = os.path.basename(csv_file)
        print(f"Processing: {filename}")
        
        rows = load_csv_file(csv_file)
        
        for row in rows[:min(30, limit - records_sent)]:  # 30 per file max
            # Create JSON message from prediction data
            message = json.dumps({
                "route": row.get("rt", ""),
                "stop_id": row.get("stpid", ""),
                "stop_name": row.get("stpnm", ""),
                "direction": row.get("rtdir", ""),
                "predicted_time": row.get("prdtm", ""),
                "delay": row.get("dly", "False") == "True",
                "countdown_min": row.get("prdctdn", ""),
                "timestamp": datetime.now().isoformat()
            })
            
            success = produce_message(TOPIC, message)
            
            if success:
                records_sent += 1
                route = row.get("rt", "?")
                stop = row.get("stpnm", "?")[:25]
                print(f"  [{records_sent:3d}] Route {route:>3} → {stop}")
            else:
                print(f"  [ERR] Failed to send record")
            
            if delay_ms > 0:
                time.sleep(delay_ms / 1000)
    
    elapsed = time.time() - start_time
    rate = records_sent / elapsed if elapsed > 0 else 0
    
    print()
    print("=" * 60)
    print(f"COMPLETE: Sent {records_sent} records in {elapsed:.2f}s")
    print(f"Rate: {rate:.1f} msg/sec")
    print("=" * 60)
    print()
    print("To consume these messages:")
    print(f"  go run ./cmd/sentinel-cli consume -broker {SENTINEL_HOST}:{SENTINEL_PORT} -topic {TOPIC} -from-beginning")

def quick_demo():
    """Quick demo with just 10 messages."""
    replay_predictions(limit=10, delay_ms=200)

def full_replay():
    """Full replay with 100 messages."""
    replay_predictions(limit=100, delay_ms=50)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--full":
        full_replay()
    else:
        quick_demo()
