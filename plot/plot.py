import time
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


LOG_FILE = '/logs/metric_log.csv'
PLOT_FILE = '/logs/error_distribution.png'
SLEEP_INTERVAL = 15  # seconds between plot updates


print("[plot] Starting plot service...")

while True:
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 0:
            df = pd.read_csv(LOG_FILE)
            if len(df) > 0:
                fig, ax = plt.subplots(figsize=(8, 5))
                ax.hist(df['absolute_error'], bins=20, color='steelblue', edgecolor='black', alpha=0.8)
                ax.set_title(f'Error Distribution (n={len(df)} samples)', fontsize=14)
                ax.set_xlabel('Absolute Error', fontsize=12)
                ax.set_ylabel('Count', fontsize=12)
                ax.grid(axis='y', linestyle='--', alpha=0.7)
                plt.tight_layout()
                plt.savefig(PLOT_FILE)
                plt.close(fig)
                print(f"[plot] Updated {PLOT_FILE} with {len(df)} records.")
            else:
                print("[plot] CSV is empty, skipping plot update.")
        else:
            print("[plot] Log file not found or empty, waiting...")
    except Exception as e:
        print(f"[plot] Error: {e}")

    time.sleep(SLEEP_INTERVAL)
