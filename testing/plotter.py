#!/usr/bin/env python3
import json
import argparse
import matplotlib.pyplot as plt

def load_data(path):
    records = []
    read_times = []
    sort_times = []
    merge_times = []

    with open(path, 'r') as f:
        for line in f:
            obj = json.loads(line)
            # detect which metric this line holds
            if 'Records' in obj:
                records.append(obj['Records'])
            elif 'Read Phase Time Elapsed' in obj:
                read_times.append(obj['Read Phase Time Elapsed'])
            elif 'Sort Phase Elapsed Time' in obj:
                sort_times.append(obj['Sort Phase Elapsed Time'])
            elif 'Merge and Write Phase Time Elapsed' in obj:
                merge_times.append(obj['Merge and Write Phase Time Elapsed'])
            else:
                raise ValueError("bad data");
    # sanity‐check: all lists must be same length
    if not (len(records) == len(read_times) == len(sort_times) == len(merge_times)):
        raise RuntimeError("Mismatched lengths: "
                           f"records={len(records)}, read={len(read_times)}, "
                           f"sort={len(sort_times)}, merge={len(merge_times)}")
    return records, read_times, sort_times, merge_times

def plot_phases(records, read_times, sort_times, merge_times, save_path=None):
    plt.figure(figsize=(14,6))
    plt.plot(records, read_times,    marker=',', linewidth=1,linestyle=':', alpha=0.6, label='Read Phase')
    plt.plot(records, sort_times,    marker=',', linewidth=1, linestyle=':', alpha=0.6, label='Sort Phase')
    plt.plot(records, merge_times,   marker=',', linewidth=1, alpha=0.6,linestyle=':',  label='Merge & Write Phase')

    plt.xlabel('Record count')
    plt.ylabel('Time elapsed (s)')
    plt.title('Phase timings vs. input size')
    plt.legend()
    plt.grid(True)

    if save_path:
        plt.savefig(save_path, bbox_inches='tight')
        print(f"Saved plot to {save_path}")
    else:
        plt.show()

def main():
    p = argparse.ArgumentParser(
        description="Plot read/sort/merge times from a JSONL log."
    )
    p.add_argument('input', help="Path to .jsonl file")
    p.add_argument('-o', '--output', help="Path to save plot (e.g. plot.png)")
    args = p.parse_args()
    i = 0
    recs, read_t, sort_t, merge_t = load_data(args.input)
    while( i <  len(merge_t)):
        if read_t[i] > 1.5 or sort_t[i] > 1.5 or merge_t[i] > 1.5 :
            del merge_t[i]
            del recs[i]
            del read_t[i]
            del sort_t[i]
        i= i + 1
    plot_phases(recs, read_t, sort_t, merge_t, save_path=args.output)

if __name__ == '__main__':
    main()

