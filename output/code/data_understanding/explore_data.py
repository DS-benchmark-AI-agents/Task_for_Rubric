#!/usr/bin/env python3
import os
import pandas as pd

data_dir = "/root/data/nobil-realtime_datadump"

def get_file_list():
    files = []
    for root, dirs, filenames in os.walk(data_dir):
        for f in filenames:
            if f.lower().endswith(('.csv', '.json')):
                files.append(os.path.join(root, f))
    return sorted(files)


def inspect_file(path, nrows=1000):
    print(f"File: {path}")
    try:
        if path.lower().endswith('.csv'):
            df = pd.read_csv(path, nrows=nrows)
        else:
            df = pd.read_json(path, lines=True, nrows=nrows)
        print(f"  Rows read: {len(df)}")
        print(f"  Columns: {df.columns.tolist()}")
        print("  Data types:")
        for col, typ in df.dtypes.items():
            print(f"    {col}: {typ}")
        missing = df.isnull().sum()
        missing = missing[missing > 0]
        print("  Missing values per column:")
        if not missing.empty:
            for col, cnt in missing.items():
                print(f"    {col}: {cnt}")
        else:
            print("    None")
    except Exception as e:
        print(f"  Error reading file: {e}")
    print("-" * 60)


def main():
    files = get_file_list()
    print(f"Total files found: {len(files)}")
    sample = files[:5]
    print(f"Inspecting first {len(sample)} files:\n")
    for f in sample:
        inspect_file(f)

if __name__ == '__main__':
    main()