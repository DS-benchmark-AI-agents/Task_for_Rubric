#!/usr/bin/env python3
"""
Data Preparation Script for NOBIL EV Charging Data
Processes a sample of Avro snapshot files using fastavro to perform missing value analysis and build hourly occupancy time series.
"""
import os
import sys
import json
from glob import glob
from fastavro import reader
import pandas as pd

def find_avro_files(root_dir):
    avro_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            if fname.endswith('.avro'):
                avro_files.append(os.path.join(dirpath, fname))
    return sorted(avro_files)


def parse_avro_file(file_path):
    """
    Parse an Avro file using avro library to extract charging records.
    """
    records = []
    with open(file_path, 'rb') as fo:
        avro_reader = reader(fo)
        for rec in avro_reader:
            try:
                body = rec.get('Body')
                if body is None:
                    continue
                payload = json.loads(body.decode('utf-8'))
                records.append({
                    'station_id': payload.get('nobilId'),
                    'evse_id': payload.get('evseUId'),
                    'status': payload.get('status'),
                    'timestamp': payload.get('timestamp')
                })
            except Exception:
                continue
    return records


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Data Preparation for NOBIL EV data')
    parser.add_argument('--input_dir', default='/root/data/nobil-realtime_datadump', help='Root directory of Avro dumps')
    parser.add_argument('--sample_size', type=int, default=500, help='Number of Avro files to sample')
    args = parser.parse_args()

    avro_files = find_avro_files(args.input_dir)
    print(f"Found {len(avro_files)} Avro files.")
    if not avro_files:
        print("No Avro files found. Exiting.")
        sys.exit(1)

    sample_files = avro_files[:args.sample_size]
    print(f"Processing sample of {len(sample_files)} files...")
    all_recs = []
    for fp in sample_files:
        recs = parse_avro_file(fp)
        all_recs.extend(recs)
    print(f"Parsed {len(all_recs)} records from sample.")

    # DataFrame
    df = pd.DataFrame(all_recs)
    df = df.dropna(subset=['station_id', 'evse_id', 'status', 'timestamp'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    df = df.dropna(subset=['timestamp'])

    # Missing value analysis for 'UNKNOWN'
    status_counts = df['status'].value_counts(dropna=False)
    total = len(df)
    unknown_count = status_counts.get('UNKNOWN', 0)
    unknown_pct = unknown_count / total * 100
    print(f"UNKNOWN status: {unknown_count}/{total} ({unknown_pct:.2f}%)")

    # Exclude UNKNOWN
    df_clean = df[df['status'] != 'UNKNOWN'].copy()

    # Build hourly occupancy time series per station
    df_clean['hour'] = df_clean['timestamp'].dt.floor('H')
    agg = df_clean.groupby(['station_id', 'hour', 'status']).size().unstack(fill_value=0)
    agg['total'] = agg.sum(axis=1)
    agg['charging'] = agg.get('CHARGING', 0)
    agg['occupancy_rate'] = agg['charging'] / agg['total']

    # Prepare output dir
    outdir = '/root/output/data'
    os.makedirs(outdir, exist_ok=True)

    # Save status summary
    status_summary = pd.DataFrame({ 'status': status_counts.index, 'count': status_counts.values })
    status_summary.to_csv(os.path.join(outdir, 'status_summary.csv'), index=False)
    agg.to_parquet(os.path.join(outdir, 'occupancy_station_hourly.parquet'))

    # Per EVSE
    agg_evse = df_clean.groupby(['evse_id', 'hour', 'status']).size().unstack(fill_value=0)
    agg_evse['total'] = agg_evse.sum(axis=1)
    agg_evse['charging'] = agg_evse.get('CHARGING', 0)
    agg_evse['occupancy_rate'] = agg_evse['charging'] / agg_evse['total']
    agg_evse.to_parquet(os.path.join(outdir, 'occupancy_evse_hourly.parquet'))

    # Save flattened sample
    df_clean.to_parquet(os.path.join(outdir, 'flattened_sample.parquet'))

    # Save unknown analysis
    with open(os.path.join(outdir, 'unknown_analysis.txt'), 'w') as f:
        f.write(f"Total records: {total}\n")
        f.write(f"UNKNOWN count: {unknown_count}\n")
        f.write(f"UNKNOWN percentage: {unknown_pct:.2f}%\n")
        f.write("Decision: Excluded 'UNKNOWN' status from occupancy metrics.\n")

    print("Data preparation complete.")

if __name__ == '__main__':
    main()
