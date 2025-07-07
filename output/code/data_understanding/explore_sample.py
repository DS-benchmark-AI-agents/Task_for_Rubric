#!/usr/bin/env python3
import os
import json
from fastavro import reader
from collections import Counter, defaultdict

data_dir = "/root/data/nobil-realtime_datadump"
# Limit number of files to sample
FILE_LIMIT = 50

def find_avro_files(limit=FILE_LIMIT):
    avros = []
    for root, dirs, files in os.walk(data_dir):
        for f in files:
            if f.endswith('.avro'):
                avros.append(os.path.join(root, f))
                if len(avros) >= limit:
                    return avros
    return avros


def process_files(files):
    total_records = 0
    unique_stations = set()
    unique_evses = set()
    status_counter = Counter()
    snapshot_sizes = []

    for f in files:
        recs = []
        try:
            with open(f, 'rb') as fo:
                avro_reader = reader(fo)
                for rec in avro_reader:
                    body = rec.get('Body')
                    if body is None:
                        continue
                    try:
                        # Body is bytes
                        obj = json.loads(body.decode('utf-8'))
                    except Exception:
                        obj = json.loads(body)
                    nobilId = obj.get('nobilId')
                    evseUId = obj.get('evseUId')
                    status = obj.get('status')
                    # accumulate
                    total_records += 1
                    unique_stations.add(nobilId)
                    unique_evses.add(evseUId)
                    status_counter[status] += 1
                    recs.append(rec)
        except Exception as e:
            print(f"Error reading {f}: {e}")
        snapshot_sizes.append(len(recs))

    return {
        'files_sampled': len(files),
        'total_records': total_records,
        'unique_stations': len(unique_stations),
        'unique_evses': len(unique_evses),
        'status_distribution': status_counter,
        'snapshot_size_stats': {
            'min': min(snapshot_sizes) if snapshot_sizes else 0,
            'max': max(snapshot_sizes) if snapshot_sizes else 0,
            'mean': sum(snapshot_sizes)/len(snapshot_sizes) if snapshot_sizes else 0
        }
    }

if __name__ == '__main__':
    files = find_avro_files()
    stats = process_files(files)
    print("Sample Data Understanding Summary")
    print(f"Files sampled: {stats['files_sampled']}")
    print(f"Total records in sample: {stats['total_records']}")
    print(f"Unique station IDs: {stats['unique_stations']}")
    print(f"Unique EVSE IDs: {stats['unique_evses']}")
    print("Status distribution:")
    for status, cnt in stats['status_distribution'].most_common():
        print(f"  {status}: {cnt}")
    print("Snapshot size statistics (records per file):")
    for k, v in stats['snapshot_size_stats'].items():
        print(f"  {k}: {v:.2f}")
