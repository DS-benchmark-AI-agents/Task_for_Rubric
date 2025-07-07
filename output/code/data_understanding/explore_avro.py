#!/usr/bin/env python3
import os
from fastavro import reader

data_dir = "/root/data/nobil-realtime_datadump"

def find_avro_files(limit=5):
    avros = []
    for root, dirs, files in os.walk(data_dir):
        for f in files:
            if f.endswith('.avro'):
                avros.append(os.path.join(root, f))
                if len(avros) >= limit:
                    return avros
    return avros

def inspect_avro(path, max_recs=5):
    print(f"Inspecting Avro file: {path}")
    try:
        with open(path, 'rb') as fo:
            avro_reader = reader(fo)
            schema = avro_reader.schema
            print("Schema fields:")
            for field in schema.get('fields', []):
                name = field.get('name')
                ftype = field.get('type')
                print(f"  - {name}: {ftype}")
            print("Sample records:")
            for i, rec in enumerate(avro_reader):
                if i >= max_recs:
                    break
                print(rec)
    except Exception as e:
        print(f"Error reading Avro: {e}")
    print("-"*60)

if __name__ == '__main__':
    avro_files = find_avro_files()
    print(f"Found {len(avro_files)} Avro files for sampling.\n")
    for p in avro_files:
        inspect_avro(p)
