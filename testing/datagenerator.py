#!/usr/bin/env python3

import argparse
import os
import random
import struct

RECORD_SIZE = 128
KEY_SIZE = 4
PAYLOAD_SIZE = RECORD_SIZE - KEY_SIZE

def generate_record(key: int) -> bytes:
    key_bytes = struct.pack('<I', key)  # 4-byte little-endian unsigned int
    payload = os.urandom(PAYLOAD_SIZE)
    return key_bytes + payload

def main():
    parser = argparse.ArgumentParser(description='Generate binary record file for psort testing.')
    parser.add_argument('outfile', help='Output file name')
    parser.add_argument('num_records', type=int, help='Number of records to generate')
    parser.add_argument('--seed', type=int, default=None, help='Random seed (optional)')
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    with open(args.outfile, 'wb') as f:
        for _ in range(args.num_records):
            key = random.randint(0, 2**32 - 1)
            #print(key)
            f.write(generate_record(key))

    print(f"Wrote {args.num_records} records to {args.outfile}")

if __name__ == '__main__':
    main()
