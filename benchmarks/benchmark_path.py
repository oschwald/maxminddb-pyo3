#!/usr/bin/python
"""Compare full-record and selective-path decoding throughput."""

import argparse
import random
import socket
import struct
import timeit

from _common import resolve_database

import maxminddb_rust

parser = argparse.ArgumentParser(description="Benchmark maxminddb get vs get_path.")
parser.add_argument("--count", default=250000, type=int, help="number of lookups")
parser.add_argument("--batch-size", default=100, type=int, help="batch size")
parser.add_argument(
    "--file",
    default=None,
    help="path to mmdb file (defaults to an installed database under /var/lib/GeoIP)",
)

args = parser.parse_args()
if args.batch_size <= 0:
    msg = "--batch-size must be positive"
    raise ValueError(msg)

random.seed(0)
database = resolve_database(args.file)
reader = maxminddb_rust.open_database(database)

# Pre-generate IPs to ensure fair comparison (though random lookup overhead is small)
ips = [
    socket.inet_ntoa(struct.pack("!L", random.getrandbits(32)))
    for _ in range(args.count)
]
batches = [
    ips[start : start + args.batch_size]
    for start in range(0, len(ips), args.batch_size)
]


def lookup_full() -> None:
    """Decode full records and extract their country codes."""
    for ip in ips:
        res = reader.get(ip)
        if res:
            res.get("country", {}).get("iso_code")


def lookup_path() -> None:
    """Read country codes through individual path lookups."""
    path = ("country", "iso_code")
    for ip in ips:
        reader.get_path(ip, path)


def lookup_many_path() -> None:
    """Read country codes through batched path lookups."""
    path = ("country", "iso_code")
    for batch in batches:
        reader.get_many_path(batch, path)


print(f"Benchmarking with {args.count:,} lookups...")

time_full = timeit.timeit(lookup_full, number=1)
print(f"Full record decode: {int(args.count / time_full):,} lookups per second")

time_path = timeit.timeit(lookup_path, number=1)
print(f"Path decode (get_path): {int(args.count / time_path):,} lookups per second")

time_many_path = timeit.timeit(lookup_many_path, number=1)
print(
    f"Batch path decode (get_many_path): "
    f"{int(args.count / time_many_path):,} lookups per second"
)

print(f"Speedup: {time_full / time_path:.2f}x")
print(f"Batch path speedup vs get_path: {time_path / time_many_path:.2f}x")
