#!/usr/bin/python

import argparse
import random
import socket
import struct
import timeit

import maxminddb_rust

from _common import resolve_database


def generate_ips(count):
    random.seed(0)
    return [
        socket.inet_ntoa(struct.pack("!L", random.getrandbits(32)))
        for _ in range(count)
    ]


parser = argparse.ArgumentParser(description="Benchmark maxminddb.")
parser.add_argument("--count", default=250000, type=int, help="number of lookups")
parser.add_argument("--mode", default=0, type=int, help="reader mode to use")
parser.add_argument(
    "--file",
    default=None,
    help="path to mmdb file (defaults to an installed database under /var/lib/GeoIP)",
)

args = parser.parse_args()

database = resolve_database(args.file)
reader = maxminddb_rust.open_database(database, args.mode)
ips = generate_ips(args.count)


def lookup_ip_addresses() -> None:
    for ip in ips:
        reader.get(ip)


elapsed = timeit.timeit(
    lookup_ip_addresses,
    number=1,
)

print(f"{int(args.count / elapsed):,}", "lookups per second")
