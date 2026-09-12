#!/usr/bin/python
"""Measure full-record lookup throughput."""

from __future__ import annotations

import argparse
import random
import socket
import struct
import timeit

from _common import resolve_database

import maxminddb_rust


def generate_ips(count: int) -> list[str]:
    """Generate a reproducible sequence of random IPv4 addresses."""
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
    """Look up each prepared IP address."""
    for ip in ips:
        reader.get(ip)


elapsed = timeit.timeit(
    lookup_ip_addresses,
    number=1,
)

print(f"{int(args.count / elapsed):,}", "lookups per second")
