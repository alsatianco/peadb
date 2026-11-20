#!/usr/bin/env python3
"""Real socket-target masters feed direct replicas: all four pairings with AOF/chain/restart."""
from _replication_direct_persistence import PEADB, reference, success


def main():
    for master, replica in [(reference(), reference()), (reference(), PEADB),
                            (PEADB, reference()), (PEADB, PEADB)]:
        success(master, replica, True)


if __name__ == '__main__':
    main()
