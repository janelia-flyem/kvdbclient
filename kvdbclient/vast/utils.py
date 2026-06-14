"""VAST-specific helpers (row-key <-> Arrow encoding, timestamp conversion,
key-list chunking against VAST request-size caps, etc.).

Scaffold (pcgvast-0001): populated alongside the primitives in later slices,
mirroring ``kvdbclient.bigtable.utils`` / ``kvdbclient.hbase.utils``.
"""
