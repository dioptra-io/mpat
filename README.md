# MPAT: Measurement Platform Analysis Tool

<p align="center">
  <img src="assets/mpat-logo.png" alt="MPAT Logo" width="200"/>
</p>

MPAT is a high-performance command-line tool for extracting and analyzing Internet-scale route tracing measurements. It enables researchers and network operators to retrieve, store, and query traceroute-like probe data from measurement platforms at scale.

MPAT is part of Sorbonne's **IP Route Survey** (IPRS) initiative: [https://iprs.dioptra.io](https://iprs.dioptra.io)

---

## Overview

Internet Measurement Platforms (IMPs) such as [Iris](https://iris.dioptra.io) collect massive amounts of probe data for observing network behavior. However, retrieving, storing, and querying this data at scale requires efficient tooling.

**MPAT** addresses this by providing:

- A client for the Iris measurement platform API.
- A high-throughput pipeline for fetching probe results into a local ClickHouse instance.
- A computation pipeline for deriving higher-level structures such as Forwarding Info Elements (FIEs) from raw probe results.
- A flexible query interface for filtering measurements by state, date range, and tag.

---

## Architecture

MPAT is structured around two internal packages:

- **`internal/iris`** — Client for the Iris API. Handles JWT authentication, measurement queries, and ClickHouse result retrieval via HTTP streaming.
- **`internal/store`** — Client for the local ClickHouse instance. Handles table creation, write policies, bulk insertion, and derived table computation.

Data flows as follows:

```
Iris ClickHouse  →  mp fetch (HTTP stream)  →  Local ClickHouse
                                                      ↓
                                               mp compute
                                                      ↓
                                            Derived ClickHouse tables (e.g. FIEs)
```

No intermediate deserialization occurs during fetch — the JSON stream from Iris is piped directly into the local ClickHouse instance. Compute operations run entirely server-side within ClickHouse.

---

## Prerequisites

- Go 1.21+
- A running ClickHouse instance
- Access to the Iris measurement platform

---

## Installation

```bash
git clone https://github.com/dioptra-io/mpat.git
cd mpat
make install
```

This builds the `mp` binary and installs it to `$GOPATH/bin`.

---

## Environment Variables

| Variable          | Required | Description                                                        |
| ----------------- | -------- | ------------------------------------------------------------------ |
| `IRIS_USERNAME`   | Yes      | Iris account email                                                 |
| `IRIS_PASSWORD`   | Yes      | Iris account password                                              |
| `IRIS_ENDPOINT`   | No       | Iris API endpoint (default: `https://api.iris.dioptra.io`)         |
| `MPAT_CLICKHOUSE` | Yes      | ClickHouse DSN (e.g. `clickhouse://user:pass@localhost:9000/mpat`) |
| `MPAT_DATABASE`   | No       | Destination ClickHouse database (default: `mpat`)                  |

---

## Usage

### `mp fetch iris-results <dest-table>`

Fetches Iris probe results into a local ClickHouse table. Supports three source selection modes — exactly one must be specified.

#### Flags

| Flag            | Default    | Description                                            |
| --------------- | ---------- | ------------------------------------------------------ |
| `--policy`      | `fail`     | Write policy: `replace`, `truncate`, `fail`, `append`  |
| `--table`       | —          | Mode 1: fetch a specific source table by name          |
| `--measurement` | —          | Mode 2: fetch all result tables for a measurement UUID |
| `--from`        | —          | Mode 3: start of date range (RFC3339)                  |
| `--to`          | —          | Mode 3: end of date range (RFC3339)                    |
| `--state`       | `finished` | Mode 3: filter measurements by state                   |
| `--tag`         | —          | Mode 3: filter measurements by tag (regex)             |

#### Write Policies

| Policy     | Behaviour                                                |
| ---------- | -------------------------------------------------------- |
| `replace`  | Drop destination table if it exists, recreate and insert |
| `truncate` | Truncate destination table if not empty, then insert     |
| `fail`     | Fail if destination table is not empty                   |
| `append`   | Insert into destination regardless of existing data      |

#### Mode 1 — Explicit table name

```bash
mp fetch iris-results my_results \
  --table results__b78e5bf4_100a_4c20_af14__53863928_7a54_45de_b51a \
  --policy replace
```

#### Mode 2 — By measurement UUID

```bash
mp fetch iris-results my_results \
  --measurement b78e5bf4-100a-4c20-af14-311a9d43f8a0 \
  --policy replace
```

#### Mode 3 — By date range

```bash
mp fetch iris-results my_results \
  --from 2026-06-01T00:00:00Z \
  --to   2026-06-02T00:00:00Z \
  --policy replace
```

With optional filters:

```bash
mp fetch iris-results my_results \
  --from  2026-06-01T00:00:00Z \
  --to    2026-06-02T00:00:00Z \
  --state finished \
  --tag   "zeph" \
  --policy append
```

#### Example output

```
found 2 table(s)   policy: replace
total: 147 chunks across 2 table(s)
[1/2] results__b78e5bf4_...   58,365,836 rows   117 chunks
      chunk 1/1/147   |   4s   |   119,047 rows/s   |   Jun 2, 8:12pm (in ~10m14s)
      chunk 2/2/147   |   4s   |   123,451 rows/s   |   Jun 2, 8:11pm (in ~9m58s)
[2/2] results__c4a1_...   44,619,062 rows   30 chunks
      chunk 1/118/147   |   4s   |   121,951 rows/s   |   Jun 2, 8:10pm (in ~2m1s)
```

---

### `mp compute results-fies <input-table> <output-table>`

Computes Forwarding Info Elements (FIEs) from a raw Iris results table and writes them into a destination ClickHouse table. The computation runs entirely server-side within ClickHouse using keyset-paginated chunks, with no data movement through the client.

A FIE represents a observed forwarding step between two consecutive TTL hops within the same flow. Given a flow with probe TTL values `h` and `h+1`, a FIE captures the near router (at TTL `h`) and the far router (at TTL `h+1`) along with their associated timestamps and reply addresses. Only flows where each TTL hop has exactly one distinct reply address are included (skip policy).

#### Flags

| Flag               | Default   | Description                                          |
| ------------------ | --------- | ---------------------------------------------------- |
| `--database`       | `mpat`    | Destination ClickHouse database                      |
| `--chunk-size`     | `1000000` | Number of destination prefixes per chunk             |
| `--rtt-resolution` | `0.1`     | RTT resolution in milliseconds (Iris default: `0.1`) |

#### Example

```bash
mp compute results-fies iris_results__20260601 iris_fies__20260601
```

#### Example output

```
computing fies: iris_results__20260601 -> mpat.iris_fies__20260601
[chunk 1] cursor=::                       last=::ffff:1.199.230.219   rows=3384343  elapsed=3.5s   total=3384343
[chunk 2] cursor=::ffff:1.199.230.219     last=::ffff:2.x.x.x        rows=3291872  elapsed=3.2s   total=6676215
...
done: 156 chunks, 696001505 rows, elapsed=16m38s
```

#### Output table schema

| Column                    | Type       | Description                                |
| ------------------------- | ---------- | ------------------------------------------ |
| `sequence_number`         | `UInt64`   | Globally unique monotonic FIE identifier   |
| `agent_id`                | `IPv6`     | Probing agent (source address)             |
| `probing_directive_id`    | `UInt32`   | Always `0` for this computation            |
| `ip_version`              | `UInt8`    | IP version: `4` or `6`                     |
| `protocol`                | `UInt8`    | Probe protocol (ICMP=1, UDP=17, ICMPv6=58) |
| `source_address`          | `IPv6`     | Probe source address                       |
| `destination_address`     | `IPv6`     | Probe destination address                  |
| `near_probe_ttl`          | `UInt8`    | TTL of the near hop `h`                    |
| `near_reply_address`      | `IPv6`     | Reply address at TTL `h`                   |
| `near_sent_timestamp`     | `DateTime` | Probe send time at TTL `h`                 |
| `near_received_timestamp` | `DateTime` | Estimated reply receive time at TTL `h`    |
| `far_probe_ttl`           | `UInt8`    | TTL of the far hop `h+1`                   |
| `far_reply_address`       | `IPv6`     | Reply address at TTL `h+1`                 |
| `far_sent_timestamp`      | `DateTime` | Probe send time at TTL `h+1`               |
| `far_received_timestamp`  | `DateTime` | Estimated reply receive time at TTL `h+1`  |
| `production_timestamp`    | `DateTime` | Time at which this FIE was computed        |

The table is ordered by `(near_reply_address, destination_address, agent_id, production_timestamp)` for efficient queries grouping by forwarding hop and destination.

---

## Maintainers

- Ufuk Bombar – Sorbonne Université / LINCS · [contact@bombar.dev](mailto:contact@bombar.dev)

---

## License

MIT License
