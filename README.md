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
- A flexible query interface for filtering measurements by state, date range, and tag.

---

## Architecture

MPAT is structured around two internal packages:

- **`internal/iris`** — Client for the Iris API. Handles JWT authentication, measurement queries, and ClickHouse result retrieval via HTTP streaming.
- **`internal/store`** — Client for the local ClickHouse instance. Handles table creation, write policies, and bulk insertion.

Data flows as follows:

```
Iris ClickHouse  →  mp (HTTP stream)  →  Local ClickHouse
```

No intermediate deserialization occurs — the JSON stream from Iris is piped directly into the local ClickHouse instance.

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
  --tag   "diamond-miner.*" \
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

## Contributors

- Ufuk Bombar – Sorbonne Université / LINCS
- Timur Friedman – Sorbonne Université / LINCS
- Olivier Fourmaux – Sorbonne Université
- Kevin Vermeulen – LAAS-CNRS

---

## License

MIT License
