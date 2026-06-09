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
- A client for the RIPE Stat Data API for fetching BGP prefix data.
- A high-throughput pipeline for fetching probe results into a local ClickHouse instance.
- A computation pipeline for deriving higher-level structures such as Forwarding Info Elements (FIEs) from raw probe results.
- A flexible query interface for filtering measurements by state, date range, and tag.

---

## Architecture

MPAT is structured around the following internal packages:

- **`internal/iris`** — Client for the Iris API. Handles JWT authentication, measurement queries, and ClickHouse result retrieval via HTTP streaming.
- **`internal/ripe`** — Client for the RIPE Stat Data API. Handles BGP prefix queries using a builder pattern, with support for historical snapshots via time-of-day or raw timestamp.
- **`internal/store`** — Low-level client for the local ClickHouse instance. Handles table creation, write policies, and bulk insertion.
- **`internal/schema`** — Schema definitions for all supported table types (`results`, `resultslite`, `fies`, `ripeprefixes`). Provides schema introspection, compatibility checking, DDL rendering, and DDL parsing via the AfterShip ClickHouse SQL parser.
- **`internal/service`** — Business logic for fetch and compute operations. Each service owns its SQL templates and orchestrates store and client interactions.

Data flows as follows:

```
Iris ClickHouse      →  mp fetch iris-results (HTTP stream)  →  Local ClickHouse
                                                                       ↓
                                                              mp compute fies
                                                                       ↓
                                                         Derived tables (e.g. FIEs)

RIPE Stat API        →  mp fetch ripe-prefixes (native insert)  →  Local ClickHouse
```

No intermediate deserialization occurs during Iris fetch — the JSON stream is piped directly into ClickHouse. RIPE prefix data is inserted via the native ClickHouse driver. Compute operations run entirely server-side within ClickHouse.

---

## Prerequisites

- Go 1.23.4+
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

| Variable                  | Required | Description                                                        |
| ------------------------- | -------- | ------------------------------------------------------------------ |
| `IRIS_USERNAME`           | Yes      | Iris account email                                                 |
| `IRIS_PASSWORD`           | Yes      | Iris account password                                              |
| `IRIS_ENDPOINT`           | No       | Iris API endpoint (default: `https://api.iris.dioptra.io`)         |
| `MPAT_CLICKHOUSE`         | Yes      | ClickHouse DSN (e.g. `clickhouse://user:pass@localhost:9000/mpat`) |
| `MPAT_DATABASE`           | No       | Destination ClickHouse database (default: `mpat`)                  |
| `MPAT_RIPE_STAT_ENDPOINT` | No       | RIPE Stat API endpoint (default: `https://stat.ripe.net`)          |

---

## Usage

### `mp fetch iris-results <dest-table>`

Fetches Iris probe results into a local ClickHouse table. Supports four source selection modes — exactly one must be specified.

By default, only the columns required for downstream computation are fetched (`--lite`). Use `--lite=false` to fetch the full results schema.

#### Flags

| Flag            | Default    | Description                                                                   |
| --------------- | ---------- | ----------------------------------------------------------------------------- |
| `--policy`      | `fail`     | Write policy: `replace`, `truncate`, `fail`, `append`                         |
| `--database`    | `mpat`     | Destination ClickHouse database                                               |
| `--lite`        | `true`     | Use ResultsLiteSchema (fewer columns, faster fetch)                           |
| `--chunk-size`  | `500000`   | Number of rows per streaming chunk                                            |
| `--ewma-alpha`  | `0.2`      | Alpha parameter for ETA estimation                                            |
| `--table`       | —          | Mode 1: fetch a specific source table by name                                 |
| `--measurement` | —          | Mode 2: fetch all result tables for a measurement UUID                        |
| `--from`        | —          | Mode 3: start of date range (RFC3339)                                         |
| `--to`          | —          | Mode 3: end of date range (RFC3339)                                           |
| `--date`        | —          | Mode 4: date to fetch (YYYY-MM-DD), used with `--snapshot`                    |
| `--snapshot`    | —          | Mode 4: snapshot time: `4am-zeph`, `8am-zeph`, `4pm-zeph`, `8pm-zeph`, `ipv6` |
| `--state`       | `finished` | Measurement state filter (modes 3 and 4)                                      |
| `--tag`         | —          | Mode 3: tag regex filter                                                      |

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

#### Mode 4 — By date and snapshot

Fetches measurements for a specific date and snapshot time. Measurements are filtered to the full day (`00:00:00`–`23:59:59 UTC`) and further narrowed by the snapshot tag. `--state` is supported as an optional filter.

```bash
mp fetch iris-results my_results \
  --date     2026-06-01 \
  --snapshot 4am-zeph \
  --policy   replace
```

With optional state filter:

```bash
mp fetch iris-results my_results \
  --date     2026-06-01 \
  --snapshot 8pm-zeph \
  --state    finished \
  --policy   append
```

#### Example output

```
found 2 table(s), policy is set to 'replace'
total of 147 chunk(s) will be fetched.
[1/2] results__b78e5bf4_...   58,365,836 rows   117 chunks
      chunk 1/1/147   |   4s   |   119,047 rows/s   |   Jun 2, 8:12pm (in ~10m14s)
      chunk 2/2/147   |   4s   |   123,451 rows/s   |   Jun 2, 8:11pm (in ~9m58s)
[2/2] results__c4a1_...   44,619,062 rows   30 chunks
      chunk 1/118/147   |   4s   |   121,951 rows/s   |   Jun 2, 8:10pm (in ~2m1s)
```

---

### `mp fetch ripe-prefixes <dest-table>`

Fetches BGP prefixes originated by a set of ASes from the RIPE Stat RIS API and inserts them into a local ClickHouse table. Data is retrieved from historical RIS snapshots, which are available three times per day at 00:00, 08:00, and 16:00 UTC.

Exactly one of `--asns` or `--tier1` must be specified to select the ASes to query. Exactly one of `--date` or `--timestamp` must be specified to select the snapshot time.

#### Flags

| Flag            | Default | Description                                                                      |
| --------------- | ------- | -------------------------------------------------------------------------------- |
| `--policy`      | `fail`  | Write policy: `replace`, `truncate`, `fail`, `append`                            |
| `--database`    | `mpat`  | Destination ClickHouse database                                                  |
| `--asns`        | —       | Comma-separated list of ASNs (e.g. `3356,1299,3257`)                             |
| `--tier1`       | `false` | Use the hardcoded list of 16 tier-1 ASNs                                         |
| `--date`        | —       | Date for the snapshot (e.g. `2026-06-01`), used with `--snapshot`                |
| `--snapshot`    | `dawn`  | Time of day: `dawn` (08:00 UTC), `day` (16:00 UTC), `night` (00:00 UTC next day) |
| `--timestamp`   | —       | Raw RFC3339 timestamp, alternative to `--date` + `--snapshot`                    |
| `--max-retries` | `10`    | Maximum number of retry attempts on failure                                      |
| `--retry-delay` | `5s`    | Duration to wait between retry attempts                                          |

#### Write Policies

| Policy     | Behaviour                                                |
| ---------- | -------------------------------------------------------- |
| `replace`  | Drop destination table if it exists, recreate and insert |
| `truncate` | Truncate destination table if not empty, then insert     |
| `fail`     | Fail if destination table is not empty                   |
| `append`   | Insert into destination regardless of existing data      |

#### Tier-1 ASNs

The `--tier1` flag uses the following 16 ASNs:

| ASN   | Operator                 |
| ----- | ------------------------ |
| 3356  | Lumen (Level 3)          |
| 1299  | Arelion                  |
| 3257  | GTT                      |
| 2914  | NTT                      |
| 6453  | Tata                     |
| 6461  | Zayo                     |
| 6762  | Sparkle (Telecom Italia) |
| 3491  | PCCW Global              |
| 5511  | Orange                   |
| 12956 | Telxius (Telefonica)     |
| 3320  | Deutsche Telekom         |
| 6830  | Liberty Global           |
| 7018  | AT&T                     |
| 701   | Verizon                  |
| 174   | Cogent                   |
| 6939  | Hurricane Electric       |

#### Examples

```bash
# Fetch tier-1 ASNs at dawn on June 1st 2026
mp fetch ripe-prefixes ripeprefixes_20260601 \
  --tier1 \
  --date 2026-06-01 \
  --snapshot dawn

# Fetch tier-1 ASNs at night (resolves to 2026-06-02 00:00 UTC)
mp fetch ripe-prefixes ripeprefixes_20260601 \
  --tier1 \
  --date 2026-06-01 \
  --snapshot night

# Fetch specific ASNs using a raw timestamp
mp fetch ripe-prefixes ripeprefixes_20260601 \
  --asns 3356,1299,3257 \
  --timestamp 2026-06-01T08:00:00Z

# Append a second snapshot to an existing table
mp fetch ripe-prefixes ripeprefixes_20260601 \
  --tier1 \
  --date 2026-06-01 \
  --snapshot day \
  --policy append

# Custom retry configuration
mp fetch ripe-prefixes ripeprefixes_20260601 \
  --tier1 \
  --date 2026-06-01 \
  --snapshot dawn \
  --max-retries 5 \
  --retry-delay 10s
```

#### Output table schema

| Column       | Type       | Description                                      |
| ------------ | ---------- | ------------------------------------------------ |
| `asn`        | `UInt32`   | AS number                                        |
| `network`    | `IPv6`     | Prefix address (IPv4 mapped to `::ffff:x.x.x.x`) |
| `prefix_len` | `UInt8`    | Prefix length                                    |
| `ip_version` | `UInt8`    | MATERIALIZED: `4` for IPv4-mapped, `6` for IPv6  |
| `query_time` | `DateTime` | RIS snapshot time                                |
| `fetched_at` | `DateTime` | Time at which the data was fetched               |

The table is ordered by `(asn, network, prefix_len)` for efficient per-ASN queries and prefix lookups. It is designed to work with ClickHouse's `IP_TRIE` dictionary layout for fast prefix matching against other tables.

---

### `mp compute fies <input-table> <output-table>`

Computes Forwarding Info Elements (FIEs) from a raw Iris results table and writes them into a destination ClickHouse table. The computation runs entirely server-side within ClickHouse using keyset-paginated chunks, with no data movement through the client.

A FIE represents an observed forwarding step between two consecutive TTL hops within the same flow. Given a flow with probe TTL values `h` and `h+1`, a FIE captures the near router (at TTL `h`) and the far router (at TTL `h+1`) along with their associated timestamps and reply addresses. Only flows where each TTL hop has exactly one distinct reply address are included (skip policy).

Both `results` and `resultslite` source schemas are supported. The appropriate computation template is selected automatically based on the source table's schema.

#### Flags

| Flag               | Default   | Description                                           |
| ------------------ | --------- | ----------------------------------------------------- |
| `--policy`         | `append`  | Write policy: `replace`, `truncate`, `fail`, `append` |
| `--database`       | `mpat`    | Destination ClickHouse database                       |
| `--chunk-size`     | `1000000` | Number of destination prefixes per chunk              |
| `--rtt-resolution` | `0.1`     | RTT resolution in milliseconds (Iris default: `0.1`)  |

#### Example

```bash
mp compute fies iris_resultslite__20260601 iris_fies__20260601
```

#### Example output

```
computing [results to fies]: mpat.iris_resultslite__20260601 -> mpat.iris_fies__20260601
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
