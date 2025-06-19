# MPAT: Measurement Pipeline for Adaptive Tracing
<p align="center">
  <img src="assets/mpat-logo.png" alt="MPAT Logo" width="200"/>
</p>

MPAT is a high-performance command-line tool for enhancing Internet-scale route tracing measurements. It enables researchers and network operators to extract and analyze forwarding behavior from traceroute-like data and compute key metrics to assess the quality and coverage of measurements.

---

## Overview

Internet Measurement Platforms (IMPs) like CAIDA Ark and RIPE Atlas collect massive amounts of probe data for observing network behavior. However, interpreting and acting on this data can be difficult due to its scale and complexity.

**MPAT** addresses this challenge by:

- Constructing forwarding information from traceroute data.
- Calculating route quality metrics.
- Guiding informed and adaptive probing strategies.

MPAT is part of Sorbonne’s **IP Route Survey** (IPRS) initiative: [https://iprs.dioptra.io](https://iprs.dioptra.io)

---

## Features

- 📦 **Go-based** implementation for high concurrency and performance.
- ⚡ **ClickHouse** integration for efficient data storage and querying.
- 🧱 Modular pipeline design — each processing stage is independently runnable or chainable.
- 📈 Computation of:
  - **Forwarding Decisions**
  - **Forwarding Information Tuples**
  - **Route Score**
  - **Route Completeness**
- 🎯 Adaptive probing support based on gaps in observed paths.

---

## Concepts

### Forwarding Information

Forwarding info summarizes how a router interface routes toward a set of prefixes. It is a 3-tuple:
```

(an, d, Af)

````
Where:
- `an`: router interface address
- `d`: destination prefix
- `Af`: set of next-hop addresses

The implementation uses a radix-tree.

### Metrics

- **Route Score**: Number of distinct /24 (or /48) prefixes a router interface forwards to.
- **Route Completeness**: Fraction of public IP space observed through a given router.

---

## Architecture

MPAT pipeline stages:

1. **Ingestion** – Load raw probe data into ClickHouse.
2. **Normalization** – Convert probe data into a standard format.
3. **Forwarding Decision Computation** – Derive routing steps.
4. **Forwarding Info Extraction** – Infer router behavior.
5. **Metric Calculation** – Quantify measurement coverage.
6. **Adaptive Probing** – Trigger probes to fill in missing data.

---

## Getting Started

> 📌 **Prerequisites**:
> - Go 1.18+
> - ClickHouse Server
> - Access to traceroute-like raw probe data

### Clone and Build

```bash
git clone https://github.com/your-org/mpat.git
cd mpat
go build -o mpat ./cmd/mpat
````

### Configuration

Set up ClickHouse and ensure credentials and target schema are properly configured in `config.yaml`.

---

## Usage

Basic pipeline:

```bash
./mpat upload iris-results '2025-05-05'
TBD
```

Each step can also be executed independently or integrated into a larger system like IPRS.

---

## Roadmap

- [ ] Retrieving data from:
    - [x] Iris production instance
    - [x] Ark dataset
    - [ ] RIPE dataset
- [ ] Forwarding decision computation 
- [ ] Metric computation
    - [ ] Route score 
    - [ ] Route completeness 

---

## Contributors

* Ufuk Bombar – Sorbonne Université / LINCS
* Timur Friedman – Sorbonne Université / LINCS
* Olivier Fourmaux – Sorbonne Université
* Kevin Vermeulen – LAAS-CNRS

---


## License

MIT License


