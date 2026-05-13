# MPAT

**MPAT** (Measurement Platform Analysis Tool) is a lightweight and extensible task-oriented platform for interacting with internet measurement infrastructures.

The goal of MPAT is to provide a unified and simple interface for collecting, managing, and analyzing measurement data from multiple platforms without exposing users to platform-specific operational complexity.

MPAT is designed around a worker-server architecture:

- A persistent server manages tasks
- Clients submit jobs through a simple CLI
- Tasks are queued and executed asynchronously
- Results and task metadata are stored locally

The system is intentionally minimal and automation-friendly.

---

# Design Goals

- Simple command-line interface
- Unified interaction model across platforms
- Asynchronous task execution
- Queue-based processing
- Persistent local task database
- Easy integration into analysis pipelines
- Remote worker execution support
- Extensible platform architecture

---

# Supported Platforms

| Platform Name | Backend Tool |
|---|---|
| `caida` | `ark` |
| `iprs` | `iris` |
| `iprl` | `retina` |

The internal implementation details are abstracted away from the user.

Users interact directly with the measurement tools:

```bash
mp get ark <args>
mp get iris <args>
mp get retina <args>
````

---

# Architecture

MPAT operates in two parts:

1. CLI client
2. Worker server

The server exposes an HTTP endpoint and processes incoming jobs.

The client submits commands to the server.

```text
+-------------+        HTTP        +----------------+
| CLI Client  | --------------->  | MPAT Server    |
+-------------+                   +----------------+
                                          |
                                          v
                                  +---------------+
                                  | Task Queue    |
                                  +---------------+
                                          |
                                          v
                                  +---------------+
                                  | Workers       |
                                  +---------------+
                                          |
                                          v
                                  +---------------+
                                  | Local DB      |
                                  +---------------+
```

---

# Running the Server

Start the MPAT worker server:

```bash
mp serve
```

By default this exposes:

```text
http://localhost:9293
```

The server:

* Accepts incoming tasks
* Queues jobs
* Executes tasks asynchronously
* Stores task metadata locally

---

# Worker Configuration

The number of concurrent workers can be configured:

```bash
mp serve --num-workers 4
```

This allows parallel execution of queued tasks.

---

# Submitting Tasks

Measurement jobs are submitted using the `get` command.

Examples:

```bash
mp get retina <args>
```

```bash
mp get iris <args>
```

```bash
mp get ark <args>
```

When a command is submitted:

1. A task is created
2. The task is persisted locally
3. The task enters the queue
4. A worker processes the task
5. Task state is updated

---

# Task Management

MPAT provides task inspection and queue management commands.

## List all tasks

```bash
mp ls
```

---

## Show queued and running tasks

```bash
mp queue
```

---

## Show completed tasks

```bash
mp done
```

---

## Cancel a task

```bash
mp cancel <task-id>
```

This removes the task from the queue or stops execution if possible.

---

# Task Lifecycle

```text
Queued -> Running -> Done
                \
                 -> Failed
```

Tasks are persisted in the local database for inspection and reproducibility.

---

# Local Database

The MPAT server maintains a local database containing:

* Task metadata
* Execution status
* Parameters
* Timing information
* Logs
* Results metadata

This allows:

* Persistent queues
* Recovery after restart
* Task history inspection
* Future analysis integration

---

# Example Workflow

Start the worker server:

```bash
mp serve --num-workers 2
```

Submit tasks:

```bash
mp get retina --target 1.1.1.1
```

```bash
mp get ark --from paris
```

Inspect the queue:

```bash
mp queue
```

List completed jobs:

```bash
mp done
```

---

# Long-Term Vision

MPAT is intended to evolve into a general-purpose internet measurement orchestration platform.

Planned directions include:

* Distributed workers
* Remote execution nodes
* Result querying
* Data export pipelines
* Plugin system
* Analysis modules
* Web dashboard
* Scheduling
* Streaming measurements
* Reproducible experiments

The core philosophy is:

> Simple commands, persistent execution, scalable analysis.

---

# Status

Early development.

The current focus is:

* Core task execution
* Queue management
* Worker orchestration
* Platform abstraction
* Stable CLI interface

---

# License

MIT Licence
