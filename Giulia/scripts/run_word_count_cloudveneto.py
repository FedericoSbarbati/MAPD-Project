#!/usr/bin/env python3
"""
Run Giulia's CORD-19 word count on the CloudVeneto Dask cluster.

Default topology, based on the current MAPD CloudVeneto setup:
  - scheduler / notebook VM: MAPD-project-1, 10.67.22.118
  - worker VM:               MAPD-project-2, 10.67.22.206
  - worker VM:               MAPD-project-3, 10.67.22.53

This script lives in Giulia/scripts inside the shared repository. It writes all
Giulia-specific outputs under Giulia/reports, so commits do not touch colleagues'
task folders.

By default the scheduler VM is kept out of the Dask worker pool. This is more
stable on small cldareapd.medium instances because the scheduler, SSH tunnel,
Jupyter/VS Code, and OS also need memory.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from pathlib import Path


DEFAULT_SCHEDULER_IP = "10.67.22.118"
DEFAULT_WORKER_IPS = ("10.67.22.206", "10.67.22.53")
DEFAULT_WORKER_MEMORY = "2500MiB"
DEFAULT_SPLIT_OUT = 12
DEFAULT_TOP_N = 50


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Start an SSHCluster on CloudVeneto and run the distributed word count."
    )
    parser.add_argument(
        "--project-root",
        default=os.environ.get("MAPD_PROJECT_ROOT", "/data/MAPD-Project"),
        help="Project directory on CloudVeneto. Default: /data/MAPD-Project.",
    )
    parser.add_argument(
        "--scheduler-ip",
        default=os.environ.get("MAPD_SCHEDULER_IP", DEFAULT_SCHEDULER_IP),
        help=f"Private IP of the scheduler VM. Default: {DEFAULT_SCHEDULER_IP}.",
    )
    parser.add_argument(
        "--worker-ips",
        default=os.environ.get("MAPD_WORKER_IPS", ",".join(DEFAULT_WORKER_IPS)),
        help="Comma-separated private worker IPs. Default: 10.67.22.206,10.67.22.53.",
    )
    parser.add_argument(
        "--use-scheduler-as-worker",
        action="store_true",
        default=parse_bool(os.environ.get("MAPD_USE_SCHEDULER_AS_WORKER", "0")),
        help="Also start one Dask worker on the scheduler VM. More throughput, less headroom.",
    )
    parser.add_argument(
        "--worker-memory",
        default=os.environ.get("MAPD_WORKER_MEMORY", DEFAULT_WORKER_MEMORY),
        help=f"Memory limit for each Dask worker. Default: {DEFAULT_WORKER_MEMORY}.",
    )
    parser.add_argument(
        "--threads-per-worker",
        type=int,
        default=int(os.environ.get("MAPD_THREADS_PER_WORKER", "1")),
        help="Threads per worker. Default: 1 to avoid GIL and memory spikes.",
    )
    parser.add_argument(
        "--split-out",
        type=int,
        default=int(os.environ.get("MAPD_SPLIT_OUT", str(DEFAULT_SPLIT_OUT))),
        help=f"Dask groupby split_out. Default: {DEFAULT_SPLIT_OUT}.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=int(os.environ.get("MAPD_TOP_N", str(DEFAULT_TOP_N))),
        help=f"Number of top words to export. Default: {DEFAULT_TOP_N}.",
    )
    parser.add_argument(
        "--run-name",
        default=os.environ.get("MAPD_WORDCOUNT_RUN_NAME", "giulia_word_count_cloudveneto"),
        help="Output folder name under Giulia/reports/. Default: giulia_word_count_cloudveneto.",
    )
    parser.add_argument(
        "--write-document-counts",
        action="store_true",
        help="Also write the intermediate per-document counts. This can be large.",
    )
    parser.add_argument(
        "--no-exclude-reference-like",
        action="store_true",
        help="Keep paragraphs flagged as reference-like. Default is to exclude them.",
    )
    return parser.parse_args(argv)


def configure_worker_memory() -> None:
    import dask

    pre_spawn_env = dict(dask.config.get("distributed.nanny.pre-spawn-environ", {}))
    pre_spawn_env.update(
        {
            "MALLOC_TRIM_THRESHOLD_": "0",
            "MALLOC_ARENA_MAX": "2",
        }
    )
    dask.config.set(
        {
            "distributed.nanny.pre-spawn-environ": pre_spawn_env,
            "distributed.worker.memory.target": 0.60,
            "distributed.worker.memory.spill": 0.70,
            "distributed.worker.memory.pause": 0.82,
            "distributed.worker.memory.terminate": 0.95,
        }
    )


def worker_probe(input_path: str, local_dir: str) -> dict[str, object]:
    from pathlib import Path

    Path(local_dir).mkdir(parents=True, exist_ok=True)
    return {
        "host": socket.gethostname(),
        "input_exists": Path(input_path).exists(),
        "input": input_path,
        "local_dir": local_dir,
        "pid": os.getpid(),
    }


def memory_sweep() -> dict[str, object]:
    import ctypes
    import ctypes.util
    import gc

    import pyarrow as pa

    collected = gc.collect()
    pa.default_memory_pool().release_unused()
    trimmed = False
    try:
        libc_path = ctypes.util.find_library("c")
        if libc_path:
            libc = ctypes.CDLL(libc_path)
            trimmed = bool(libc.malloc_trim(0))
    except Exception:
        trimmed = False
    return {"host": socket.gethostname(), "gc_collected": collected, "malloc_trim": trimmed}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    root = Path(args.project_root).expanduser().resolve()
    giulia_root = root / "Giulia"
    script_dir = Path(__file__).resolve().parent
    input_path = root / "data" / "silver" / "paragraphs"
    output_dir = giulia_root / "reports" / args.run_name
    worker_local_dir = giulia_root / "dask-worker-space"
    venv_python = giulia_root / ".venv" / "bin" / "python"
    remote_python = str(venv_python if venv_python.exists() else Path(sys.executable))
    worker_ips = csv_list(args.worker_ips)

    if not root.exists():
        raise SystemExit(f"Project root not found: {root}")
    if not giulia_root.exists():
        raise SystemExit(f"Giulia folder not found: {giulia_root}")
    if not input_path.exists():
        raise SystemExit(
            f"Input dataset not found: {input_path}\n"
            "On CloudVeneto, mount/copy the clean data so that "
            "/data/MAPD-Project/data/silver/paragraphs exists."
        )
    if not worker_ips:
        raise SystemExit("No worker IPs configured. Set --worker-ips or MAPD_WORKER_IPS.")

    hosts = [args.scheduler_ip]
    if args.use_scheduler_as_worker:
        hosts.append(args.scheduler_ip)
    hosts.extend(worker_ips)
    expected_workers = len(hosts) - 1

    os.chdir(root)
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))

    print("CloudVeneto distributed word count")
    print("project root       :", root)
    print("giulia root        :", giulia_root)
    print("input              :", input_path)
    print("output             :", output_dir)
    print("scheduler          :", args.scheduler_ip)
    print("worker IPs         :", worker_ips)
    print("scheduler as worker:", args.use_scheduler_as_worker)
    print("expected workers   :", expected_workers)
    print("worker memory      :", args.worker_memory)
    print("threads/worker     :", args.threads_per_worker)
    print("split_out          :", args.split_out)
    print("remote python      :", remote_python)

    configure_worker_memory()

    from dask.distributed import Client, SSHCluster

    cluster = None
    client = None
    started = time.perf_counter()
    try:
        cluster = SSHCluster(
            hosts=hosts,
            connect_options={
                "username": "ubuntu",
                "known_hosts": None,
            },
            scheduler_options={
                "host": args.scheduler_ip,
                "port": 8786,
                "dashboard_address": ":8787",
            },
            worker_options={
                "n_workers": 1,
                "nthreads": args.threads_per_worker,
                "memory_limit": args.worker_memory,
                "local_directory": str(worker_local_dir),
                "death_timeout": "120s",
            },
            remote_python=remote_python,
        )
        client = Client(cluster)
        client.wait_for_workers(expected_workers, timeout=180)

        print("scheduler address  :", client.scheduler.address)
        print("dashboard          :", client.dashboard_link)
        print("startup seconds    :", round(time.perf_counter() - started, 2))

        probes = client.run(worker_probe, str(input_path), str(worker_local_dir))
        for address, probe in probes.items():
            print("worker probe       :", address, probe)
            if not probe["input_exists"]:
                raise SystemExit(
                    "At least one worker cannot see the input dataset. "
                    "Run `bash scripts/cluster_storage_up.sh "
                    + " ".join(worker_ips)
                    + "` on the scheduler VM, then retry."
                )

        client.run(memory_sweep)

        import word_count_dask

        word_count_args = [
            "--input",
            str(input_path),
            "--output-dir",
            str(output_dir),
            "--scheduler",
            client.scheduler.address,
            "--split-out",
            str(args.split_out),
            "--top-n",
            str(args.top_n),
        ]
        if not args.no_exclude_reference_like:
            word_count_args.append("--exclude-reference-like")
        if args.write_document_counts:
            word_count_args.append("--write-document-counts")

        print("\nLaunching MapReduce word count...")
        status = word_count_dask.main(word_count_args)
        if client.status != "closed":
            client.run(memory_sweep)
        return int(status)
    finally:
        if client is not None:
            client.close()
        if cluster is not None:
            cluster.close()


if __name__ == "__main__":
    raise SystemExit(main())
