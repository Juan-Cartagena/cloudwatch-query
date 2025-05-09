#!/usr/bin/env python3
"""
cloudwatch_query.py

Requisitos
pip install boto3


Uso:
  python cloudwatch_query.py <fecha_ini> <hora_ini> [<fecha_fin> <hora_fin>]

Ejemplo:
  python cloudwatch_query.py 2025-05-08 07:14
  python cloudwatch_query.py 2025-05-08 07:14 2025-05-08 08:00
"""

import sys
import time
from datetime import datetime, timezone
from typing import List

import boto3
from botocore.exceptions import ClientError

# ---------- Configuración ---------- #
LOG_GROUPS: List[str] = [
    "/aws/lambda/bm-qrec-api-redeban",
    "/aws/lambda/bm-qrec-authorizer",
    "/aws/lambda/bm-qrec-create-transaction",
    "/aws/lambda/bm-qrec-do-transaction",
]

QUERY_STRING = """
fields @timestamp, @logStream, @message
| sort @timestamp desc
| limit 20
"""
# ----------------------------------- #

logs = boto3.client("logs")


def to_epoch(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def parse_cli_time(date_str: str, time_str: str) -> datetime:
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")


def start_query(log_group: str, start_ts: int, end_ts: int) -> str:
    resp = logs.start_query(
        logGroupName=log_group,
        startTime=start_ts,
        endTime=end_ts,
        queryString=QUERY_STRING.strip(),
    )
    return resp["queryId"]


def wait_results(query_id: str, poll_interval: float = 1.0) -> list:
    while True:
        resp = logs.get_query_results(queryId=query_id)
        status = resp["status"]
        if status == "Complete":
            return resp["results"]
        if status in {"Cancelled", "Failed", "Timeout"}:
            raise RuntimeError(f"Consulta {query_id} terminó con estado {status}")
        time.sleep(poll_interval)


def flatten(results: list) -> list:
    rows = []
    for row in results:
        d = {c["field"]: c["value"] for c in row}
        rows.append(d)
    return rows


def pretty_print(rows: list):
    if not rows:
        print("No se encontraron eventos.")
        return
    try:
        from tabulate import tabulate

        print(tabulate(rows, headers="keys", tablefmt="github"))
    except ImportError:
        import json

        print(json.dumps(rows, indent=2, ensure_ascii=False))


def main():
    if len(sys.argv) not in (3, 5):
        print(__doc__)
        sys.exit(1)

    start_dt = parse_cli_time(sys.argv[1], sys.argv[2])
    end_dt = parse_cli_time(sys.argv[3], sys.argv[4]) if len(sys.argv) == 5 else datetime.now()

    if start_dt >= end_dt:
        print("La fecha de inicio debe ser anterior a la final.")
        sys.exit(1)

    start_ts, end_ts = map(to_epoch, (start_dt, end_dt))

    print(f"Ejecutando consulta entre {start_dt} y {end_dt}...\n")

    for lg in LOG_GROUPS:
        print(f"===> Log group: {lg}")
        try:
            qid = start_query(lg, start_ts, end_ts)
            rows = flatten(wait_results(qid))
            pretty_print(rows)
        except ClientError as e:
            print(f"Error consultando {lg}: {e}")
        print()


if __name__ == "__main__":
    main()