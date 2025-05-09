#!/usr/bin/env python3
"""
cloudwatch_query.py

Uso:
  python cloudwatch_query.py <fecha_ini> <hora_ini> [<fecha_fin> <hora_fin>]
  python cloudwatch_query.py 2025-05-08 07:14

Ejemplo:
  python cloudwatch_query.py 2025-05-08 07:14
  python cloudwatch_query.py 2025-05-08 07:14 2025-05-08 08:00
"""

import sys
import json
import subprocess
import time
from datetime import datetime, timezone
from typing import List

# ---------- Configuración ---------- #
LOG_GROUPS = [
    "/aws/lambda/bm-qrec-api-redeban",
    "/aws/lambda/bm-qrec-authorizer",
    "/aws/lambda/bm-qrec-create-transaction",
    "/aws/lambda/bm-qrec-do-transaction",
]

# Cambia la consulta a tu necesidad (lenguaje CloudWatch Logs Insights)
QUERY = """
fields @timestamp, @logStream, @message
| sort @timestamp desc
| limit 20
"""
# ----------------------------------- #


def to_epoch_seconds(dt: datetime) -> int:
    """Convierte datetime -> epoch (segundos) en UTC."""
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def parse_cli_time(date_str: str, hour_str: str) -> datetime:
    """Parsea 'YYYY-MM-DD' 'HH:MM' a datetime (se asume local tz)."""
    return datetime.strptime(f"{date_str} {hour_str}", "%Y-%m-%d %H:%M")


def run_aws_cli(cmd: List[str]) -> dict:
    """Ejecuta AWS CLI y devuelve la salida JSON parseada."""
    try:
        result = subprocess.check_output(cmd, text=True)
        return json.loads(result)
    except subprocess.CalledProcessError as e:
        print("Error al ejecutar AWS CLI:", e.stderr)
        sys.exit(1)


def start_query(log_group: str, start: int, end: int) -> str:
    """Lanza logs start-query y devuelve el queryId."""
    cmd = [
        "aws",
        "logs",
        "start-query",
        "--log-group-name", log_group,
        "--start-time", str(start),
        "--end-time", str(end),
        "--query-string", QUERY.strip(),
    ]
    resp = run_aws_cli(cmd)
    return resp["queryId"]


def wait_for_results(query_id: str) -> list:
    """Espera a que la consulta termine y devuelve los resultados."""
    while True:
        resp = run_aws_cli(["aws", "logs", "get-query-results", "--query-id", query_id])
        status = resp["status"]
        if status == "Complete":
            return resp["results"]
        elif status in {"Cancelled", "Failed", "Timeout"}:
            print(f"La consulta {query_id} terminó con estado {status}")
            sys.exit(1)
        time.sleep(1)  # vuelve a consultar en 1 s


def flatten(results: list) -> list:
    """Convierte el formato de AWS CLI a lista de dicts."""
    flattened = []
    for row in results:
        d = {}
        for cell in row:
            d[cell["field"]] = cell["value"]
        flattened.append(d)
    return flattened


def pretty_print(rows: list):
    """Muestra los resultados de forma tabular si hay tabulate instalado."""
    if not rows:
        print("No se encontraron eventos.")
        return
    try:
        from tabulate import tabulate
        print(tabulate(rows, headers="keys", tablefmt="github"))
    except ImportError:
        # salida simple si no hay tabulate
        print(json.dumps(rows, indent=2, ensure_ascii=False))


def main():
    if len(sys.argv) not in (3, 5):
        print(__doc__)
        sys.exit(1)

    start_dt = parse_cli_time(sys.argv[1], sys.argv[2])

    if len(sys.argv) == 5:
        end_dt = parse_cli_time(sys.argv[3], sys.argv[4])
    else:
        end_dt = datetime.now()

    start_epoch = to_epoch_seconds(start_dt)
    end_epoch = to_epoch_seconds(end_dt)

    if start_epoch >= end_epoch:
        print("La fecha/hora de inicio debe ser anterior a la de fin.")
        sys.exit(1)

    print(f"Ejecutando consulta entre {start_dt} y {end_dt}...\n")

    for lg in LOG_GROUPS:
        print(f"===> Log group: {lg}")
        qid = start_query(lg, start_epoch, end_epoch)
        rows = flatten(wait_for_results(qid))
        pretty_print(rows)
        print()  # línea en blanco entre grupos


if __name__ == "__main__":
    main()