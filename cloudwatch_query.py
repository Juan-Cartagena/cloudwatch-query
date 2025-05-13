#!/usr/bin/env python3
"""
cloudwatch_query.py

Requisitos:
    pip install boto3 tabulate

Uso:
  python cloudwatch_query.py <fecha_ini> <hora_ini> [<fecha_fin> <hora_fin>] [--out <archivo>]
  python cloudwatch_query.py 2025-05-08 07:14 --out a.json

Ejemplos:
  python cloudwatch_query.py 2025-05-08 07:14
  python cloudwatch_query.py 2025-05-07 23:11:37 2025-05-07 23:11:47 --out a.json
  python cloudwatch_query.py 2025-05-08 07:14 --out resultado.csv
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List

import boto3
from botocore.exceptions import ClientError

# ---------- Configuración ---------- #
LOG_GROUPS: List[str] = [
    "/aws/lambda/bm-qrec-api-redeban",
    "/aws/lambda/bm-qrec-authorizer",
    #"/aws/lambda/bm-qrec-create-transaction",
    "/aws/lambda/bm-qrec-do-transaction",
]

QUERY_STRING = """
fields @timestamp, @logStream, @message, @entity.KeyAttributes.Name
| sort @timestamp desc
| limit 10000
"""
# ----------------------------------- #

logs = boto3.client("logs")


def to_epoch(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())


def parse_cli_time(date_str: str, time_str: str) -> datetime:
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")


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
    """Convierte los resultados de AWS a lista de diccionarios."""
    return [{c["field"]: c["value"] for c in row} for row in results]


def pretty_print(rows: list):
    if not rows:
        print("No se encontraron eventos.")
        return
    try:
        from tabulate import tabulate

        #print(tabulate(rows, headers="keys", tablefmt="github"))
    except ImportError:
        print(json.dumps(rows, indent=2, ensure_ascii=False))


# -------------- LÓGICA DE GRABADO --------------- #
def save_results(data: Dict[str, List[dict]], file_path: Path):
    """
    Guarda los resultados en file_path.
    El formato se decide por la extensión:
       .json  -> JSON
       .csv   -> CSV (un CSV por log group concatenado; se añade 'log_group')
    """
    suffix = file_path.suffix.lower()

    if suffix == ".json":
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Resultados guardados en {file_path.resolve()} (JSON).")

    elif suffix == ".csv":
        # Detectar campos máximos presentes
        all_rows = []
        for lg, rows in data.items():
            for r in rows:
                r = r.copy()
                r["log_group"] = lg
                all_rows.append(r)

        if not all_rows:
            print("No hay datos para guardar en CSV.")
            return

        # Determinar todas las columnas existentes
        fieldnames = sorted({k for row in all_rows for k in row.keys()})
        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"Resultados guardados en {file_path.resolve()} (CSV).")

    else:
        print(f"Extensión {suffix} no soportada. Usa .json o .csv")
# ------------------------------------------------- #


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Consulta varios log groups de CloudWatch.")
    p.add_argument("fecha_ini", help="YYYY-MM-DD")
    p.add_argument("hora_ini", help="HH:MM:SS")
    p.add_argument("fecha_fin", nargs="?", help="YYYY-MM-DD")
    p.add_argument("hora_fin", nargs="?", help="HH:MM:SS")
    p.add_argument(
        "--out",
        metavar="archivo",
        help="Ruta de salida (.json o .csv). Si no se especifica, solo imprime.",
    )
    return p


def main():
    args = build_arg_parser().parse_args()

    try:
        start_dt = parse_cli_time(args.fecha_ini, args.hora_ini) + timedelta(hours=5)
        end_dt = (
            parse_cli_time(args.fecha_fin, args.hora_fin) + timedelta(hours=5)
            if args.fecha_fin and args.hora_fin
            else datetime.now()
        )
    except ValueError:
        print("Error en el formato de fecha/hora. Usa YYYY-MM-DD HH:MM")
        sys.exit(1)

    if start_dt >= end_dt:
        print("La fecha de inicio debe ser anterior a la final.")
        sys.exit(1)

    start_ts, end_ts = map(to_epoch, (start_dt, end_dt))
    print(f"Ejecutando consulta entre {start_dt} y {end_dt}...\n")

    salida_total: Dict[str, List[dict]] = {}

    for lg in LOG_GROUPS:
        print(f"===> Log group: {lg}")
        try:
            qid = start_query(lg, start_ts, end_ts)
            rows = flatten(wait_results(qid))
            pretty_print(rows)
            salida_total[lg] = rows
        except ClientError as e:
            print(f"Error consultando {lg}: {e}")
        print()

    # Guardar si corresponde
    if args.out:
        save_results(salida_total, Path(args.out))


if __name__ == "__main__":
    main()