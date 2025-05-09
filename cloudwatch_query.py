#!/usr/bin/env python3
"""
cloudwatch_query.py  (hora local de Colombia)

Uso:
  python cloudwatch_query.py <fecha_ini> <hora_ini> [<fecha_fin> <hora_fin>] [--out <archivo>]

Ejemplos:
  python cloudwatch_query.py 2025-05-08 07:14
  python cloudwatch_query.py 2025-05-08 07:14 2025-05-08 08:00 --out logs.json
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import boto3
from botocore.exceptions import ClientError

# ───────── 1.  ZONA HORARIA DE COLOMBIA ────────── #
try:
    from zoneinfo import ZoneInfo          #  Python 3.9+
except ImportError:                         #  Python 3.8 o anterior
    from pytz import timezone as _tz        #  pip install pytz
    class ZoneInfo:                         #  envoltorio mínimo
        def __init__(self, name): self._tz = _tz(name)
        def utcoffset(self, dt):  return self._tz.utcoffset(dt)
        def dst(self, dt):        return self._tz.dst(dt)
        def tzname(self, dt):     return self._tz.tzname(dt)
CO_TZ = ZoneInfo("America/Bogota")
# ──────────────────────────────────────────────── #

# -------- Configuración de log groups y query -------- #
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
# ----------------------------------------------------- #

logs = boto3.client("logs")


# ---------- Utilidades de fecha/hora ---------- #
def parse_cli_time(date_str: str, time_str: str) -> datetime:
    """
    Convierte 'YYYY-MM-DD' 'HH:MM' a datetime con zona America/Bogota.
    """
    naive = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    return naive.replace(tzinfo=CO_TZ)


def to_epoch_seconds(dt_local: datetime) -> int:
    """
    Pasa un datetime zonado en America/Bogota a epoch segundos UTC,
    que es lo que exige el API de CloudWatch Logs.
    """
    return int(dt_local.astimezone(timezone.utc).timestamp())
# ---------------------------------------------- #


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
    """
    Convierte el formato devuelto por get_query_results a lista de dicts
    y transforma el campo @timestamp a string local (America/Bogota).
    """
    filas = []
    for row in results:
        d = {}
        for cell in row:
            field, value = cell["field"], cell["value"]
            if field == "@timestamp":
                # CloudWatch devuelve epoch-ms → int → datetime UTC
                ts_ms = int(value)
                dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                dt_local = dt_utc.astimezone(CO_TZ)
                value = dt_local.strftime("%Y-%m-%d %H:%M:%S")
            d[field] = value
        filas.append(d)
    return filas


def pretty_print(rows: list):
    if not rows:
        print("No se encontraron eventos.")
        return
    try:
        from tabulate import tabulate
        print(tabulate(rows, headers="keys", tablefmt="github"))
    except ImportError:
        print(json.dumps(rows, indent=2, ensure_ascii=False))


# -------------- Guardar a archivo ---------------- #
def save_results(data: Dict[str, List[dict]], file_path: Path):
    """
    Guarda los resultados en JSON o CSV según la extensión del nombre.
    """
    suffix = file_path.suffix.lower()
    if suffix == ".json":
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Resultados guardados en {file_path.resolve()} (JSON).")

    elif suffix == ".csv":
        all_rows = []
        for lg, rows in data.items():
            for r in rows:
                r = r.copy()
                r["log_group"] = lg
                all_rows.append(r)
        if not all_rows:
            print("No hay datos para guardar en CSV.")
            return
        fieldnames = sorted({k for row in all_rows for k in row})
        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"Resultados guardados en {file_path.resolve()} (CSV).")
    else:
        print(f"Extensión {suffix} no soportada → usa .json o .csv")
# --------------------------------------------------- #


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Consulta varios log groups de CloudWatch (hora Colombia).")
    p.add_argument("fecha_ini", help="YYYY-MM-DD")
    p.add_argument("hora_ini", help="HH:MM")
    p.add_argument("fecha_fin", nargs="?", help="YYYY-MM-DD")
    p.add_argument("hora_fin", nargs="?", help="HH:MM")
    p.add_argument("--out", metavar="archivo", help="Fichero de salida (.json o .csv)")
    return p


def main():
    args = build_arg_parser().parse_args()

    # --- Parseo de fechas ---
    try:
        start_dt = parse_cli_time(args.fecha_ini, args.hora_ini)
        if args.fecha_fin and args.hora_fin:
            end_dt = parse_cli_time(args.fecha_fin, args.hora_fin)
        else:
            end_dt = datetime.now(CO_TZ)
    except ValueError:
        print("Error en el formato de fecha/hora. Usa YYYY-MM-DD HH:MM")
        sys.exit(1)

    if start_dt >= end_dt:
        print("La fecha de inicio debe ser anterior a la final.")
        sys.exit(1)

    start_ts = to_epoch_seconds(start_dt)
    end_ts = to_epoch_seconds(end_dt)

    print(f"Ejecutando consulta entre {start_dt} y {end_dt} (hora Colombia)…\n")

    resultados: Dict[str, List[dict]] = {}

    for lg in LOG_GROUPS:
        print(f"===> Log group: {lg}")
        try:
            qid = start_query(lg, start_ts, end_ts)
            rows = flatten(wait_results(qid))
            pretty_print(rows)
            resultados[lg] = rows
        except ClientError as e:
            print(f"Error consultando {lg}: {e}")
        print()

    if args.out:
        save_results(resultados, Path(args.out))


if __name__ == "__main__":
    main()