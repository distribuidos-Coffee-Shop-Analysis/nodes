#!/usr/bin/env python3

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional, Any

import yaml
import docker
from docker.errors import NotFound, APIError


SIGKILL = "SIGKILL"
SLEEP_INTERVAL = 3


def timestamp() -> str:
    return (
        datetime.now(tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def log_event(kind: str, **data) -> None:
    evt = {"ts": timestamp(), "event": kind, **data}
    print(json.dumps(evt), flush=True)


def load_compose_services(compose_path: Path) -> List[Dict[str, Any]]:
    """Lee el docker-compose.yaml y extrae información de servicios."""
    with open(compose_path, "r", encoding="utf-8") as f:
        compose = yaml.safe_load(f)

    services = compose.get("services", {})
    targets = []

    for svc_name, svc in services.items():
        cname = svc.get("container_name", svc_name)
        targets.append(
            {
                "service": svc_name,
                "container_name": cname,
                "role": _env_value(svc.get("environment", []), "NODE_ROLE"),
                "node_id": _env_value(svc.get("environment", []), "NODE_ID"),
            }
        )

    return targets


def _env_value(env_list, key: str) -> Optional[str]:
    """Extrae el valor de una variable de entorno desde la config del compose."""
    if isinstance(env_list, dict):
        return env_list.get(key)
    if isinstance(env_list, list):
        for item in env_list:
            if isinstance(item, str) and item.startswith(f"{key}="):
                return item.split("=", 1)[1]
    return None


def compile_patterns(patterns: List[str]) -> List[re.Pattern]:
    """Compila una lista de patrones regex."""
    if not patterns:
        return []
    return [re.compile(p) for p in patterns]


def match_any(name: str, patterns: List[re.Pattern]) -> bool:
    """Verifica si el nombre coincide con algún patrón."""
    if not patterns:
        return False
    return any(p.search(name) for p in patterns)


def kill_container(container) -> bool:
    """Mata el contenedor con SIGKILL."""
    try:
        container.kill(signal=SIGKILL)
        return True
    except NotFound:
        log_event("container_disappeared", container=container.name)
        return False
    except APIError as e:
        if "is not running" in str(e).lower():
            return True
        log_event("error_kill", container=container.name, error=str(e))
        return False


def parse_args():
    """Parsea argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Chaos Monkey",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  %(prog)s --print-targets
  %(prog)s --include "aggregate|joiner"
  %(prog)s --exclude "rabbitmq" --limit 5
  %(prog)s --dry-run
""",
    )

    parser.add_argument(
        "--compose",
        type=str,
        default="docker-compose.yaml",
        help="Ruta al docker-compose.yaml (default: docker-compose.yaml)",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        help="Regex de contenedores a incluir.",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Regex de contenedores a excluir.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Semilla para reproducibilidad.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Cantidad de kills antes de salir. 0 = infinito.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No ejecuta, solo imprime lo que haría.",
    )
    parser.add_argument(
        "--print-targets",
        action="store_true",
        help="Lista targets candidatos y sale.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    rnd = random.Random(args.seed) if args.seed is not None else random.Random()

    # Cargar compose
    compose_path = Path(args.compose)
    if not compose_path.exists():
        print(f"ERROR: no encuentro {compose_path}", file=sys.stderr)
        sys.exit(2)

    all_targets = load_compose_services(compose_path)

    for i in range(2, 4):
        all_targets.append(
            {
                "service": f"coordinator-{i}",
                "container_name": f"coordinator-{i}",
                "role": "coordinator",
                "node_id": str(i),
            }
        )

    include_re = compile_patterns(args.include)
    exclude_re = compile_patterns(args.exclude)

    candidates = []
    for t in all_targets:
        name = t["container_name"]
        if include_re and not match_any(name, include_re):
            continue
        if match_any(name, exclude_re):
            continue
        candidates.append(t)

    if not candidates:
        print("No hay contenedores candidatos tras aplicar filtros.", file=sys.stderr)
        sys.exit(1)

    # Print targets mode
    if args.print_targets:
        print(f"# Chaos Monkey - {len(candidates)} targets\n")
        for t in candidates:
            print(f"- {t['container_name']} (role: {t['role']})")
        sys.exit(0)

    # Conexión a Docker
    try:
        cli = docker.from_env()
        cli.ping()
    except Exception as e:
        print(f"ERROR: No puedo conectar a Docker: {e}", file=sys.stderr)
        sys.exit(3)

    # Loop principal
    kills = 0
    log_event("chaos_start", total_candidates=len(candidates), dry_run=args.dry_run)

    try:
        while True:
            target = rnd.choice(candidates)
            cname = target["container_name"]

            # Obtener contenedor
            try:
                container = cli.containers.get(cname)
                container.reload()
            except NotFound:
                log_event("skip", container=cname, reason="not_found")
                time.sleep(SLEEP_INTERVAL)
                continue

            if container.status != "running":
                log_event("skip", container=cname, reason="not_running")
                time.sleep(SLEEP_INTERVAL)
                continue

            log_event("kill", container=cname, role=target["role"])

            if not args.dry_run:
                kill_container(container)

            kills += 1

            if args.limit > 0 and kills >= args.limit:
                log_event("limit_reached", kills=kills)
                break

            time.sleep(SLEEP_INTERVAL)

    except KeyboardInterrupt:
        log_event("interrupted", kills=kills)
    finally:
        log_event("chaos_end", total_kills=kills)


if __name__ == "__main__":
    main()
