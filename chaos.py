#!/usr/bin/env python3
"""
Chaos Distribuido
"""

import argparse
import json
import os
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


# -----------------------------
# Señales
# -----------------------------
SIGINT = "SIGINT"
SIGTERM = "SIGTERM"
SIGKILL = "SIGKILL"


# -----------------------------
# Utils
# -----------------------------
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
    """
    Lee el docker-compose.yaml y extrae información de servicios.

    Returns:
        Lista de dicts con: service, container_name, role, node_id
    """
    with open(compose_path, "r", encoding="utf-8") as f:
        compose = yaml.safe_load(f)

    services = compose.get("services", {})
    targets = []

    for svc_name, svc in services.items():
        # Preferimos container_name si está definido
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
    """
    Extrae el valor de una variable de entorno desde la config del compose.

    Soporta ambos formatos:
      - Lista: ["KEY=VALUE", ...]
      - Dict: {"KEY": "VALUE", ...}
    """
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


# -----------------------------
# Acciones sobre contenedores
# -----------------------------
def send_signal_safe(container, sig: str) -> bool:
    """
    Envía una señal al contenedor de forma segura.

    Args:
        container: Objeto container de Docker SDK
        sig: String de señal ("SIGINT", "SIGTERM", "SIGKILL")

    Returns:
        True si se envió exitosamente, False en caso de error
    """
    try:
        container.kill(signal=sig)
        return True
    except NotFound:
        log_event(
            "container_disappeared", container=container.name, during="send_signal"
        )
        return False
    except APIError as e:
        if "is not running" in str(e).lower():
            return True
        log_event("error_signal", container=container.name, signal=sig, error=str(e))
        return False


def wait_for_stop(container, timeout: float, poll_interval: float = 0.5) -> bool:
    """
    Espera a que el contenedor se detenga.

    Args:
        container: Objeto container de Docker SDK
        timeout: Tiempo máximo de espera en segundos
        poll_interval: Intervalo de polling en segundos

    Returns:
        True si el contenedor se detuvo, False si timeout
    """
    waited = 0.0
    while waited < timeout:
        try:
            container.reload()
            if container.status != "running":
                return True
        except NotFound:
            return True
        except APIError as e:
            log_event("error_polling", container=container.name, error=str(e))
            return False

        time.sleep(poll_interval)
        waited += poll_interval

    return False


def stop_graceful(container, timeout: int) -> bool:
    """
    Parada graceful: SIGINT -> espera -> SIGTERM -> stop forzado.
    - SIGINT: os/signal lo captura, permite cleanup
    - SIGTERM: señal estándar de terminación

    Args:
        container: Objeto container de Docker SDK
        timeout: Tiempo máximo de espera para parada limpia

    Returns:
        True si el contenedor se detuvo exitosamente
    """
    cname = container.name

    log_event("signal_sent", container=cname, signal=SIGINT)
    if not send_signal_safe(container, SIGINT):
        try:
            container.reload()
            return container.status != "running"
        except NotFound:
            return True

    # Paso 2: Esperar a que termine solo
    if wait_for_stop(container, timeout):
        log_event("stopped_gracefully", container=cname, via=SIGINT)
        return True

    # Paso 3: Fallback a SIGTERM
    log_event("signal_sent", container=cname, signal=SIGTERM, reason="SIGINT timeout")
    if not send_signal_safe(container, SIGTERM):
        try:
            container.reload()
            return container.status != "running"
        except NotFound:
            return True

    # Paso 4: Esperar un poco más
    if wait_for_stop(container, timeout):
        log_event("stopped_gracefully", container=cname, via=SIGTERM)
        return True

    # Paso 5: Forzar stop (Docker enviará SIGKILL tras su propio timeout)
    log_event("forcing_stop", container=cname)
    try:
        container.stop(timeout=5)
        container.reload()
        return container.status != "running"
    except NotFound:
        return True
    except APIError as e:
        log_event("error_stop", container=cname, error=str(e))
        return False


def stop_abrupt(container) -> bool:
    """
    Parada abrupta: SIGKILL inmediato.

    Simula un crash o pérdida de energía.

    Args:
        container: Objeto container de Docker SDK

    Returns:
        True si el contenedor se detuvo exitosamente
    """
    cname = container.name
    log_event("signal_sent", container=cname, signal=SIGKILL)

    if not send_signal_safe(container, SIGKILL):
        try:
            container.reload()
            return container.status != "running"
        except NotFound:
            return True

    # Dar un momento para que Docker actualice el estado
    return wait_for_stop(container, timeout=2.0, poll_interval=0.3)


# TODO: Remove
def restart_container(container, cli) -> bool:
    """
    Reinicia un contenedor de forma segura.

    Args:
        container: Objeto container de Docker SDK
        cli: Cliente Docker

    Returns:
        True si se reinició exitosamente
    """
    cname = container.name

    try:
        # Refrescar referencia por si el objeto quedó stale
        container = cli.containers.get(cname)
        container.start()
        container.reload()
        log_event("restart_success", container=cname, status=container.status)
        return True
    except NotFound:
        log_event("restart_failed", container=cname, reason="container_not_found")
        return False
    except APIError as e:
        if "already started" in str(e).lower():
            log_event("restart_skipped", container=cname, reason="already_running")
            return True
        log_event("restart_failed", container=cname, error=str(e))
        return False


# -----------------------------
# Lógica principal
# -----------------------------
def parse_args():
    """Parsea argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Chaos Monkey: inyección aleatoria de fallas sobre contenedores Docker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Listar targets disponibles
  %(prog)s --print-targets
  
  # Solo afectar nodos aggregate y joiner
  %(prog)s --include "aggregate|joiner"
  
  # Excluir rabbitmq, 5 eventos máximo
  %(prog)s --exclude "rabbitmq" --limit 5
  
  # Modo seco (no ejecuta, solo muestra)
  %(prog)s --dry-run
  
  # Alta probabilidad de kills abruptos
  %(prog)s --graceful-prob 0.2
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
        help="Regex de contenedores a incluir (repetible). Por defecto: todos.",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Regex de contenedores a excluir (repetible). Ej: '^rabbitmq$'",
    )
    parser.add_argument(
        "--graceful-prob",
        type=float,
        default=0.6,
        help="Probabilidad de parada graceful (0.0-1.0). Resto es SIGKILL. (default: 0.6)",
    )
    parser.add_argument(
        "--graceful-timeout",
        type=int,
        default=8,
        help="Timeout (s) para parada graceful antes de forzar. (default: 8)",
    )
    parser.add_argument(
        "--sleep-min",
        type=float,
        default=16.0,
        help="Espera mínima (s) entre inyecciones. (default: 16.0)",
    )
    parser.add_argument(
        "--sleep-max",
        type=float,
        default=20.0,
        help="Espera máxima (s) entre inyecciones. (default: 20.0)",
    )
    parser.add_argument(
        "--restart-after",
        type=float,
        default=4.0,
        help="Si > 0: segundos antes de reiniciar el contenedor. (default: 10.0)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Semilla para reproducibilidad. (default: aleatorio)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Cantidad de eventos antes de salir. 0 = infinito. (default: 0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No ejecuta acciones, solo imprime lo que haría.",
    )
    parser.add_argument(
        "--print-targets", action="store_true", help="Lista targets candidatos y sale."
    )
    parser.add_argument(
        "--one-shot",
        action="store_true",
        help="Ejecuta una sola inyección y sale (equivale a --limit 1).",
    )

    return parser.parse_args()


def sleep_between(rnd: random.Random, mn: float, mx: float) -> None:
    """Espera un tiempo aleatorio entre mn y mx segundos."""
    delay = rnd.uniform(mn, mx)
    time.sleep(delay)


def get_container_safe(cli, cname: str):
    """
    Obtiene un contenedor de forma segura.

    Returns:
        (container, error_reason) - container es None si hay error
    """
    try:
        container = cli.containers.get(cname)
        return container, None
    except NotFound:
        return None, "not_found"
    except APIError as e:
        return None, str(e)


def main():
    args = parse_args()

    # Configurar semilla
    if args.seed is not None:
        rnd = random.Random(args.seed)
    else:
        rnd = random.Random()

    # One-shot mode
    if args.one_shot:
        args.limit = 1

    # Cargar compose
    compose_path = Path(args.compose)
    if not compose_path.exists():
        print(f"ERROR: no encuentro {compose_path}", file=sys.stderr)
        sys.exit(2)

    all_targets = load_compose_services(compose_path)

    # Aplicar filtros
    include_re = compile_patterns(args.include)
    exclude_re = compile_patterns(args.exclude)

    candidates = []
    for t in all_targets:
        name = t["container_name"]
        # Si hay includes, debe matchear al menos uno
        if include_re and not match_any(name, include_re):
            continue
        # Si matchea algún exclude, lo saltamos
        if match_any(name, exclude_re):
            continue
        candidates.append(t)

    if not candidates:
        print("No hay contenedores candidatos tras aplicar filtros.", file=sys.stderr)
        print("\nContenedores disponibles:", file=sys.stderr)
        for t in all_targets:
            print(f"  - {t['container_name']}", file=sys.stderr)
        sys.exit(1)

    # Print targets mode
    if args.print_targets:
        print("# Chaos Monkey - Targets candidatos")
        print(f"# Total: {len(candidates)} de {len(all_targets)} servicios\n")
        for t in candidates:
            print(f"- {t['container_name']}")
            print(f"    service: {t['service']}")
            print(f"    role:    {t['role']}")
            print(f"    node_id: {t['node_id']}")
        sys.exit(0)

    # Conexión a Docker
    try:
        cli = docker.from_env()
        cli.ping()
    except Exception as e:
        print(f"ERROR: No puedo conectar a Docker: {e}", file=sys.stderr)
        sys.exit(3)

    # Loop principal
    injected = 0
    log_event(
        "chaos_start",
        compose=str(compose_path),
        total_candidates=len(candidates),
        graceful_prob=args.graceful_prob,
        restart_after=args.restart_after,
        dry_run=args.dry_run,
    )

    try:
        while True:
            # Seleccionar víctima
            target = rnd.choice(candidates)
            cname = target["container_name"]

            # Obtener contenedor
            container, err = get_container_safe(cli, cname)
            if container is None:
                log_event("skip", container=cname, reason=err or "not_found")
                sleep_between(rnd, args.sleep_min, args.sleep_max)
                continue

            # Verificar que esté corriendo
            try:
                container.reload()
            except NotFound:
                log_event("skip", container=cname, reason="disappeared")
                sleep_between(rnd, args.sleep_min, args.sleep_max)
                continue

            if container.status != "running":
                log_event(
                    "skip",
                    container=cname,
                    reason="not_running",
                    status=container.status,
                )
                sleep_between(rnd, args.sleep_min, args.sleep_max)
                continue

            # Elegir tipo de falla
            graceful = rnd.random() < args.graceful_prob
            action = "graceful_stop" if graceful else "abrupt_kill"

            log_event(
                "inject_start",
                container=cname,
                service=target["service"],
                role=target["role"],
                node_id=target["node_id"],
                action=action,
            )

            if args.dry_run:
                log_event("dry_run", container=cname, would_do=action)
            else:
                # # Ejecutar acción
                # if graceful:
                #     ok = stop_graceful(container, args.graceful_timeout)
                # else:
                #     ok = stop_abrupt(container)
                ok = stop_abrupt(container)

                log_event("inject_done", container=cname, action=action, success=ok)

                # # Restart opcional
                if ok and args.restart_after > 0:
                    log_event(
                        "waiting_restart", container=cname, seconds=args.restart_after
                    )
                    time.sleep(args.restart_after)
                    restart_container(container, cli)

            injected += 1

            # Check limit
            if args.limit > 0 and injected >= args.limit:
                log_event("limit_reached", injected=injected, limit=args.limit)
                break

            sleep_between(rnd, args.sleep_min, args.sleep_max)

    except KeyboardInterrupt:
        log_event("interrupted", injected=injected)
    except Exception as e:
        log_event("error_fatal", error=str(e), injected=injected)
        raise
    finally:
        log_event("chaos_end", total_injected=injected)


if __name__ == "__main__":
    main()
