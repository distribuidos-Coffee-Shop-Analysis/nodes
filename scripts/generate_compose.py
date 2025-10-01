#!/usr/bin/env python3
import sys
import yaml

COMMON_ENV = [
    "RABBITMQ_HOST=rabbitmq",
    "RABBITMQ_PORT=5672",
    "RABBITMQ_USER=admin",
    "RABBITMQ_PASSWORD=admin",
]


def parse_pairs(pairs):
    out = {}
    for p in pairs:
        if "=" not in p:
            raise ValueError(f"Esperado tipo=cantidad, recibido: {p}")
        t, n = p.split("=", 1)
        out[t.strip()] = int(n.strip())
    return out


def role_from_type(node_type: str) -> str:
    mapping = {
        "filter-node-year": "filter_year",
        "filter-node-hour": "filter_hour",
        "filter-node-amount": "filter_amount",
        "group-by-node-q2": "q2_group",
        "group-by-node-q3": "q3_group",
        "group-by-node-q4": "q4_group",
        "aggregate-node-q2": "q2_aggregate",
        "aggregate-node-q3": "q3_aggregate",
        "aggregate-node-q4": "q4_aggregate",
        "joiner-node-q2": "q2_join",
        "joiner-node-q3": "q3_join",
        "joiner-node-q4-users": "q4_join_users",
        "joiner-node-q4-stores": "q4_join_stores",
    }
    return mapping.get(node_type, node_type)


def make_service_name(node_type: str, idx: int) -> str:
    return f"{node_type}-{idx:02d}"


def generate_compose(output_file, type_counts):
    compose = {
        "services": {},
        "networks": {
            "coffee_net": {
                "external": True,
                "name": "connection-node_coffee_net",
            }
        },
    }

    for node_type, replicas in type_counts.items():
        role = role_from_type(node_type)
        for i in range(1, replicas + 1):
            svc_name = make_service_name(node_type, i)
            env = list(COMMON_ENV) + [
                f"NODE_ROLE={role}",
                f"NODE_ID={i:02d}",
            ]
            service = {
                "build": ".",
                "environment": env,
                "networks": ["coffee_net"],
                "volumes": [f"./config/{role}.json:/app/config/{role}.json:ro"],
            }
            compose["services"][svc_name] = service

    with open(output_file, "w") as f:
        yaml.dump(compose, f, sort_keys=False)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Uso: python3 generate_compose_nodes.py <output_file> <tipo=cantidad> [<tipo=cantidad> ...]"
        )
        sys.exit(1)

    output_file = sys.argv[1]
    type_counts = parse_pairs(sys.argv[2:])
    generate_compose(output_file, type_counts)

