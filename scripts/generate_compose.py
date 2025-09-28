import sys
import yaml


def generate_compose(output_file, n_clients):
    # Armar el compose a mano
    compose = {
        "name": "tp0",
        "services": {
            "server": {
                "container_name": "server",
                "image": "server:latest",
                "entrypoint": "python3 /main.py",
                "environment": ["PYTHONUNBUFFERED=1", f"EXPECTED_AGENCIES={n_clients}"],
                "volumes": ["./server/config.ini:/config.ini"],
                "networks": ["testing_net"],
            }
        },
        "networks": {
            "testing_net": {
                "ipam": {"driver": "default", "config": [{"subnet": "172.25.125.0/24"}]}
            }
        },
    }

    # 2. Agregar N clientes dinámicamente
    # 2. Agregar N clientes dinámicamente
    for i in range(1, n_clients + 1):
        compose["services"][f"client{i}"] = {
            "container_name": f"client{i}",
            "image": "client:latest",
            "entrypoint": "/client",
            "environment": [
                f"CLI_ID={i}",
                f"CLI_CSV_FILE=/agency-{i}.csv",
            ],
            "volumes": [
                "./client/config.yaml:/config.yaml",
                f"./.data/agency-{i}.csv:/agency-{i}.csv",
            ],
            "networks": ["testing_net"],
            "depends_on": ["server"],
        }

    # 3. Guardar el nuevo compose en archivo de salida
    with open(output_file, "w") as f:
        yaml.dump(compose, f, sort_keys=False)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 generate_compose.py <output_file> <n_clients>")
        sys.exit(1)

    output_file = sys.argv[1]
    n_clients = int(sys.argv[2])
    generate_compose(output_file, n_clients)
