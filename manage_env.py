import argparse
import base64
import getpass
import os
from pathlib import Path
import re
import secrets
import subprocess

from env_utils import read_lines, write_lines


ROOT = Path(__file__).resolve().parent
STACKS = {
    "appliance": "docker-compose.swarm.yml",
    "home_appliance_airflow": "docker-compose.airflow.swarm.yml",
    "home_appliance_redash": "services/docker-compose-redash-swarm.yml",
}


def migrate() -> None:
    destination = ROOT / ".env"
    lines, values = read_lines(destination)
    for source in (ROOT / ".env.airflow", ROOT / "services" / ".env"):
        _, additions = read_lines(source)
        conflicts = [key for key, value in additions.items() if key in values and values[key] != value]
        if conflicts:
            raise ValueError(f"Conflicting keys in {source.relative_to(ROOT)}: {', '.join(conflicts)}")
        for key, value in additions.items():
            if key not in values:
                lines.append(f"{key}={value}")
                values[key] = value
    write_lines(destination, lines)
    print("Merged settings into .env. Original files remain unchanged.")


def initialize(service: str) -> None:
    destination = ROOT / ".env"
    lines, values = read_lines(destination)
    if service == "airflow":
        keys = (
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
            "AIRFLOW__CORE__FERNET_KEY",
            "AIRFLOW__WEBSERVER__SECRET_KEY",
            "FLOWER_BASIC_AUTH",
        )
    else:
        keys = ("REDASH_POSTGRES_PASSWORD", "REDASH_COOKIE_SECRET", "REDASH_SECRET_KEY")
    if any(key in values for key in keys):
        raise ValueError(f"{service} settings already exist or are incomplete; refusing to replace them.")
    if service == "airflow":
        password = getpass.getpass("Airflow MySQL user password (hexadecimal): ")
        if not re.fullmatch(r"[0-9a-fA-F]+", password):
            raise ValueError("Airflow MySQL password must be nonempty hexadecimal.")
        additions = (
            f"mysql+mysqldb://airflow:{password}@appliance_mysql:3306/airflow",
            base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii"),
            secrets.token_hex(32),
            f"admin:{secrets.token_hex(24)}",
        )
    else:
        additions = tuple(secrets.token_hex(32) for _ in keys)
    lines.extend(f"{key}={value}" for key, value in zip(keys, additions))
    write_lines(destination, lines)
    print(f"Added {service} settings to .env.")


def stack(action: str, name: str, registry_auth: bool) -> None:
    _, values = read_lines(ROOT / ".env")
    referenced = set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)", (ROOT / STACKS[name]).read_text()))
    environment = {key: value for key, value in os.environ.items() if key not in values and key not in referenced}
    result = subprocess.run(
        ["docker", "compose", "--env-file", ".env", "config", "--environment"],
        cwd=ROOT, env=environment, check=True, text=True, capture_output=True,
    )
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator and key in values:
            environment[key] = value
    missing = values.keys() - environment.keys()
    if missing:
        raise ValueError(f"Cannot read .env keys: {', '.join(sorted(missing))}")
    compose_file = str(ROOT / STACKS[name])
    command = ["docker", "stack", "config", "-c", compose_file]
    subprocess.run(command, cwd=ROOT, env=environment, check=True, stdout=subprocess.DEVNULL)
    if action == "deploy":
        command = ["docker", "stack", "deploy", "-c", compose_file, "--resolve-image", "never"]
        if name == "home_appliance_redash":
            command.append("--detach=false")
        if registry_auth:
            command.append("--with-registry-auth")
        subprocess.run([*command, name], cwd=ROOT, env=environment, check=True)
        if name == "home_appliance_redash":
            from redash.bootstrap import URL, bootstrap
            required = ("MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE")
            missing = [key for key in required if not environment.get(key)]
            if missing:
                raise ValueError(f"Missing settings for Redash: {', '.join(missing)}")
            bootstrap(URL, *(environment[key] for key in required))
    else:
        print(f"{name} configuration is valid.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage the shared .env and Swarm stacks.")
    subcommands = parser.add_subparsers(dest="action", required=True)
    subcommands.add_parser("migrate", help="Merge existing Airflow and Redash settings.")
    initializer = subcommands.add_parser("init", help="Add settings for a new service.")
    initializer.add_argument("service", choices=("airflow", "redash"))
    for action in ("check", "deploy"):
        command = subcommands.add_parser(action)
        command.add_argument("stack", choices=STACKS)
        if action == "deploy":
            command.add_argument("--with-registry-auth", action="store_true")
    args = parser.parse_args()
    try:
        if args.action == "migrate":
            migrate()
        elif args.action == "init":
            initialize(args.service)
        else:
            stack(args.action, args.stack, getattr(args, "with_registry_auth", False))
    except (EOFError, OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
