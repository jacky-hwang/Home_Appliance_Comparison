import argparse
from configparser import ConfigParser
import os
from pathlib import Path

from env_utils import read_lines, write_lines

PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_FILE = PROJECT_ROOT / "local.ini"
ENV_FILE = PROJECT_ROOT / ".env"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate .env from a local.ini section.")
    parser.add_argument(
        "environment",
        nargs="?",
        default=os.environ.get("ENV"),
        help="Environment section to use, e.g. DEV or DOCKER (or set ENV)",
    )
    args = parser.parse_args()
    if not args.environment:
        parser.error("Choose an environment with ENV=DEV or pass DEV/DOCKER as an argument.")

    config = ConfigParser(interpolation=None)
    if not config.read(CONFIG_FILE, encoding="utf-8"):
        parser.error(f"Cannot read {CONFIG_FILE}; copy local.ini.example to local.ini first.")

    section_name = args.environment.upper()
    if not config.has_section(section_name):
        sections = ", ".join(config.sections())
        parser.error(f"Unknown environment {section_name!r}. Available sections: {sections}")

    values = config[section_name]
    required_keys = {
        "worker_account",
        "worker_password",
        "rabbitmq_host",
        "rabbitmq_port",
        "csv_output_dir",
    }
    missing_keys = required_keys - values.keys()
    if missing_keys:
        parser.error(f"Missing settings in [{section_name}]: {', '.join(sorted(missing_keys))}")

    settings = dict(values)
    settings.setdefault("docker_image_version", "0.0.8")
    settings["rabbitmq_default_user"] = settings["worker_account"]
    settings["rabbitmq_default_pass"] = settings["worker_password"]
    replacements = {key.upper(): value for key, value in settings.items()}
    if any("\n" in value or "\r" in value for value in replacements.values()):
        parser.error("Multiline environment values are not supported.")
    try:
        lines, existing = read_lines(ENV_FILE) if ENV_FILE.exists() else ([], {})
        updated = [
            f"{line.partition('=')[0]}={replacements[line.partition('=')[0]]}"
            if line.partition("=")[0] in replacements else line
            for line in lines
        ]
        updated.extend(f"{key}={value}" for key, value in replacements.items() if key not in existing)
        write_lines(ENV_FILE, updated)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(f"Generated {ENV_FILE.name} using [{section_name}].")


if __name__ == "__main__":
    main()
