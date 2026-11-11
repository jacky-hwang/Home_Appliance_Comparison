import os
from pathlib import Path
import re
import tempfile


KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def read_lines(path: Path) -> tuple[list[str], dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    values: dict[str, str] = {}
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not KEY.fullmatch(key) or key in values:
            raise ValueError(f"Invalid or duplicate environment key in {path.name}")
        values[key] = value
    return lines, values


def write_lines(path: Path, lines: list[str]) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=".env-tmp-", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write("\n".join(lines) + "\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
