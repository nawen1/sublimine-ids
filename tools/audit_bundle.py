from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return "<missing>\n"


def _run(args: list[str], *, cwd: Path) -> str:
    try:
        proc = subprocess.run(args, cwd=str(cwd), check=False, capture_output=True, text=True)
    except FileNotFoundError:
        return "<missing executable>\n"
    out = (proc.stdout or "") + (proc.stderr or "")
    return out.strip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect critical repo state into a single text bundle.")
    parser.add_argument("--out", required=True, help="Output file path")
    args = parser.parse_args()

    root = _repo_root()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    files = [
        "README.md",
        "pyproject.toml",
        "config/sublimine.yaml",
        "src/sublimine/config.py",
        "src/sublimine/run.py",
        "src/sublimine/events/detectors.py",
        "src/sublimine/events/microbars.py",
        "src/sublimine/events/setups.py",
        "tests/test_microbars_builder.py",
        "tests/test_setups_dlv.py",
        "tests/test_setups_saf.py",
        "tests/test_setups_afs.py",
        "tests/test_setups_per.py",
    ]

    parts: list[str] = []
    parts.append(f"python: {sys.version}\n")
    parts.append("git_head:\n" + _run(["git", "rev-parse", "HEAD"], cwd=root))
    parts.append("git_status:\n" + _run(["git", "status", "--porcelain=v1"], cwd=root))

    for rel in files:
        path = root / rel
        parts.append(f"\n===== {rel} =====\n")
        parts.append(_read_text(path))

    out_path.write_text("".join(parts), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

