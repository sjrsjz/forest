"""Build with the pinned Lean kernel and audit the trust of the exported theorems."""

from pathlib import Path
import hashlib
import json
import re
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parent
ALLOWED_AXIOMS = {"propext", "Classical.choice", "Quot.sound"}


def run(*command):
    result = subprocess.run(command, cwd=ROOT, text=True, encoding="utf-8",
                            errors="replace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout, end="")
    if result.returncode:
        raise SystemExit(result.returncode)
    return result.stdout


def main():
    lake = shutil.which("lake")
    if lake is None:
        raise SystemExit("lake is missing from PATH; install the version in lean-toolchain")
    expected = (ROOT / "lean-toolchain").read_text().strip().split(":v")[-1]
    version = run(lake, "env", "lean", "--version")
    if not re.search(r"\bversion " + re.escape(expected) + r"(?:\s|,)", version):
        raise SystemExit("Lean version differs from lean-toolchain")
    sources = [ROOT / "Forest.lean", ROOT / "Audit.lean", *sorted((ROOT / "Forest").rglob("*.lean"))]
    forbidden = re.compile(r"\b(?:sorry|admit|native_decide)\b|skipKernelTC|^\s*(?:axiom|constant)\s", re.M)
    for source in sources:
        if forbidden.search(source.read_text(encoding="utf-8")):
            raise SystemExit("Forbidden proof shortcut in " + str(source.relative_to(ROOT)))
    mapping = json.loads((ROOT / "source-map.json").read_text(encoding="utf-8"))
    for entry in mapping["files"]:
        source = ROOT.parents[1] / entry["path"]
        digest = hashlib.sha256(source.read_text(encoding="utf-8").encode("utf-8")).hexdigest()
        if digest != entry["sha256"]:
            raise SystemExit("Rust source changed; review the model correspondence: " + entry["path"])
    run(lake, "build")
    output = run(lake, "env", "lean", "Audit.lean")
    theorems = re.findall(r"^#print axioms ([\w.]+)$", (ROOT / "Audit.lean").read_text(), re.M)
    if not theorems:
        raise SystemExit("Empty theorem audit")
    for theorem in theorems:
        report = re.search(
            r"'" + re.escape(theorem) + r"' (?:does not depend on any axioms|depends on axioms: (\[[^\]]*\]))",
            output, re.S,
        )
        if report is None:
            raise SystemExit("Missing kernel trust report: " + theorem)
        dependencies = set(re.findall(r"[\w.]+", report.group(1) or ""))
        unexpected = dependencies - ALLOWED_AXIOMS
        if unexpected:
            raise SystemExit(f"Unexpected axioms for {theorem}: {sorted(unexpected)}")
    print(f"PASS: {len(theorems)} theorem trust reports; pinned Lean {expected}; no extra axioms")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    main()
