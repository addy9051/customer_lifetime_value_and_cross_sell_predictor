"""
Model Artifact Checksum Generator (VULN-006, VULN-016)
=====================================================
Generates SHA-256 checksums for all model artifacts and writes them to
artifact_checksums.json. This file is used by the API at startup to verify
model integrity before loading.

Usage:
    python -m models.generate_checksums [--artifacts-dir models/artifacts]
"""

import argparse
import hashlib
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# File extensions to checksum
ARTIFACT_EXTENSIONS = {".joblib", ".pkl", ".pickle", ".json", ".parquet", ".csv"}


def generate_checksums(artifacts_dir: Path) -> dict:
    """Generate SHA-256 checksums for all model artifact files."""
    checksums = {}

    for path in sorted(artifacts_dir.rglob("*")):
        if path.is_file() and path.suffix in ARTIFACT_EXTENSIONS:
            relative = str(path.relative_to(artifacts_dir)).replace("\\", "/")
            sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
            checksums[relative] = sha256
            logger.info("  %-55s  %s", relative, sha256[:16] + "...")

    return checksums


def main():
    parser = argparse.ArgumentParser(description="Generate model artifact checksums")
    parser.add_argument(
        "--artifacts-dir",
        type=str,
        default="models/artifacts",
        help="Path to model artifacts directory",
    )
    args = parser.parse_args()

    artifacts_dir = Path(args.artifacts_dir)
    if not artifacts_dir.exists():
        logger.error("Artifacts directory not found: %s", artifacts_dir)
        return

    logger.info("=" * 60)
    logger.info("Model Artifact Checksum Generator")
    logger.info("=" * 60)
    logger.info("Scanning: %s", artifacts_dir.resolve())

    checksums = generate_checksums(artifacts_dir)

    output_path = artifacts_dir / "artifact_checksums.json"
    with open(output_path, "w") as f:
        json.dump(checksums, f, indent=2, sort_keys=True)

    logger.info("=" * 60)
    logger.info("Generated %d checksums → %s", len(checksums), output_path)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
