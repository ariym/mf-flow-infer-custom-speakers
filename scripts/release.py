#!/usr/bin/env python3
"""Build release archives and a hub manifest. Uses only Python's standard library."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
PLATFORMS = ("darwin/amd64", "darwin/arm64", "linux/amd64", "linux/arm64")

def build(repo, version, platforms, output):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise ValueError("--repo must be OWNER/REPOSITORY")
    kind = "flow" if (ROOT / "flow.json").exists() else "program"
    manifest = json.loads((ROOT / (kind + ".json")).read_text())
    if type(manifest["version"]) is not int or version < 1 or manifest["version"] != version:
        raise ValueError("release version must be positive and match the source manifest")
    key = manifest["key" if kind == "flow" else "program_name"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,127}", key):
        raise ValueError("invalid entity identifier")
    platforms = list(dict.fromkeys(platforms))
    if not platforms or any(p not in PLATFORMS for p in platforms):
        raise ValueError("unsupported release platform")
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest["gh_repo" if kind == "flow" else "git_repo"] = "https://github.com/" + repo
    manifest["artifacts"] = []
    for platform in platforms:
        target_os, target_arch = platform.split("/")
        name = f"{key}-v{version}-{target_os}-{target_arch}.tar.gz"
        entrypoint = "bin/" + key
        with tempfile.TemporaryDirectory(prefix="mf-release-") as temporary:
            stage = Path(temporary)
            (stage / "bin").mkdir()
            env = dict(os.environ, CGO_ENABLED="0", GOOS=target_os, GOARCH=target_arch)
            subprocess.run(["go", "build", "-trimpath", "-o", str(stage / entrypoint), "."],
                           cwd=ROOT, env=env, check=True)
            hook = ROOT / "scripts" / "build-upstream.sh"
            if kind == "program":
                subprocess.run(["sh", str(hook), str(stage), target_os, target_arch],
                               cwd=ROOT, env=env, check=True)
            # Fixed metadata and gzip timestamp make identical builds reproducible.
            with (output / name).open("wb") as raw:
                with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
                    with tarfile.open(fileobj=compressed, mode="w") as archive:
                        for item in sorted(stage.rglob("*")):
                            info = archive.gettarinfo(str(item), arcname=item.relative_to(stage).as_posix())
                            info.uid = info.gid = info.mtime = 0
                            info.uname = info.gname = ""
                            if item.is_file():
                                with item.open("rb") as content:
                                    archive.addfile(info, content)
                            else:
                                archive.addfile(info)
        checksum = hashlib.sha256((output / name).read_bytes()).hexdigest()
        (output / (name + ".sha256")).write_text(checksum + "  " + name + "\n")
        manifest["artifacts"].append({
            "os": target_os, "arch": target_arch,
            "url": f"https://github.com/{repo}/releases/download/v{version}/{name}",
            "sha256": checksum, "entrypoint": entrypoint,
        })
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(output / "manifest.json")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--platform", action="append", choices=PLATFORMS)
    parser.add_argument("--output", default=str(ROOT / "dist"))
    args = parser.parse_args()
    try:
        build(args.repo, args.version, args.platform or PLATFORMS, args.output)
    except (ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, str(error) + "\n")

if __name__ == "__main__":
    main()
