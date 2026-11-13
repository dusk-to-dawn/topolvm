#!/usr/bin/env python3
"""Print one line per failure of the ordinary rounds of a run, nothing if none.

A round fails when a claim was not provisioned, a provisioning failed, or a
LogicalVolume or a thin volume was left after the round's deletion. The limit
round is judged by run.sh, since provisioning failures are expected there.
"""
import json
import sys
from pathlib import Path


def main(out):
    for result in sorted(Path(out).glob("*/result.json")):
        try:
            r = json.loads(result.read_text())
        except ValueError:
            print(f"{result.parent.name}: no result")
            continue
        name = r.get("round", result.parent.name)
        if (r.get("succeeded") or 0) < r.get("volumes", 0):
            print(f"{name}: {r.get('succeeded')} of {r.get('volumes')} provisioned")
        if r.get("provisioning_failed"):
            print(f"{name}: {r['provisioning_failed']} ProvisioningFailed")
        if r.get("leftover_logicalvolumes"):
            print(f"{name}: {r['leftover_logicalvolumes']} LogicalVolumes left after deletion")
        if r.get("leftover_lvs"):
            print(f"{name}: {r['leftover_lvs']} thin volumes left after deletion")


if __name__ == "__main__":
    main(sys.argv[1])
