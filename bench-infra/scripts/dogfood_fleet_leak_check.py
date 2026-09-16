#!/usr/bin/env python3
"""Account-wide orphan sweep for the dogfood operator fleet (map #16, #22).

`bench-infra`'s own leak check verifies only the local terraform.tfstate; a
run that failed before writing state, or a fleet brought up under a stale
state file, leaves instances nothing local knows about. This sweeps the AWS
account by tag, independent of any state file.

Lists every non-terminated EC2 instance in the region tagged `owner=<owner>`
(default `uc-dogfood`). Exit 0 if none (clean), 1 if any are found (a leak to
destroy), 2 on an AWS/credential error. Read-only: it never terminates
anything — destroying is the maintainer's `terraform destroy`, or a manual
call once identified.

    source bench-infra/.env    # AWS creds
    python3 bench-infra/scripts/dogfood_fleet_leak_check.py [--owner uc-dogfood] [--region us-east-1]
"""
import argparse
import sys


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--owner", default="uc-dogfood", help="the owner tag to sweep for")
    ap.add_argument("--region", default="us-east-1")
    args = ap.parse_args()
    try:
        import boto3
    except ImportError:
        print("boto3 not installed", file=sys.stderr)
        return 2
    try:
        ec2 = boto3.client("ec2", region_name=args.region)
        resp = ec2.describe_instances(
            Filters=[
                {"Name": "tag:owner", "Values": [args.owner]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped", "shutting-down"]},
            ]
        )
    except Exception as e:  # noqa: BLE001 — surface any credential/region/API error as exit 2
        print(f"AWS error: {e}", file=sys.stderr)
        return 2
    found = []
    for res in resp.get("Reservations", []):
        for inst in res.get("Instances", []):
            name = next((t["Value"] for t in inst.get("Tags", []) if t["Key"] == "Name"), "")
            found.append((inst["InstanceId"], inst["InstanceType"], inst["State"]["Name"], str(inst.get("LaunchTime", "")), name))
    if not found:
        print(f"CLEAN: no instances tagged owner={args.owner} in {args.region}")
        return 0
    print(f"LEAK: {len(found)} instance(s) tagged owner={args.owner} in {args.region} — destroy them:")
    for iid, itype, state, launched, name in found:
        print(f"  {iid}  {itype:14} {state:12} {launched}  {name}")
    print("\nRemedy: `terraform -chdir=bench-infra/terraform destroy -var-file=../operator-fleet.aws.tfvars`")
    print("or, if the state file is gone, terminate by id after confirming they are this fleet's.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
