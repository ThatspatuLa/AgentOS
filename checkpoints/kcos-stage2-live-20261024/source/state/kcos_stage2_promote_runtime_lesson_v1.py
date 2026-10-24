#!/usr/bin/env python3
"""Attach candidate-matched Stage 3 execution evidence to one validated lesson."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import kcos_stage2_learning_v1 as learning


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--lesson-id", required=True)
    parser.add_argument("--stage3-receipt", type=Path, required=True)
    args = parser.parse_args()
    event_id = learning.promote_runtime_validated_lesson(
        args.ledger, args.lesson_id, args.stage3_receipt)
    current = learning.latest_lesson(args.ledger, args.lesson_id)
    print(json.dumps({"lesson_id": args.lesson_id, "lesson_event_id": event_id,
                      "state": current["state"] if current else None,
                      "stage3_receipt": str(args.stage3_receipt.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
