#!/usr/bin/env python3
import json
import sys

sys.path.insert(0, r"C:\Users\Tyson\Projects\kiyosaki_stage2b\state")
from kcos_stage2_prepare_v2 import prepare

context = prepare(dry_run=False)
context["contract"] = r"C:\Users\Tyson\Projects\kiyosaki_stage2b\state\contracts\kcos_stage2_v2.md"
print(json.dumps(context, separators=(",", ":")))
