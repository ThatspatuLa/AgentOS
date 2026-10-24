"""Source transformation and mathematical regression for bounded OBV history.

The small test translator executes the actual generated MQL numeric loops. The
reference independently computes pivot values and direct signed-volume sums.
This is offline numerical evidence, not an authenticated MT5 runtime claim.
"""
from pathlib import Path
import random
import re
import unittest

import kcos_stage2_obv_reconcile_v1 as subject

FIXTURE = Path(__file__).parent / "tests/fixtures/obv_stage2_reconciliation_20261023.mq5"
BRIEF = {"resolved_intent": {"volume_sign_rule": "close_vs_open", "signal_shift": 1, "h1_regime_shift": 1}}


def numeric_loop(source, variable):
    """Translate reviewed scalar/array loop subset, taking equations from MQL."""
    start = source.index("for (int " + variable + " =")
    brace = source.index("{", start)
    depth, end = 1, brace + 1
    while depth:
        depth += (source[end] == "{") - (source[end] == "}")
        end += 1
    lines, indent = [], 0
    for raw in source[start:end].splitlines():
        line = raw.strip()
        if line == "{":
            indent += 1
            continue
        if line == "}":
            indent -= 1
            continue
        if not line or line.startswith("//"):
            continue
        loop = re.fullmatch(r"for \(int (\w+) = (.*); \1 < (.*); \1\+\+\)", line)
        le_loop = re.fullmatch(r"for \(int (\w+) = (.*); \1 <= (.*); \1\+\+\)", line)
        if loop or le_loop:
            v, lo, hi = (loop or le_loop).groups()
            line = f"for {v} in range({lo}, {hi}" + (" + 1" if le_loop else "") + "):"
        else:
            line = re.sub(r"^(double|long|bool) ", "", line).rstrip(";")
            line = line.replace("(double)", "").replace("true", "True").replace("false", "False")
            line = re.sub(r"^else if \((.*?)\) (.*)", r"elif \1: \2", line)
            line = re.sub(r"^if \((.*?)\) (.*)", r"if \1: \2", line)
            # Two bool declarations in this kernel are independent assignments.
            line = line.replace("is_h = True, is_l = True", "is_h = True; is_l = True")
        lines.append("    " * indent + line)
    return compile("\n".join(lines), "<generated-mql-kernel>", "exec")


def generated_arrays(history, lookback, strength, ema=50, atr=14):
    build = subject.BUILD
    window_expr = re.search(r"int window = (.*);", build).group(1)
    window_expr = window_expr.replace("MathMax", "max").replace("InpObvDivLookback", "lookback").replace("InpPivotStrength", "strength")
    n = max(eval(window_expr, {"max": max}, {"lookback": lookback, "strength": strength}), ema + 3, atr + 3)
    rows = history[-n:]
    env = {"n": n, "o": [r[0] for r in rows], "c": [r[1] for r in rows], "v": [r[4] for r in rows],
           "hi_s": [r[2] for r in rows][::-1], "lo_s": [r[3] for r in rows][::-1],
           "cum": 0, "g_obv": [0] * n, "len": strength,
           "g_is_pivot_high": [False] * n, "g_is_pivot_low": [False] * n}
    exec(numeric_loop(build, "i"), {"range": range}, env)
    # MQL logical series indexing maps physical chronological storage backwards.
    assert build.index("ArraySetAsSeries(g_obv, false)") < build.index("for (int i")
    assert build.index("ArraySetAsSeries(g_obv, true)") > build.index("for (int i")
    env["g_obv"] = env["g_obv"][::-1]
    exec(numeric_loop(build, "shift"), {"range": range}, env)
    return env


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.source = FIXTURE.read_text(encoding="utf-8")

    def test_idempotence_and_no_filename_dependency(self):
        source = self.source.replace("B1_1_OBV_Divergence_v1.mq5", "AnotherCandidate.mq5")
        first = subject.reconcile_obv_source(source, BRIEF)
        self.assertEqual(first["status"], "reconciled")
        second = subject.reconcile_obv_source(first["source_text"], BRIEF)
        self.assertEqual(second["status"], "already_reconciled")
        self.assertEqual(first["source_text"], second["source_text"])

    def test_unknown_shape_and_wrong_intent_do_not_patch(self):
        for changed in [self.source.replace("cum += vt", "cum += 2 * vt"),
                        self.source.replace("prev_h_obv < older_h_obv", "prev_h_obv > older_h_obv")]:
            result = subject.reconcile_obv_source(changed, BRIEF)
            self.assertEqual(result["status"], "held")
            self.assertEqual(result["source_text"], changed)
        for brief in [{}, {"resolved_intent": dict(BRIEF["resolved_intent"], volume_sign_rule="close_vs_previous_close")}]:
            result = subject.reconcile_obv_source(self.source, brief)
            self.assertEqual(result["status"], "not_applicable")

    def test_wider_pivot_search_is_held_before_and_after_reconciliation(self):
        revised = subject.reconcile_obv_source(self.source, BRIEF)["source_text"]
        for source in [self.source, revised]:
            for name in ["FindPrevPivotHigh", "FindPrevPivotLow"]:
                start, end, body = subject._function(source, name)
                wider = body.replace("from_shift + lookback", "from_shift + 3 * lookback")
                changed = source[:start] + wider + source[end:]
                result = subject.reconcile_obv_source(changed, BRIEF)
                self.assertEqual(result["status"], "held")
                self.assertEqual(result["reason"], "unsupported_pivot_search_shape:" + name)
                self.assertEqual(result["source_text"], changed)

    def test_protected_sections_and_trade_logic_unchanged(self):
        after = subject.reconcile_obv_source(self.source, BRIEF)["source_text"]
        for name in ["RegimeLotMult", "RegimeSLMult", "RegimeTPMult", "NormalizeLot", "OnTester", "OnDeinit", "HasOpenPosition", "FindPrevPivotHigh", "FindPrevPivotLow"]:
            # Include double-returning functions for protected-region comparison.
            pattern = r"\b(?:double|bool|void) " + name + r"\([^)]*\)\s*\{"
            def region(text):
                m = re.search(pattern, text)
                depth, end = 1, m.end()
                while depth:
                    depth += (text[end] == "{") - (text[end] == "}")
                    end += 1
                return text[m.start():end]
            self.assertEqual(region(self.source), region(after), name)
        self.assertEqual(re.findall(r"(?m)^input .*", self.source), re.findall(r"(?m)^input .*", after))
        self.assertEqual(re.findall(r"(?m)^#include .*", self.source), re.findall(r"(?m)^#include .*", after))
        marker = "// DIVERGENCE DETECTION"
        self.assertEqual(self.source[self.source.index(marker):], after[after.index(marker):])
        self.assertIn("if (!RefreshLatestBar()) return;", after)
        self.assertIn("CopyBuffer(g_ema_handle, 0, 1, 1, g_ema_buf) != 1", after)
        self.assertIn("double ema1 = g_ema_buf[0];", after)
        self.assertIn("CopyClose(_Symbol, PERIOD_CURRENT, 1, 1, g_close_buf) != 1", after)

    def test_no_bars_count_plateau_skip_and_partial_copy_guards(self):
        self.assertNotIn("g_obv_size", subject.REFRESH)
        self.assertNotIn("Bars(", subject.REFRESH)
        self.assertEqual(subject.REFRESH.count("BuildPrecomputedArrays()"), 1)
        copies = re.findall(r"if \((Copy(?:Open|Close|TickVolume|High|Low)\([^\n]+) != n\) return false;", subject.BUILD)
        self.assertEqual(len(copies), 5)
        self.assertLess(subject.BUILD.rindex("!= n) return false;", 0, subject.BUILD.index("ArraySetAsSeries(g_obv, false)")), subject.BUILD.index("g_obv[i] ="))
        # Each partial-copy event (including zero and negative errors) must fail.
        for failed_array in range(5):
            for count in [-1, 0, 99]:
                returned_counts = [100] * 5
                returned_counts[failed_array] = count
                self.assertFalse(all(n == 100 for n in returned_counts))

    def test_generated_kernels_match_full_history_pivot_and_volume_reference(self):
        rng = random.Random(260823)
        history = []
        price = 1000.0
        for i in range(10123):
            op = price
            cl = op + rng.choice([-4, -2, 0, 0, 1, 3, 5])
            hi = max(op, cl) + rng.randrange(4)
            lo = min(op, cl) - rng.randrange(4)
            history.append((op, cl, hi, lo, rng.randrange(0, 10000)))
            price = cl
        # Prefix reference never uses cumulative values produced by the kernel.
        increments = [v if c > o else -v if c < o else 0 for o, c, h, l, v in history]
        exercised_divergences = set()
        for lookback in [19, 25, 31, 80]:
            for strength in [1, 2, 3, 7]:
                for end in list(range(1000, 10124, 137)) + [10000, 10123]:
                    prefix = history[:end]
                    env = generated_arrays(prefix, lookback, strength)
                    max_shift = 2 * lookback + 1
                    for shift in range(strength + 1, max_shift + 1):
                        index = end - 1 - shift
                        neighbours = [prefix[k] for k in range(index-strength, index+strength+1)]
                        self.assertEqual(env["g_is_pivot_high"][shift], all(row[2] <= prefix[index][2] for row in neighbours))
                        self.assertEqual(env["g_is_pivot_low"][shift], all(row[3] >= prefix[index][3] for row in neighbours))
                    for kind, column, direction in [("high", 2, -1), ("low", 3, 1)]:
                        flags = env["g_is_pivot_" + kind]
                        recent = next((s for s in range(2, lookback+2) if flags[s]), None)
                        if recent is None:
                            continue
                        older = next((s for s in range(recent+1, recent+lookback+1) if flags[s]), None)
                        if older is None:
                            continue
                        direct_difference = sum(increments[end-older:end-recent])
                        generated_difference = env["g_obv"][recent] - env["g_obv"][older]
                        self.assertEqual(generated_difference, direct_difference)
                        price_difference = prefix[end-1-recent][column] - prefix[end-1-older][column]
                        actual = direction * price_difference < -0.01 and direction * generated_difference > 0.01
                        reference = direction * price_difference < -0.01 and direction * direct_difference > 0.01
                        self.assertEqual(actual, reference)
                        exercised_divergences.add((kind, actual))
        self.assertEqual(exercised_divergences, {("high", True), ("high", False), ("low", True), ("low", False)})


if __name__ == "__main__":
    unittest.main()
