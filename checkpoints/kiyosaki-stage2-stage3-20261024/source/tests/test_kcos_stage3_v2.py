import csv
import hashlib
import json
import os
import struct
import subprocess
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import kcos_stage3_evaluate_v2 as evaluator
import kcos_stage3_cron_v2 as stage3_cron
import kcos_stage3_duration_policy_v2 as duration_policy
import kcos_stage3_driver_v2 as driver
import kcos_stage3_launch_v2 as launcher
import kcos_stage3_prepare_v2 as preparer
import kcos_stage3_parse_v2 as stage3_parser
import kcos_stage3_publish_v2 as publisher
import kcos_stage3_stop_v2 as stage3_stop
import kcos_stage3_verify_v2 as verifier
import kcos_stage3_watch_v2 as watcher
import kcos_stage4_queue_v2 as stage4_queue
from kcos_stage3_common_v2 import sha256
import normalize_opt_ini


def build_stage2_ini_artifacts(spec, source_path, template_path, directory, cycle_id):
    source_text = source_path.read_text(encoding="utf-8-sig")
    approved = preparer.stage2_config_from_spec(spec, source_text=source_text)
    approved_hash = preparer.config_sha256(approved)
    runtime = preparer.stage3_config_from_spec(spec, source_text=source_text)
    runtime_spec = dict(spec); runtime_spec["tester_config"] = runtime
    template = template_path.read_text(encoding="utf-8-sig")
    files = {}
    for timeframe in ("M5", "M15", "H1"):
        rendered = preparer.render_ini(template, str(spec.get("ea", "EA")), timeframe,
                                       runtime_spec, approved["from_date"], approved["to_date"],
                                       source_text=source_text)
        rendered = preparer.apply_config_surface(rendered, runtime)
        rendered = preparer.rebuild_smoke_inputs_from_source(rendered, source_path, approved)
        rendered = preparer.ensure_stage1_parameter_surface(rendered, runtime_spec)
        rendered = preparer.apply_source_fixed_nflag_inputs(rendered, source_text)
        rendered = preparer.normalize_optimized_ini_numbers(rendered)
        rendered = preparer.apply_shared_input_surface(rendered, runtime_spec)
        preparer.validate_source_regime_contract(rendered, source_path, "default")
        preparer.validate_tester_inputs(rendered, runtime_spec, "default")
        path = directory / f"{timeframe}.ini"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
        normalize_opt_ini.normalize_opt_ini(path)
        files[timeframe] = {"path": str(path.resolve()), "sha256": sha256(path)}
    return {
        "schema_version": 1, "status": "published",
        "policy": "stage2_materialized_immutable_tester_inis_v1",
        "template_ini": str(template_path.resolve()), "template_ini_sha256": sha256(template_path),
        "source_sha256": sha256(source_path),
        "approved_tester_config_sha256": approved_hash,
        "timeframe_order": ["M5", "M15", "H1"], "files": files,
    }


class Stage3V2Tests(unittest.TestCase):
    def test_m5_continuation_archives_historical_m15_evidence_without_stage2_grid_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "stage3"
            output.mkdir()
            cycle = "cycle"
            handoff = {
                "cycle_id": cycle,
                "runtime_manifest": str((output / f"{cycle}.stage3.runtime.json").resolve()),
                "stage2_event_sha256": "A" * 64,
                "approved_tester_config_sha256": "B" * 64,
                "ini_sha256": {"M15": "C" * 64},
            }
            stale_event = output / f"{cycle}.stage3.M15.complete.json"
            stale_event.write_text(json.dumps({
                "cycle_id": cycle, "timeframe": "M15", "status": "TIMEFRAME_COMPLETE",
                "completed_utc": "2026-10-23T11:03:36Z",
            }), encoding="utf-8")
            stale_event_hash = sha256(stale_event)
            stale_parse = output / f"{cycle}.stage3.M15.parse.json"
            stale_parse.write_text("historical parse", encoding="utf-8")
            stale_opt = output / f"{cycle}.stage3.M15.opt"
            stale_opt.write_bytes(b"historical opt")

            result = driver.archive_unqualified_future_timeframes(handoff, "M5")

            self.assertEqual(["M15"], [item["timeframe"] for item in result["archived_timeframes"]])
            for source in (stale_event, stale_parse, stale_opt):
                self.assertFalse(source.exists())
            archived = result["archived_timeframes"][0]["artifacts"]
            self.assertEqual({stale_event_hash},
                             {item["sha256"] for item in archived if item["source"].endswith("complete.json")})
            self.assertTrue(all(Path(item["archive"]).is_file() for item in archived))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.cycle = "cycle-synthetic"
        self.handoff = self.root / "stage3-handoff.json"
        self.spec = self.root / "spec.json"
        self.source = self.root / "EA.mq5"
        self.source.write_text("input int InpPeriod = 20; // [lo=10,hi=30,step=1]\nint read(){return InpPeriod;}\n", encoding="utf-8")
        self.ex5 = self.root / "EA.ex5"
        self.ex5.write_bytes(b"test-ex5")
        self.spec.write_text(json.dumps({"strategy": {"parameters": ["InpPeriod=20"]}, "source_paths": [str(self.source.resolve())], "tester_config": {"schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"], "from_date": "2025.01.10", "to_date": "2026.01.10", "inputs": {"InpPeriod": {"default": 20, "start": 10, "step": 1, "stop": 30, "optimize": "Y"}}, "input_constraints": []}}), encoding="utf-8")
        self.stage2_event = self.root / "stage2-complete.json"
        self.stage2_event.write_text(json.dumps({"cycle_id": self.cycle, "stage": 2,
                                                 "stage1_spec_sha256": sha256(self.spec)}), encoding="utf-8")
        self.inis = {}
        for timeframe in ("M5", "M15", "H1"):
            ini = self.root / f"{timeframe}.ini"
            ini.write_text("[TesterInputs]\nInpPeriod=20||10||1||30||Y\n", encoding="utf-8")
            self.inis[timeframe] = ini
        self.handoff.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "EA",
            "from_date": "2025.01.10", "to_date": "2026.01.10", "timeframe_order": ["M5", "M15", "H1"],
            "stage1_spec": str(self.spec), "stage1_spec_sha256": sha256(self.spec),
            "stage2_event": str(self.stage2_event),
            "ini_files": {tf: str(path) for tf, path in self.inis.items()},
            "ini_sha256": {tf: sha256(path) for tf, path in self.inis.items()},
        }), encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def test_invalid_genetic_parameter_passes_are_diagnostic_not_run_fatal(self):
        log = self.root / "tester.log"
        log.write_text(
            'AA\t2\t12:00:00.000\tCore 1\tgenetic pass (0, 1) tested with error "incorrect input parameters" in 0:00:00.000\n'
            'BB\t0\t12:00:00.100\tCore 2\t1 (0, 2) OnTester result 0 : passed in 0:00:00.100\n'
            'CC\t0\t12:00:00.200\tTester\toptimization finished\n',
            encoding="utf-8",
        )

        result = launcher.read_fresh_tester_journal(log, 0)

        self.assertIsNone(result["code"])
        self.assertEqual([], result["error_lines"])
        self.assertEqual(1, len(result["invalid_parameter_passes"]))
        self.assertTrue(any("OnTester result" in line for line in result["fresh_lines"]))

    def test_real_input_grid_error_remains_fatal(self):
        log = self.root / "tester.log"
        log.write_text(
            'AA\t2\t12:00:00.000\tTester\toptimization cannot be started: step=0\n',
            encoding="utf-8",
        )

        result = launcher.read_fresh_tester_journal(log, 0)

        self.assertEqual("TESTER_INPUT_GRID_REJECTED", result["code"])
        self.assertEqual(1, len(result["error_lines"]))

    def test_minato_summary_captures_only_bound_appended_run_rows(self):
        agent = (self.root / "Tester" / "Agent-127.0.0.1-3001" / "MQL5" / "Files" /
                 "minato_v2_opt_results.csv")
        agent.parent.mkdir(parents=True)
        header = ("score_version\tea\tsymbol\tfitness\tprimary_diagnostic\tsecondary_flags\t"
                  "geometric_weekly_return_pct\treturn_base\treturn_bonus\treturn_confidence\t"
                  "trades\ttpm\ttpm_base\ttpm_bonus\tdd_pct\tdd_points\tsharpe\tsharpe_points\t"
                  "pf\tpf_points\tsample_confidence\tmin_margin\ttester_start\ttester_end\t"
                  "first_tick\tlast_tick\tending_equity\tlast_tick_balance\tontester_equity\t"
                  "ontester_balance\tendpoint_open\n")
        prefix = header.encode("ascii")
        agent.write_bytes(prefix)
        values = ["MINATO_BASE_SCORE_V2.0", "EA", "ETHUSD", "0", "ZERO_TRADES", "", "0", "0", "0", "0",
                  "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "1.7976931348623157e308",
                  "2025.01.10 00:00", "2026.01.10 00:00", "2025.01.10 00:00:30", "2026.01.09 23:58:59",
                  "10000", "10000", "10000", "10000", "0"]
        appended = ("\t".join(values) + "\n").encode("ascii")
        agent.write_bytes(prefix + appended)
        logs = agent.parents[2] / "logs" / "20261020.log"
        logs.parent.mkdir()
        logs.write_bytes(b"worker log")
        foreign_agent = (self.root / "Tester" / "Agent-127.0.0.1-3002" / "MQL5" / "Files" /
                         "minato_v2_opt_results.csv")
        foreign_agent.parent.mkdir(parents=True)
        foreign_agent.write_bytes(prefix + appended)
        runtime = {
            "cycle_id": "cycle", "stage": 3, "ea": "EA", "symbol": "ETHUSD", "timeframe": "M5",
            "agent_logs": [{"address": "127.0.0.1:3001", "path": str(logs)}],
            "minato_diagnostic_offsets_before": {
                str(agent.resolve()): {"address": "127.0.0.1:3001", "size": len(prefix),
                                       "prefix_sha256": hashlib.sha256(prefix).hexdigest().upper()},
                str(foreign_agent.resolve()): {"address": "127.0.0.1:3002", "size": len(prefix),
                                               "prefix_sha256": hashlib.sha256(prefix).hexdigest().upper()},
            },
        }
        handoff = {"cycle_id": "cycle", "ea": "EA", "symbol": "ETHUSD",
                   "from_date": "2025.01.10", "to_date": "2026.01.10"}

        result = stage3_parser.capture_minato_diagnostic_summary(
            handoff, runtime, self.root / "runtime.json", "M5", 1)

        self.assertEqual("COMPLETE", result["status"])
        self.assertEqual(1, result["record_count"])
        self.assertTrue(result["optimizer_row_count_matches"])
        self.assertEqual(0, result["rows_with_trades"])
        self.assertEqual(0, result["rows_with_open_endpoint"])
        self.assertEqual(0, result["rows_with_observed_margin"])
        self.assertEqual(["127.0.0.1:3001"], list(result["agents"]))
        self.assertEqual(1, result["agents"]["127.0.0.1:3001"]["appended_bytes"] > 0)

        mismatch = stage3_parser.capture_minato_diagnostic_summary(
            handoff, runtime, self.root / "runtime.json", "M5", 2)
        self.assertEqual("INCONCLUSIVE", mismatch["status"])
        self.assertIn("row count mismatch", mismatch["reason"])

    def test_minato_summary_rejects_changed_prefix_and_wrong_run_identity(self):
        agent = (self.root / "Tester" / "Agent-127.0.0.1-3001" / "MQL5" / "Files" /
                 "minato_v2_opt_results.csv")
        agent.parent.mkdir(parents=True)
        prefix = b"score_version\tea\tsymbol\ttrades\n"
        agent.write_bytes(prefix + b"run data\n")
        logs = agent.parents[2] / "logs" / "20261020.log"
        logs.parent.mkdir()
        logs.write_bytes(b"worker log")
        runtime = {
            "cycle_id": "cycle", "stage": 3, "ea": "EA", "symbol": "ETHUSD", "timeframe": "M5",
            "agent_logs": [{"address": "127.0.0.1:3001", "path": str(logs)}],
            "minato_diagnostic_offsets_before": {
                str(agent.resolve()): {"address": "127.0.0.1:3001", "size": len(prefix),
                                       "prefix_sha256": hashlib.sha256(prefix).hexdigest().upper()}
            },
        }
        handoff = {"cycle_id": "cycle", "ea": "EA", "symbol": "ETHUSD",
                   "from_date": "2025.01.10", "to_date": "2026.01.10"}
        agent.write_bytes(b"changed" + agent.read_bytes()[len(prefix):])

        result = stage3_parser.capture_minato_diagnostic_summary(
            handoff, runtime, self.root / "runtime.json", "M5", 1)

        self.assertEqual("INCONCLUSIVE", result["status"])
        self.assertIn("prefix changed", result["reason"])

    def test_domain_contract_keeps_search_estimates_distinct_from_hard_limits(self):
        estimates = stage3_parser.authenticated_source_domains({
            "tester_config": {"inputs": {
                "InpEstimate": {"optimize": "Y", "start": 0.01,
                                "step": 0.01, "stop": 0.2},
            }}
        })["InpEstimate"]
        self.assertEqual("stage1_search_estimate", estimates["basis"])
        self.assertFalse(estimates["hard_domain"])

        explicit = stage3_parser.authenticated_source_domains({
            "tester_config": {"inputs": {
                "InpBounded": {
                    "optimize": "Y", "start": 0.01, "step": 0.01,
                    "stop": 0.2,
                    "domain_provenance": {"source_domain": [0.001, 0.5, 0.001]},
                },
            }}
        })["InpBounded"]
        self.assertEqual("source_declared_domain", explicit["basis"])
        self.assertTrue(explicit["hard_domain"])

    def test_input_contract_failure_is_recorded_with_authenticated_upstream_hashes(self):
        stage2_event = self.root / "stage2-event-authenticated.json"
        stage2_event.write_text(json.dumps({"cycle_id": self.cycle, "stage": 2,
                                            "status": "complete"}), encoding="utf-8")
        stage2_handoff = self.root / "Stage2_v2.json"
        stage2_handoff.write_text(json.dumps({
            "cycle_id": self.cycle, "stage1_spec": str(self.spec.resolve()),
            "expected_event": str(stage2_event.resolve()),
        }), encoding="utf-8")
        state_path = self.root / "cycle-state.json"
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "current_ea": "EA",
            "current_stage": "stage3_pending", "stage_status": "stage2_complete_held",
            "hold_after_stage": 3,
            "run_authorization": {"schema_version": 1, "cycle_id": self.cycle,
                                  "scope": "current_cycle_stages_1_to_5",
                                  "authorized_by": "user_confirmed", "allowed_stages": [1, 2, 3, 4, 5],
                                  "hold_after_stage": 5,
                                  "reason": "User approved this full cycle through all five stages",
                                  "authorized_utc": "2026-10-09T15:34:06Z"},
        }), encoding="utf-8")

        event = preparer.record_input_contract_failure(
            state_path, stage2_handoff, "INPUT_CONSTRAINTS_UNDECLARED", "missing coupled-input contract")

        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage3_input_contract_rejected", current["stage_status"])
        self.assertEqual("rotate_stage1", current["next_action"])
        self.assertEqual(5, current["hold_after_stage"])
        self.assertEqual("STAGE3_INPUT_CONTRACT_FAILURE", event["failure_kind"])
        self.assertEqual(sha256(self.spec), event["evidence"]["stage1_spec"]["sha256"])
        self.assertEqual(sha256(stage2_event), event["evidence"]["stage2_event"]["sha256"])

    def test_optimized_default_and_type_must_match_ea_source(self):
        config = {"inputs": {
            "InpOmega": {"default": 2.5e-05, "optimize": "Y", "type": "double"},
            "InpSignedThreshold": {"default": -0.5, "optimize": "Y", "type": "double"},
        }}
        source = "input double InpOmega = 2.5e-05;\ninput double InpSignedThreshold = -0.5;"
        preparer.validate_optimized_input_source_contract(config, source)

        config["inputs"]["InpSignedThreshold"]["type"] = "int"
        with self.assertRaisesRegex(preparer.Stage3InputContractError,
                                    "Stage 1 type int conflicts with EA type double"):
            preparer.validate_optimized_input_source_contract(config, source)

    def test_optimized_default_mismatch_fails_even_when_wide_range_is_valid(self):
        config = {"inputs": {
            "InpOmega": {"default": 2.5e-04, "optimize": "Y", "type": "double"},
        }}
        with self.assertRaisesRegex(preparer.Stage3InputContractError,
                                    "Stage 1 default 0.00025 conflicts with EA default 2.5e-05"):
            preparer.validate_optimized_input_source_contract(
                config, "input double InpOmega = 2.5e-05;"
            )

    def test_input_surface_audit_labels_scientific_notation_bound_separately_from_default(self):
        config = {"inputs": {
            "InpOmega": {"default": 5e-05, "start": 1e-06, "stop": 1e-03,
                         "optimize": "Y", "type": "double"},
        }}
        rendered = "[TesterInputs]\nInpOmega=5e-05||2.5e-05||5e-06||0.0001||Y\n"
        audit = preparer.build_input_surface_audit(
            config, "input double InpOmega = 5e-05;", {"M15": rendered},
            cycle_id="test-cycle", ea="TestEA", source_path="TestEA.mq5",
            source_sha256="A" * 64, stage1_spec="spec.json",
            stage1_spec_sha256="B" * 64, config_sha256="C" * 64,
        )
        item = audit["timeframes"]["M15"]["optimized_inputs"]["InpOmega"]
        self.assertEqual(5e-05, item["ea_source_default"])
        self.assertEqual(2.5e-05, item["search_start"])
        self.assertEqual(5e-06, item["search_step"])
        self.assertEqual(0.0001, item["search_stop"])
        self.assertTrue(item["default_is_optimization_candidate"])
        self.assertEqual("positive", item["search_sign_domain"])
        self.assertEqual("stage1_estimate", item["domain_basis"])
        self.assertEqual(5, item["ea_default_grid_index"])
        self.assertAlmostEqual(1.0 / 3.0, item["ea_default_grid_position"])
        self.assertAlmostEqual(5.0, item["ea_default_interval_distance_to_nearest_edge"])

    def test_input_surface_audit_rejects_grid_violating_authenticated_domain_constraint(self):
        config = {"inputs": {
            "InpThreshold": {"default": 0.5, "start": -1, "stop": 1,
                             "optimize": "Y", "type": "double"},
        }, "input_constraints": [{
            "id": "threshold_nonnegative",
            "coefficients": {"InpThreshold": 1},
            "operator": ">=", "limit": 0,
        }]}
        rendered = "[TesterInputs]\nInpThreshold=0.5||-1||0.1||1||Y\n"
        with self.assertRaisesRegex(ValueError, "threshold_nonnegative rejects optimization grid"):
            preparer.build_input_surface_audit(
                config, "input double InpThreshold = 0.5;", {"M5": rendered},
                cycle_id="test-cycle", ea="TestEA", source_path="TestEA.mq5",
                source_sha256="A" * 64, stage1_spec="spec.json",
                stage1_spec_sha256="B" * 64, config_sha256="C" * 64,
            )

    def test_input_surface_audit_rejects_wide_grid_outside_15_to_20_intervals(self):
        config = {"inputs": {
            "InpOmega": {"default": 0.01, "start": 0.01, "stop": 0.2,
                         "optimize": "Y", "type": "double"},
        }}
        rendered = "[TesterInputs]\nInpOmega=0.01||0.01||0.01||0.24||Y\n"
        with self.assertRaisesRegex(ValueError, r"23 MT5 steps \(24 values\).*requires 15-20 steps"):
            preparer.build_input_surface_audit(
                config, "input double InpOmega = 0.01;", {"M5": rendered},
                cycle_id="test-cycle", ea="TestEA", source_path="TestEA.mq5",
                source_sha256="A" * 64, stage1_spec="spec.json",
                stage1_spec_sha256="B" * 64, config_sha256="C" * 64,
            )

    def test_input_surface_audit_records_cross_zero_without_guessing_it_is_invalid(self):
        config = {"inputs": {
            "InpSignedThreshold": {"default": -0.5, "start": -1.9, "stop": 1.1,
                                   "optimize": "Y", "type": "double"},
        }}
        rendered = "[TesterInputs]\nInpSignedThreshold=-0.5||-1.9||0.2||1.1||Y\n"
        audit = preparer.build_input_surface_audit(
            config, "input double InpSignedThreshold = -0.5;", {"H1": rendered},
            cycle_id="test-cycle", ea="TestEA", source_path="TestEA.mq5",
            source_sha256="A" * 64, stage1_spec="spec.json",
            stage1_spec_sha256="B" * 64, config_sha256="C" * 64,
        )
        item = audit["timeframes"]["H1"]["optimized_inputs"]["InpSignedThreshold"]
        self.assertEqual(-0.5, item["ea_source_default"])
        self.assertTrue(item["search_crosses_zero"])
        self.assertEqual("crosses_zero", item["search_sign_domain"])

    def test_input_surface_audit_labels_negative_only_domain_without_rejecting_it(self):
        config = {"inputs": {
            "InpShortThreshold": {"default": -0.5, "start": -2, "stop": -0.1,
                                  "optimize": "Y", "type": "double"},
        }}
        rendered = "[TesterInputs]\nInpShortThreshold=-0.5||-2||0.1||-0.1||Y\n"
        audit = preparer.build_input_surface_audit(
            config, "input double InpShortThreshold = -0.5;", {"H1": rendered},
            cycle_id="test-cycle", ea="TestEA", source_path="TestEA.mq5",
            source_sha256="A" * 64, stage1_spec="spec.json",
            stage1_spec_sha256="B" * 64, config_sha256="C" * 64,
        )
        item = audit["timeframes"]["H1"]["optimized_inputs"]["InpShortThreshold"]
        self.assertEqual("negative", item["search_sign_domain"])
        self.assertTrue(item["default_is_optimization_candidate"])

    def test_input_surface_audit_handles_boolean_defaults_generically(self):
        config = {"inputs": {
            "InpEnabled": {"default": True, "start": 0, "stop": 1,
                           "optimize": "Y", "type": "bool"},
        }}
        rendered = "[TesterInputs]\nInpEnabled=1||0||1||1||Y\n"
        audit = preparer.build_input_surface_audit(
            config, "input bool InpEnabled = true;", {"M5": rendered},
            cycle_id="test-cycle", ea="TestEA", source_path="TestEA.mq5",
            source_sha256="A" * 64, stage1_spec="spec.json",
            stage1_spec_sha256="B" * 64, config_sha256="C" * 64,
        )
        item = audit["timeframes"]["M5"]["optimized_inputs"]["InpEnabled"]
        self.assertEqual("true", item["ea_source_default_literal"])
        self.assertEqual(1.0, item["ea_source_default"])
        self.assertTrue(item["default_is_optimization_candidate"])

    def test_cron_driver_records_input_contract_failure_for_rotation(self):
        stage2_handoff = self.root / "Stage2_v2.json"
        stage2_handoff.write_text(json.dumps({
            "cycle_id": self.cycle,
            "stage1_spec": str(self.spec.resolve()),
            "expected_event": str(self.stage2_event.resolve()),
        }), encoding="utf-8")
        state_path = self.root / "cycle-state.json"
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "current_ea": "EA",
            "current_stage": "stage3_pending", "stage_status": "stage2_complete",
            "hold_after_stage": 3,
        }), encoding="utf-8")
        error = preparer.Stage3InputContractError(
            "SOURCE_INPUT_CONTRACT_MISMATCH", "InpOmega source default mismatch")

        with (mock.patch.object(driver, "stage3_admission_hold", return_value=None),
              mock.patch.object(driver, "stage3_repair_retry_is_ready", return_value=False),
              mock.patch.object(driver, "find_current_template", return_value=self.root / "template.ini"),
              mock.patch.object(driver.preparer, "prepare", side_effect=error)):
            result = driver.start_stage3(
                state_path, stage2_handoff, self.handoff, self.root / "lease.json",
                self.root / "output")

        current = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("STAGE3_INPUT_CONTRACT_REJECTED", result["status"])
        self.assertEqual("rotate_stage1", current["next_action"])
        self.assertTrue(Path(result["failure_event"]).is_file())

    def test_render_grid_failure_becomes_candidate_input_contract_rejection(self):
        detail = ("InpRegime_Fear_Lot_Mult default 0.25 cannot fit a "
                  "15-20 interval grid inside [0.25,0.25]")
        with mock.patch.object(preparer, "render_ini", side_effect=ValueError(detail)):
            with self.assertRaises(preparer.Stage3InputContractError) as raised:
                preparer.render_ini_for_candidate_contract(
                    "template", "EA", "M5", {}, "2025.01.10", "2026.01.10")
        self.assertEqual("INPUT_GRID_INVALID", raised.exception.code)

    def test_duration_retry_preserves_wide_profile_when_projection_fits_extended_cap(self):
        self.assertTrue(driver.duration_retry_fits_wide_budget({
            "status": "DURATION_REDUCTION_REQUIRED",
            "duration_policy_action": "resume_wide_or_review",
            "grid_mode": "default",
            "timeframe_budget_seconds": 4 * 60 * 60,
            "duration_projection": {"elapsed_seconds": 778, "completed_passes": 302,
                                    "projected_seconds": 7.51 * 60 * 60},
        }))
        self.assertFalse(driver.duration_retry_fits_wide_budget({
            "status": "DURATION_REDUCTION_REQUIRED",
            "duration_policy_action": "prepare_reduced_grid",
            "grid_mode": "reduced",
            "timeframe_budget_seconds": 4 * 60 * 60,
            "duration_projection": {"elapsed_seconds": 778, "completed_passes": 302,
                                    "projected_seconds": 7.51 * 60 * 60},
        }))
        self.assertTrue(driver.duration_retry_fits_wide_budget({
            "status": "DURATION_REDUCTION_REQUIRED",
            "duration_policy_action": "prepare_reduced_grid",
            "grid_mode": "default",
            "timeframe_budget_seconds": 9 * 60 * 60,
            "duration_projection": {"elapsed_seconds": 115, "completed_passes": 24,
                                    "projected_seconds": 50280},
        }))

    def write_result(self, tf, *, profit, cf, trades, dd, margin=120.0, name=None):
        csv_path = self.root / f"{tf}.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["Pass", "Profit", "CustomFitness", "Trades", "EquityDrawdownPercent", "SharpeRatio", "ProfitFactor", "MarginLevel"])
            writer.writeheader(); writer.writerow({"Pass": 1, "Profit": profit, "CustomFitness": cf, "Trades": trades,
                                                    "EquityDrawdownPercent": dd, "SharpeRatio": 1.2, "ProfitFactor": 1.4,
                                                    "MarginLevel": margin})
        opt = self.root / f"{tf}.opt"; opt.write_bytes(b"opt")
        evidence = self.root / (name or f"{tf}.evidence.json")
        evidence.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "EA",
            "timeframe": tf, "passed": True,
            "from_date": "2025.01.10", "to_date": "2026.01.10", "csv": str(csv_path), "csv_sha256": sha256(csv_path),
            "opt": str(opt), "opt_sha256": sha256(opt),
        }), encoding="utf-8")
        return evidence

    def test_reconcile_stale_lease_rejects_live_worker_process_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "output"; output.mkdir()
            runtime_path = output / "cycle.stage3.runtime.json"
            lease_path = root / "lease.json"
            runtime = {
                "schema_version": 2, "stage": 3, "cycle_id": "cycle",
                "status": "START_REQUIRED", "launched_epoch": 100.0,
                "terminal_pid": 11, "command": [r"C:\\portable\\terminal64.exe", "/portable"],
                "worker_pids": [], "agent_logs": [],
                "worker_process_evidence": [{
                    "pid": 21, "address": "127.0.0.1:3001",
                    "exe": r"C:\\portable\\metatester64.exe", "create_time": 110.0,
                }],
            }
            runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
            lease_path.write_text(json.dumps({"cycle_id": "cycle"}), encoding="utf-8")
            worker = mock.Mock()
            worker.name.return_value = "metatester64.exe"
            worker.exe.return_value = r"C:\\portable\\metatester64.exe"
            worker.cmdline.return_value = [worker.exe.return_value, "/local", "/address:127.0.0.1:3001", "/portable"]
            worker.create_time.return_value = 110.0

            def process_for(pid):
                if pid == 11:
                    raise preparer.psutil.NoSuchProcess(pid)
                return worker

            with mock.patch.object(preparer.psutil, "Process", side_effect=process_for):
                with self.assertRaisesRegex(ValueError, "recorded live PIDs: \\[21\\]"):
                    preparer.reconcile_stale_lease(lease_path, "cycle", output)
            self.assertTrue(lease_path.exists())

    def test_margin_below_120_forces_effective_cf_to_zero(self):
        evidence = self.write_result("M5", profit=1000, cf=30, trades=500, dd=5, margin=119.99)
        result = evaluator.load_completed_results(json.loads(self.handoff.read_text()), [evidence])[0]["M5"]
        self.assertEqual(30, result["raw_cf"])
        self.assertEqual(0, result["cf"])
        self.assertFalse(result["margin_gate_passed"])
        self.assertFalse(result["passed"])

    def test_margin_exactly_120_is_valid(self):
        evidence = self.write_result("M5", profit=1000, cf=30, trades=500, dd=5, margin=120.0)
        result = evaluator.load_completed_results(json.loads(self.handoff.read_text()), [evidence])[0]["M5"]
        self.assertEqual(30, result["cf"])
        self.assertTrue(result["margin_gate_passed"])

    def test_non_finite_margin_is_rejected(self):
        evidence = self.write_result("M5", profit=1000, cf=30, trades=500, dd=5, margin="nan")
        with self.assertRaisesRegex(ValueError, "non-finite MarginLevel"):
            evaluator.load_completed_results(json.loads(self.handoff.read_text()), [evidence])

    def test_coupled_constraints_ignore_undeclared_legacy_inputs(self):
        rendered = "[TesterInputs]\nInpPeriod=20||10||1||30||Y\n"
        result = preparer.apply_coupled_wide_constraints(rendered)
        self.assertEqual(rendered, result)

    def test_coupled_constraints_reject_partial_declared_family(self):
        rendered = "[TesterInputs]\nInpYzLowPct=0||0||0.1||2||Y\n"
        with self.assertRaisesRegex(ValueError, "coupled input family incomplete"):
            preparer.apply_coupled_wide_constraints(rendered)

    def test_stage3_uses_hash_bound_stage2_config_without_rebuilding_ranges(self):
        approved = {
            "schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
            "from_date": "2025.01.10", "to_date": "2026.01.10",
            "inputs": {"InpObvDivLookback": {
                "default": 25, "start": 15, "step": 1, "stop": 35, "optimize": "Y",
                "domain_provenance": {"method": "stage2_owned_optimization_grid"},
            }},
            "input_constraints": [],
        }
        handoff = {"approved_tester_config": approved,
                   "approved_tester_config_sha256": preparer.config_sha256(approved)}
        consumed = preparer.authenticated_stage2_tester_config(handoff)
        self.assertIs(consumed, approved)
        self.assertEqual((15, 1, 35), tuple(consumed["inputs"]["InpObvDivLookback"][key]
                                            for key in ("start", "step", "stop")))
        handoff["approved_tester_config"]["inputs"]["InpObvDivLookback"]["stop"] = 34
        with self.assertRaisesRegex(ValueError, "Stage 2 finalized tester_config hash mismatch"):
            preparer.authenticated_stage2_tester_config(handoff)

    def test_prepare_uses_approved_dates_and_enables_zero_token_launch(self):
        source, ex5, minato = self.root / "EA.mq5", self.root / "EA.ex5", self.root / "minato.mqh"
        source.write_text(self.source.read_text(encoding="utf-8"), encoding="utf-8")
        ex5.write_bytes(b"binary")
        minato.write_text("minato", encoding="utf-8")
        approved_config = preparer.stage2_config_from_spec(
            json.loads(self.spec.read_text(encoding="utf-8")),
            source_text=source.read_text(encoding="utf-8"))
        approved_config_hash = preparer.config_sha256(approved_config)
        verification = self.root / "verification.json"
        stage1_event = self.root / "stage1-event.json"
        stage1_event.write_text(json.dumps({"artifact": str(self.spec.resolve()),
                                            "artifact_sha256": sha256(self.spec)}), encoding="utf-8")
        template = self.root / "template.ini"
        template.write_text((Path(__file__).resolve().parent / "kcos_genetic_opt_template.stage3-utf8.ini")
                            .read_text(encoding="utf-8-sig"), encoding="utf-8")
        tester_ini_artifacts = build_stage2_ini_artifacts(
            json.loads(self.spec.read_text(encoding="utf-8")), source, template,
            self.root / "stage2-tester-inis", self.cycle)
        verification.write_text(json.dumps({"passed": True, "source": str(source), "ex5": str(ex5),
                                             "canonical_minato": str(minato),
                                             "score_version": "MINATO_BASE_SCORE_V2.0",
                                             "approved_tester_config_sha256": approved_config_hash,
                                             "stage1_spec_sha256": sha256(self.spec)}), encoding="utf-8")
        event = self.root / "stage2-event.json"
        event.write_text(json.dumps({"cycle_id": self.cycle, "stage": 2, "status": "complete", "verification": str(verification.resolve()),
                                     "verification_sha256": sha256(verification), "mq5_sha256": sha256(source), "ex5_sha256": sha256(ex5),
                                     "score_version": "MINATO_BASE_SCORE_V2.0",
                                     "canonical_minato_sha256": sha256(minato),
                                     "approved_tester_config_sha256": approved_config_hash,
                                     "tester_ini_artifacts": tester_ini_artifacts,
                                     "stage1_spec_sha256": sha256(self.spec)}), encoding="utf-8")
        state = self.root / "state.json"
        state_value = {"schema_version": 2, "cycle_id": self.cycle, "current_stage": "stage3_pending",
                       "stage_status": "stage2_complete_held", "current_ea": "EA",
                       "hold_after_stage": 2, "next_action": "audit_stage2_before_stage3"}
        state.write_text(json.dumps(state_value), encoding="utf-8")
        stage2 = self.root / "stage2-handoff.json"
        stage2.write_text(json.dumps({"schema_version": 2, "cycle_id": self.cycle, "stage": 2, "ea": "EA",
                                      "source": str(source), "approved_tester_config": approved_config,
                                      "approved_tester_config_sha256": approved_config_hash,
                                      "tester_ini_artifacts": tester_ini_artifacts,
                                      "expected_event": str(event), "expected_verification": str(verification),
                                      "stage1_spec": str(self.spec), "stage1_spec_sha256": sha256(self.spec),
                                      "stage1_event": str(stage1_event)}), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "one-run Stage 2 release authorization"):
            preparer.prepare(state, stage2, self.root / "handoff-out.json", self.root / "lease.json",
                             template, self.root / "inis", dry_run=True)
        authorization = preparer.build_stage2_release_authorization(
            state_value, stage2, authorized_by="user_confirmed", reason="test-only one-run Stage 3 release",
            authorized_utc="2026-10-03T00:00:00Z")
        state_value["stage2_release_attempt"] = dict(authorization, status="attempt_in_progress")
        state.write_text(json.dumps(state_value), encoding="utf-8")
        # The supervisor consumes the event after dispatch; this must not
        # invalidate the release authorization already sent to Stage 3.
        event_payload = json.loads(event.read_text(encoding="utf-8"))
        event_payload.update({"consumed": True, "consumed_utc": "2026-10-03T00:00:01Z",
                              "consumed_action": "start_stage3"})
        event.write_text(json.dumps(event_payload), encoding="utf-8")
        result = preparer.prepare(state, stage2, self.root / "handoff-out.json", self.root / "lease.json",
                                  template, self.root / "inis", dry_run=True,
                                  release_authorization=authorization)
        self.assertEqual("2025.01.10", result["from_date"]); self.assertEqual("2026.01.10", result["to_date"])
        self.assertTrue(result["auto_launch_allowed"])
        self.assertEqual("deterministic_zero_token_driver", result["continuation_mode"])
        self.assertEqual(64, len(result["ini_sha256"]["M5"]))
        self.assertFalse((self.root / "lease.json").exists())
        self.assertEqual("current_cycle_stage3_once", result["stage2_release"]["scope"])
        audit_preview = result["input_surface_audit_preview"]
        self.assertEqual(20, audit_preview["timeframes"]["M5"]["optimized_inputs"]["InpPeriod"]["ea_source_default"])
        prepared = preparer.prepare(state, stage2, self.root / "handoff-out.json", self.root / "lease.json",
                                    template, self.root / "inis", dry_run=False,
                                    release_authorization=authorization)
        audit_path = Path(prepared["input_surface_audit"]["path"])
        audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))
        self.assertEqual(sha256(audit_path), prepared["input_surface_audit"]["sha256"])
        self.assertEqual(sha256(self.root / "inis" / f"{self.cycle}.EA.ETHUSD.M5.20250110_20260110.wide.ini"),
                         audit_payload["timeframes"]["M5"]["ini_sha256"])
        updated_state = json.loads(state.read_text(encoding="utf-8"))
        self.assertEqual(3, updated_state["hold_after_stage"])
        self.assertEqual("stage3_m5_pending", updated_state["stage_status"])
        self.assertEqual("released", updated_state["stage2_release_attempt"]["status"])
        self.assertEqual("consumed", updated_state["stage2_release_audit"]["status"])
        self.assertEqual("current_cycle_stage3_once", prepared["stage2_release"]["scope"])
        self.assertEqual(preparer.sha256(self.root / "handoff-out.json"),
                         updated_state["stage2_release_audit"]["stage3_handoff_sha256"])
        self.assertEqual(str((self.root / "lease.json").resolve()),
                         json.loads((self.root / "handoff-out.json").read_text(encoding="utf-8"))["lease"])
        stage3_ini = Path(prepared["ini_files"]["M5"])
        stage2_ini = Path(tester_ini_artifacts["files"]["M5"]["path"])
        self.assertEqual(stage2_ini.read_bytes(), stage3_ini.read_bytes())
        self.assertEqual(tester_ini_artifacts["files"]["M5"]["sha256"], sha256(stage3_ini))

    def test_render_ini_discards_unapproved_template_input_surface(self):
        template = (
            "[Tester]\nEA_NAME=Old\nExpert=Old.ex5\nSymbol=ETHUSD\nPeriod=M5\n"
            "Optimization=2\nFromDate=2025.01.05\nToDate=2026.01.05\n"
            "OptimizationCriterion=6\nVisual=0\n[TesterInputs]\nInpEsWindowBars=2\n"
        )
        spec = json.loads(self.spec.read_text(encoding="utf-8"))
        spec["tester_config"] = preparer.stage2_config_from_spec(
            spec, source_text=self.source.read_text(encoding="utf-8-sig"))
        rendered = preparer.render_ini(template, "EA", "M5", spec)
        approved = preparer.config_from_spec(spec)
        rendered = preparer.rebuild_smoke_inputs_from_source(rendered, self.source, approved)
        self.assertNotIn("InpEsWindowBars", rendered)
        self.assertIn("InpPeriod=", rendered)
        period_row = next(row for row in rendered.splitlines() if row.startswith("InpPeriod="))
        _default, start, step, stop, enabled = period_row.split("=", 1)[1].split("||")
        self.assertEqual("Y", enabled)
        self.assertTrue(15 <= round((float(stop) - float(start)) / float(step)) <= 20)

    def test_render_ini_fixes_timing_controls_and_keeps_wide_regime_grid(self):
        template = (
            "[Tester]\nEA_NAME=Old\nExpert=Old.ex5\nSymbol=ETHUSD\nPeriod=M15\n"
            "Optimization=2\nFromDate=2025.01.10\nToDate=2026.01.10\n"
            "OptimizationCriterion=6\nVisual=0\n[TesterInputs]\n"
        )
        spec = json.loads(self.spec.read_text(encoding="utf-8"))
        inputs = spec["tester_config"]["inputs"]
        inputs["InpPeriod"].update({"start": 5, "step": 1, "stop": 20,
                                    "domain_provenance": {"method": "stage2_owned_optimization_grid"}})
        inputs["InpMaxTradeBars"] = {"type": "int", "default": 48,
            "start": 12, "step": 2, "stop": 104, "optimize": "Y"}
        inputs["InpMinBarsBetweenTrades"] = {"type": "int", "default": 12,
            "start": 2, "step": 2, "stop": 92, "optimize": "Y"}
        inputs["InpRegime_Greed_Lot_Mult"] = {"type": "double", "default": 1,
            "start": 0, "step": 1 / 3, "stop": 5, "optimize": "Y",
            "domain_provenance": {"method": "stage2_owned_optimization_grid"}}
        for name in ("InpMaxTradeBars", "InpMinBarsBetweenTrades"):
            inputs[name]["domain_provenance"] = {"method": "stage2_owned_optimization_grid"}
        rendered = preparer.render_ini(template, "EA", "M15", spec)
        rows = {line.split("=", 1)[0]: line.split("=", 1)[1]
                for line in rendered.splitlines()
                if line.startswith("Inp") and "=" in line}
        self.assertEqual("0", rows["InpMaxTradeBars"])
        self.assertNotIn("InpMinBarsBetweenTrades", rows)
        regime_default, regime_start, regime_step, regime_stop, regime_opt = rows["InpRegime_Greed_Lot_Mult"].split("||")
        self.assertEqual("1", regime_default)
        self.assertEqual("Y", regime_opt)
        self.assertTrue(15 <= round((float(regime_stop) - float(regime_start)) / float(regime_step)) <= 20)

    def test_authenticated_regime_lot_floor_can_contract_zero_endpoint(self):
        spec = {"tester_config": {
            "inputs": {"InpRegime_ExtFear_Lot_Mult": {
                "type": "double", "default": 1.0, "start": 5 / 13,
                "step": 4 / 13, "stop": 5.0, "optimize": "Y",
                "domain_provenance": {"method": "stage2_owned_optimization_grid"}}},
            "input_constraints": [{"id": "no_zero_extfear_exposure", "scope": "single",
                "coefficients": {"InpRegime_ExtFear_Lot_Mult": 1.0},
                "operator": ">=", "limit": 0.1}],
        }}
        ini = ("[TesterInputs]\n"
               "InpRegime_ExtFear_Lot_Mult=1||0.38461538461538464||0.3076923076923077||5||Y\n")
        preparer.validate_tester_inputs(ini, spec)
        with self.assertRaisesRegex(ValueError, "differs from Stage 2 approved grid"):
            preparer.validate_tester_inputs(
                ini.replace("1||0.38461538461538464||0.3076923076923077||5||Y",
                            "1||0.5||0.3||5||Y"), spec)

    @unittest.skip("Stage 3 no longer derives or repairs ranges; Stage 2 owns this behavior")
    def test_degenerate_canonical_anchor_uses_authenticated_stage1_wide_domain(self):
        spec = {"tester_config": {"inputs": {"InpMarginKillLevel": {
            "default": 120, "start": 50, "step": 10, "stop": 300, "optimize": "Y"
        }}}}
        template = "[TesterInputs]\nInpMarginKillLevel=120||120||1||120||Y\n"
        with mock.patch.object(preparer, "_canonical_stage3_wide_range", return_value=(120, 1, 120)):
            rendered = preparer.apply_wide_grid_policy(template, spec)
        row = next(line for line in rendered.splitlines() if line.startswith("InpMarginKillLevel="))
        _, fields = row.split("=", 1)
        _default, start, step, stop, optimize = fields.split("||")
        start, step, stop = float(start), float(step), float(stop)
        self.assertEqual(optimize, "Y")
        self.assertTrue(50 <= start < stop <= 300)
        self.assertTrue(15 <= round((stop - start) / step) <= 20)

    @unittest.skip("Stage 3 no longer derives or repairs ranges; Stage 2 owns this behavior")
    def test_wide_grid_preserves_signed_regime_domains_and_typed_steps(self):
        inputs = {
            "InpRegimeZLongEntry": {"type": "double", "default": 0.5, "start": 0, "step": 0.25, "stop": 2, "optimize": "Y"},
            "InpRegimeZShortEntry": {"type": "double", "default": -0.5, "start": -2, "step": 0.25, "stop": 0, "optimize": "Y"},
            "InpRegimeVolLookup": {"type": "int", "default": 20, "start": 10, "step": 5, "stop": 50, "optimize": "Y"},
            "InpRegimeVolZEntry": {"type": "double", "default": 0, "start": -1.5, "step": 0.25, "stop": 1.5, "optimize": "Y"},
            "InpBaseLot": {"type": "double", "default": 0.1, "start": 0.05, "step": 0.05, "stop": 0.5, "optimize": "Y"},
            "InpMinBarsBetweenTrades": {"type": "int", "default": 120, "start": 60, "step": 30, "stop": 240, "optimize": "Y"},
            "InpRegime_Greed_Lot_Mult": {"type": "double", "default": 1, "start": 0, "step": 0.25, "stop": 5, "optimize": "Y"},
        }
        rows = "\n".join(f"{name}={cfg['default']}||0||1||1||Y" for name, cfg in inputs.items())
        rendered = preparer.apply_wide_grid_policy("[TesterInputs]\n" + rows + "\n", {"tester_config": {"inputs": inputs}})
        parsed = {}
        for line in rendered.splitlines():
            if "=" not in line or "||" not in line:
                continue
            name, value = line.split("=", 1)
            default, start, step, stop, _flag = value.split("||")
            parsed[name] = tuple(map(float, (default, start, step, stop)))
        for name in ("InpRegimeZLongEntry", "InpRegimeZShortEntry", "InpRegimeVolLookup", "InpRegimeVolZEntry", "InpBaseLot", "InpMinBarsBetweenTrades"):
            default, start, step, stop = parsed[name]
            self.assertLessEqual(start, default, name)
            self.assertGreaterEqual(stop, default, name)
            self.assertGreater(step, 0, name)
            self.assertTrue(15 <= round((stop - start) / step) <= 20, (name, parsed[name]))
            self.assertAlmostEqual(round((default - start) / step), (default - start) / step, places=8, msg=name)
        self.assertLess(parsed["InpRegimeZShortEntry"][1], 0)
        self.assertLess(parsed["InpRegimeVolZEntry"][1], 0)
        self.assertTrue(all(parsed["InpRegimeVolLookup"][i].is_integer() for i in (1, 2, 3)))
        self.assertLessEqual(parsed["InpBaseLot"][3], 0.5)
        self.assertEqual((0.0, 0.25, 5.0), parsed["InpRegime_Greed_Lot_Mult"][1:])

    @unittest.skip("Stage 3 no longer derives or repairs ranges; Stage 2 owns this behavior")
    def test_wide_grid_rephases_off_grid_defaults_without_assuming_sign(self):
        inputs = {
            "InpBaseLot": {"type": "double", "default": 0.1, "start": 0.01, "step": 0.025, "stop": 0.485, "optimize": "Y"},
            "InpMaxTradeBars": {"type": "int", "default": 48, "start": 24, "step": 5, "stop": 104, "optimize": "Y"},
            "InpMinBarsBetweenTrades": {"type": "int", "default": 12, "start": 1, "step": 5, "stop": 96, "optimize": "Y"},
            "InpMarginKillLevel": {"type": "double", "default": 200, "start": 100, "step": 40, "stop": 780, "optimize": "Y"},
            "InpSignedThreshold": {"type": "double", "default": -0.5, "start": -2, "step": 0.3, "stop": 1, "optimize": "Y"},
        }
        rows = "\n".join(
            f"{name}={cfg['default']}||{cfg['start']}||{cfg['step']}||{cfg['stop']}||Y"
            for name, cfg in inputs.items()
        )
        rendered = preparer.apply_wide_grid_policy(
            "[TesterInputs]\n" + rows + "\n", {"tester_config": {"inputs": inputs}}
        )
        parsed = {}
        for line in rendered.splitlines():
            if "=" not in line or "||" not in line:
                continue
            name, value = line.split("=", 1)
            parsed[name] = tuple(map(float, value.split("||")[:4]))
        for name, cfg in inputs.items():
            default, start, step, stop = parsed[name]
            self.assertAlmostEqual(cfg["default"], default, places=9, msg=name)
            self.assertLessEqual(start, default, name)
            self.assertGreaterEqual(stop, default, name)
            self.assertAlmostEqual(round((default - start) / step), (default - start) / step, places=8, msg=name)
            self.assertTrue(15 <= round((stop - start) / step) <= 20, (name, parsed[name]))
        self.assertLess(parsed["InpSignedThreshold"][1], 0)
        self.assertGreater(parsed["InpSignedThreshold"][3], 0)

    @unittest.skip("Stage 3 no longer derives or repairs ranges; Stage 2 owns this behavior")
    def test_wide_grid_uses_estimated_scale_and_preserves_declared_sign(self):
        inputs = {
            "InpNegativeThreshold": {
                "type": "double", "default": -0.5, "start": -2.0,
                "step": 0.1, "stop": -0.1, "optimize": "Y"
            },
            "InpNegativeCount": {
                "type": "int", "default": -2, "start": -30,
                "step": 1, "stop": -1, "optimize": "Y"
            },
            "InpOmega": {
                "type": "double", "default": 2.5e-05, "start": 5e-06,
                "step": 5e-06, "stop": 1e-04, "optimize": "Y"
            },
        }
        rows = "\n".join(
            f"{name}={cfg['default']}||0||1||1||Y"
            for name, cfg in inputs.items()
        )
        spec = {"tester_config": {"inputs": inputs}}
        rendered = preparer.apply_wide_grid_policy(
            "[TesterInputs]\n" + rows + "\n", spec
        )
        preparer.validate_tester_inputs(rendered, spec)
        parsed = {}
        for line in rendered.splitlines():
            if "=" not in line or "||" not in line:
                continue
            name, value = line.split("=", 1)
            parsed[name] = tuple(map(float, value.split("||")[:4]))

        for name in ("InpNegativeThreshold", "InpNegativeCount"):
            default, start, step, stop = parsed[name]
            self.assertLess(start, stop)
            self.assertGreater(step, 0)
            self.assertLess(stop, 0, name)
            self.assertLessEqual(start, default, name)
            self.assertGreaterEqual(stop, default, name)
            intervals = (stop - start) / step
            self.assertTrue(15 <= intervals <= 20, (name, parsed[name]))
            self.assertAlmostEqual((default - start) / step,
                                   round((default - start) / step), places=8)

        default, start, step, stop = parsed["InpOmega"]
        self.assertAlmostEqual(default, 2.5e-05, places=12)
        self.assertGreater(start, 0)
        self.assertGreaterEqual(stop, default)
        self.assertTrue(15 <= (stop - start) / step <= 20)

    @unittest.skip("Stage 3 no longer derives or repairs ranges; Stage 2 owns this behavior")
    def test_wide_grid_preserves_explicit_cross_zero_estimate(self):
        cfg = {"type": "double", "default": -0.25, "start": -1.0,
               "step": 0.1, "stop": 0.5, "optimize": "Y"}
        spec = {"tester_config": {"inputs": {"InpThreshold": cfg}}}
        ini = "[TesterInputs]\nInpThreshold=-0.25||-1||0.1||0.5||Y\n"
        rendered = preparer.apply_wide_grid_policy(ini, spec)
        preparer.validate_tester_inputs(rendered, spec)
        _, start, step, stop = map(
            float, rendered.splitlines()[1].split("=", 1)[1].split("||")[:4]
        )
        self.assertLess(start, 0)
        self.assertGreater(stop, 0)
        self.assertTrue(15 <= (stop - start) / step <= 20)

    @unittest.skip("Stage 3 no longer derives or repairs ranges; Stage 2 owns this behavior")
    def test_wide_grid_fails_closed_without_any_declared_search_bounds(self):
        cfg = {"type": "double", "default": 2.5e-05, "optimize": "Y"}
        spec = {"tester_config": {"inputs": {"InpUnknownScale": cfg}}}
        ini = "[TesterInputs]\nInpUnknownScale=2.5e-05||0||1||1||Y\n"
        with self.assertRaisesRegex(ValueError, "refusing to guess its Stage 3 sign/scale"):
            preparer.apply_wide_grid_policy(ini, spec)

    def test_executable_bounds_parse_scientific_notation_and_intersect(self):
        source = self.root / "bounds.mq5"
        source.write_text(
            "if (InpOmega < 1e-6 || InpOmega > 1e-3) return INIT_PARAMETERS_INCORRECT;\n"
            "if (InpOmega < 2.5e-6 || InpOmega > 5e-4) return INIT_PARAMETERS_INCORRECT;\n",
            encoding="utf-8",
        )
        bounds = preparer._runtime_source_bounds({"source_paths": [str(source)]})
        self.assertEqual(bounds["InpOmega"], (2.5e-06, 5e-04))

    def test_stage1_source_path_objects_are_supported_by_grid_contract_readers(self):
        source = self.root / "structured-source.mq5"
        source.write_text(
            "input int InpPeriod = 20;\n"
            "if (InpPeriod < 5 || InpPeriod > 50) return INIT_PARAMETERS_INCORRECT;\n",
            encoding="utf-8",
        )
        spec = {"source_paths": [{"path": str(source), "role": "authenticated EA source"}]}
        self.assertEqual(preparer._runtime_source_bounds(spec)["InpPeriod"], (5.0, 50.0))
        self.assertEqual(preparer._source_input_types(spec)["InpPeriod"], "int")

    def test_validator_rejects_optimized_grid_that_omits_default(self):
        spec = {"tester_config": {"inputs": {"InpValue": {
            "type": "double", "default": 20, "start": 0, "step": 1.875, "stop": 30, "optimize": "Y"
        }}}}
        ini = "[TesterInputs]\nInpValue=20||0||1.875||30||Y\n"
        with self.assertRaisesRegex(ValueError, "default.*not on optimization grid"):
            preparer.validate_tester_inputs(ini, spec)

    def test_constraint_shaping_preserves_default_and_wide_grid_generically(self):
        # These deliberately use model-specific names only as fixture data;
        # the writer must operate from the declared coefficient map.
        cases = [
            ({
                "InpQ": {"type": "double", "default": 0.01, "start": 0.01, "step": 0.01, "stop": 0.20, "optimize": "Y"},
                "InpP": {"type": "double", "default": 0.94, "start": 0.80, "step": 0.01, "stop": 0.99, "optimize": "Y"},
            }, {"InpQ": 1, "InpP": 1}, 1.0),
            ({
                "InpAlpha": {"type": "double", "default": 0.06, "start": 0.01, "step": 0.01, "stop": 0.16, "optimize": "Y"},
                "InpBeta": {"type": "double", "default": 0.92, "start": 0.80, "step": 0.01, "stop": 0.99, "optimize": "Y"},
            }, {"InpAlpha": 1, "InpBeta": 1}, 1.0),
        ]
        for inputs, coefficients, limit in cases:
            with self.subTest(inputs=tuple(inputs)):
                spec = {"tester_config": {"inputs": inputs, "input_constraints": [{
                    "id": "persistence_sum", "coefficients": coefficients,
                    "operator": "<", "limit": limit,
                }]}}
                rows = "\n".join(
                    f"{name}={cfg['default']}||{cfg['start']}||{cfg['step']}||{cfg['stop']}||Y"
                    for name, cfg in inputs.items()
                )
                ini = "[TesterInputs]\n" + rows + "\n"
                ini = preparer.align_optimized_defaults(ini, spec)
                ini = preparer.constrain_input_grid_ranges(ini, spec)
                ini = preparer.align_optimized_defaults(ini, spec)
                preparer.validate_tester_inputs(ini, spec)
                parsed = {
                    name: tuple(map(float, value.split("||")[:4]))
                    for name, value in (line.split("=", 1) for line in ini.splitlines() if "||" in line)
                }
                worst_corner = sum(
                    coefficient * (parsed[name][3] if coefficient >= 0 else parsed[name][1])
                    for name, coefficient in coefficients.items()
                )
                self.assertLess(worst_corner, limit)
                for name, cfg in inputs.items():
                    default, start, step, stop = parsed[name]
                    self.assertAlmostEqual(default, cfg["default"], places=10)
                    self.assertTrue(start <= default <= stop)
                    self.assertAlmostEqual((default - start) / step,
                                           round((default - start) / step), places=8)
                    self.assertTrue(15 <= round((stop - start) / step) <= 20)

    def test_constraint_shaping_recovers_integer_grid_on_favorable_edge(self):
        spec = {"tester_config": {
            "inputs": {
                "InpFast": {"type": "int", "default": 12, "start": 4,
                            "step": 2, "stop": 32, "optimize": "Y"},
                "InpSlow": {"type": "int", "default": 48, "start": 16,
                            "step": 4, "stop": 96, "optimize": "Y"},
            },
            "input_constraints": [
                {"id": "fast_nonnegative", "operator": ">=", "limit": 0,
                 "coefficients": {"InpFast": 1}},
                {"id": "slow_nonnegative", "operator": ">=", "limit": 0,
                 "coefficients": {"InpSlow": 1}},
                {"id": "slow_above_fast", "operator": ">=", "limit": 4,
                 "coefficients": {"InpSlow": 1, "InpFast": -1}},
            ],
        }}
        ini = ("[TesterInputs]\n"
               "InpFast=12||12||1||32||Y\n"
               "InpSlow=48||16||4||96||Y\n")
        ini = preparer.constrain_input_grid_ranges(ini, spec)
        ini = preparer.align_optimized_defaults(ini, spec)
        for name, item in spec["tester_config"]["inputs"].items():
            fields = next(line.split("=", 1)[1].split("||")
                          for line in ini.splitlines() if line.startswith(name + "="))
            item.update({"start": float(fields[1]), "step": float(fields[2]),
                         "stop": float(fields[3]),
                         "domain_provenance": {"method": "stage2_owned_optimization_grid"}})
        preparer.validate_tester_inputs(ini, spec)
        parsed = {
            name: tuple(map(float, value.split("||")[:4]))
            for name, value in (line.split("=", 1) for line in ini.splitlines()
                                if "||" in line)
        }
        for name, expected_default in (("InpFast", 12), ("InpSlow", 48)):
            default, start, step, stop = parsed[name]
            self.assertTrue(all(value.is_integer() for value in (default, start, step, stop)))
            self.assertEqual(default, expected_default)
            self.assertTrue(15 <= round((stop - start) / step) <= 20)
            self.assertAlmostEqual((default - start) / step,
                                   round((default - start) / step), places=8)
        self.assertGreaterEqual(parsed["InpSlow"][1] - parsed["InpFast"][3], 4)

    def test_constraint_shaping_handles_signed_ordered_inputs(self):
        spec = {"tester_config": {
            "inputs": {
                "InpLower": {"type": "double", "default": -0.5, "start": -2, "step": 0.1, "stop": 1, "optimize": "Y"},
                "InpUpper": {"type": "double", "default": 0.5, "start": -1, "step": 0.1, "stop": 2, "optimize": "Y"},
            },
            "input_constraints": [{
                "id": "ordered", "coefficients": {"InpLower": 1, "InpUpper": -1},
                "operator": "<", "limit": 0,
            }],
        }}
        ini = "[TesterInputs]\nInpLower=-0.5||-2||0.1||1||Y\nInpUpper=0.5||-1||0.1||2||Y\n"
        ini = preparer.align_optimized_defaults(ini, spec)
        ini = preparer.constrain_input_grid_ranges(ini, spec)
        ini = preparer.align_optimized_defaults(ini, spec)
        preparer.validate_tester_inputs(ini, spec)
        parsed = {name: tuple(map(float, value.split("||")[:4]))
                  for name, value in (line.split("=", 1) for line in ini.splitlines() if "||" in line)}
        self.assertLess(parsed["InpLower"][1] - parsed["InpUpper"][3], 0)
        self.assertLess(parsed["InpLower"][1], 0)
        self.assertGreater(parsed["InpUpper"][3], 0)

    def test_constraints_accept_fixed_scalar_tester_inputs(self):
        spec = {"tester_config": {
            "inputs": {
                "InpLots": {"type": "double", "default": 0.5,
                            "start": 0.5, "step": 0, "stop": 0.5, "optimize": "N"},
                "InpPeriod": {"type": "int", "default": 20,
                              "start": 10, "step": 1, "stop": 30, "optimize": "Y"},
            },
            "input_constraints": [{
                "id": "InpLots_nonnegative", "coefficients": {"InpLots": 1},
                "operator": ">=", "limit": 0,
            }],
        }}
        ini = "[TesterInputs]\nInpLots=0.5\nInpPeriod=20||10||1||30||Y\n"
        preparer.validate_input_constraints(ini, spec)
        constrained = preparer.constrain_input_grid_ranges(ini, spec)
        self.assertEqual(ini, constrained)

    def test_quarter_step_regime_grid_aligns_off_phase_authenticated_default(self):
        spec = {"tester_config": {"inputs": {
            "InpRegime_ExtFearLot": {
                "type": "double", "default": 0.125, "start": 0,
                "step": 0.25, "stop": 5, "optimize": "Y",
            },
        }, "input_constraints": []}}
        ini = "[TesterInputs]\nInpRegime_ExtFearLot=0.125||0||0.25||5||Y\n"
        aligned = preparer.align_optimized_defaults(ini, spec)
        preparer.validate_tester_inputs(aligned, spec)
        row = aligned.splitlines()[1].split("=", 1)[1].split("||")
        default, start, step, stop = map(float, row[:4])
        self.assertAlmostEqual(default, 0.125)
        self.assertAlmostEqual(step, 0.25)
        self.assertGreaterEqual(start, 0)
        self.assertLessEqual(stop, 5)
        self.assertLessEqual(start, default)
        self.assertGreaterEqual(stop, default)
        self.assertTrue(15 <= round((stop - start) / step) <= 20)

    def test_strict_constraint_near_valid_default_uses_validator_tolerance(self):
        default = 0.9999999995
        spec = {"tester_config": {
            "inputs": {"InpNearLimit": {
                "type": "double", "default": default, "start": 0.5,
                "step": 0.05, "stop": 1.4, "optimize": "Y",
            }},
            "input_constraints": [{
                "id": "below_one", "coefficients": {"InpNearLimit": 1},
                "operator": "<", "limit": 1,
            }],
        }}
        ini = f"[TesterInputs]\nInpNearLimit={default}||0.5||0.05||1.4||Y\n"
        ini = preparer.align_optimized_defaults(ini, spec)
        ini = preparer.constrain_input_grid_ranges(ini, spec)
        ini = preparer.align_optimized_defaults(ini, spec)
        preparer.validate_tester_inputs(ini, spec)

    def test_default_alignment_fails_closed_when_default_is_outside_bounds(self):
        spec = {"tester_config": {"inputs": {"InpValue": {
            "type": "double", "default": 25, "start": 0, "step": 1, "stop": 20, "optimize": "Y"
        }}}}
        ini = "[TesterInputs]\nInpValue=25||0||1||20||Y\n"
        with self.assertRaisesRegex(ValueError, "default 25 is outside optimization bounds"):
            preparer.align_optimized_defaults(ini, spec)

    def test_validator_rejects_cross_input_grid_that_breaks_declared_constraint(self):
        spec = {"tester_config": {
            "inputs": {
                "InpAlpha": {"type": "double", "default": 0.06, "start": 0.01, "step": 0.01, "stop": 0.16, "optimize": "Y"},
                "InpBeta": {"type": "double", "default": 0.92, "start": 0.80, "step": 0.01, "stop": 0.99, "optimize": "Y"},
            },
            "input_constraints": [{
                "id": "garch_persistence", "coefficients": {"InpAlpha": 1, "InpBeta": 1},
                "operator": "<", "limit": 1.0,
            }],
        }}
        ini = (
            "[TesterInputs]\n"
            "InpAlpha=0.06||0.01||0.01||0.16||Y\n"
            "InpBeta=0.92||0.80||0.01||0.99||Y\n"
        )
        with self.assertRaisesRegex(ValueError, "garch_persistence rejects optimization grid"):
            preparer.validate_tester_inputs(ini, spec)

    def test_validator_accepts_signed_domain_with_declared_linear_constraint(self):
        spec = {"tester_config": {
            "inputs": {
                "InpLow": {"type": "double", "default": -0.5, "start": -2, "step": 0.1, "stop": -0.5, "optimize": "Y"},
                "InpHigh": {"type": "double", "default": 0.5, "start": 0.5, "step": 0.1, "stop": 2, "optimize": "Y"},
            },
            "input_constraints": [{
                "id": "ordered_thresholds", "coefficients": {"InpLow": 1, "InpHigh": -1},
                "operator": "<", "limit": 0.0,
            }],
        }}
        ini = (
            "[TesterInputs]\n"
            "InpLow=-0.5||-2||0.1||-0.5||Y\n"
            "InpHigh=0.5||0.5||0.1||2||Y\n"
        )
        preparer.validate_tester_inputs(ini, spec)

    def test_expected_parameters_unions_strategy_list_and_tester_config(self):
        spec = {
            "strategy": {"parameters": ["InpDccLookback=100"]},
            "tester_config": {"inputs": {"InpDccLookback": {}, "InpMagicNumber": {}}},
        }
        self.assertEqual({"InpDccLookback", "InpMagicNumber"}, stage3_parser.expected_parameters(spec))

    def test_fixed_source_input_is_admitted_only_when_ini_retains_authenticated_default(self):
        source = self.root / "EA-with-magic.mq5"
        source.write_text(
            "input int InpPeriod = 20;\ninput ulong InpMagicNumber = 123456;\n",
            encoding="utf-8",
        )
        ini = self.root / "M5-with-magic.ini"
        ini.write_text(
            "[TesterInputs]\nInpPeriod=20||10||1||30||Y\n"
            "InpMagicNumber=123456||123456||1||123456||N\n",
            encoding="utf-8",
        )
        _declared, _optimized, fixed = stage3_parser.ini_parameter_surface(ini)
        self.assertEqual(
            {"InpMagicNumber"},
            stage3_parser.authenticated_fixed_source_inputs(source, ini, {"InpPeriod"}, fixed),
        )

    def test_fixed_source_input_mismatch_is_rejected(self):
        source = self.root / "EA-with-magic.mq5"
        source.write_text("input ulong InpMagicNumber = 123456;\n", encoding="utf-8")
        ini = self.root / "M5-with-magic.ini"
        ini.write_text(
            "[TesterInputs]\nInpMagicNumber=123457||123457||1||123457||N\n",
            encoding="utf-8",
        )
        _declared, _optimized, fixed = stage3_parser.ini_parameter_surface(ini)
        with self.assertRaisesRegex(ValueError, "differs from its authenticated EA default"):
            stage3_parser.authenticated_fixed_source_inputs(source, ini, set(), fixed)

    def test_unexpected_optimized_source_input_is_not_admitted_as_fixed_metadata(self):
        source = self.root / "EA-with-magic.mq5"
        source.write_text("input ulong InpMagicNumber = 123456;\n", encoding="utf-8")
        ini = self.root / "M5-with-magic.ini"
        ini.write_text(
            "[TesterInputs]\nInpMagicNumber=123456||123000||1||124000||Y\n",
            encoding="utf-8",
        )
        _declared, optimized, fixed = stage3_parser.ini_parameter_surface(ini)
        self.assertIn("InpMagicNumber", optimized)
        self.assertNotIn("InpMagicNumber", fixed)
        self.assertEqual(set(), stage3_parser.authenticated_fixed_source_inputs(source, ini, set(), fixed))
    def test_new_cycle_full_run_authorization_ignores_prior_cycle_stage2_release(self):
        state = {
            "schema_version": 2, "cycle_id": "current-cycle", "current_stage": "stage3_pending",
            "stage_status": "stage2_complete_held", "hold_after_stage": 2,
            "next_action": "audit_stage2_before_stage3",
            "run_authorization": {
                "schema_version": 1, "scope": "current_cycle_stages_1_to_5",
                "cycle_id": "current-cycle", "authorized_by": "user_confirmed",
                "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
            },
            "stage2_release_attempt": {"cycle_id": "previous-cycle", "status": "released"},
        }
        self.assertTrue(driver.should_attempt_stage2_release(state))

    def test_stage3_admission_accepts_authorized_full_cycle_hold_after_stage5(self):
        state = {
            "schema_version": 2,
            "cycle_id": "current-cycle",
            "current_stage": "stage3_pending",
            "current_ea": "FixtureEA",
            "stage_status": "stage2_complete_ready",
            "hold_after_stage": 5,
            "next_action": "run_stage3",
            "run_authorization": {
                "schema_version": 1,
                "cycle_id": "current-cycle",
                "scope": "current_cycle_stages_1_to_5",
                "allowed_stages": [1, 2, 3, 4, 5],
                "authorized_by": "user_confirmed",
                "hold_after_stage": 5,
                "reason": "User authorized this cycle through Stage 5.",
                "authorized_utc": "2026-10-10T00:00:00Z",
            },
        }
        state_path = self.root / "full-cycle-state.json"
        state_path.write_text(json.dumps(state), encoding="utf-8")
        template_path = self.root / "full-cycle-template.ini"
        template_path.write_text((Path(__file__).resolve().parent / "kcos_genetic_opt_template.stage3-utf8.ini")
                                 .read_text(encoding="utf-8-sig"), encoding="utf-8")
        tester_ini_artifacts = build_stage2_ini_artifacts(
            json.loads(self.spec.read_text(encoding="utf-8")), self.source, template_path,
            self.root / "full-cycle-stage2-inis", "current-cycle")
        stage2_handoff = {
            "schema_version": 2,
            "cycle_id": "current-cycle",
            "stage": 2,
            "ea": "FixtureEA",
            "source": str(self.source.resolve()),
            "stage1_spec": str(self.spec.resolve()),
            "stage1_spec_sha256": sha256(self.spec),
            "stage1_event": str(self.root / "full-cycle-stage1-event.json"),
            "expected_event": str(self.stage2_event.resolve()),
            "expected_verification": str(self.root / "stage2-verification.json"),
            "approved_tester_config_sha256": preparer.config_sha256(
                preparer.stage2_config_from_spec(
                    json.loads(self.spec.read_text(encoding="utf-8")),
                    source_text=self.source.read_text(encoding="utf-8"))),
            "tester_ini_artifacts": tester_ini_artifacts,
            "canonical_minato": str(self.root / "minato.mqh"),
            "canonical_minato_sha256": "MINATO",
            "ex5": str(self.ex5.resolve()),
            "score_version": "MINATO_BASE_SCORE_V2.0",
        }
        minato_path = Path(stage2_handoff["canonical_minato"])
        minato_path.write_text("// canonical fixture include\n", encoding="utf-8")
        stage2_handoff["canonical_minato_sha256"] = sha256(minato_path)
        stage1_event_path = Path(stage2_handoff["stage1_event"])
        stage1_event_path.write_text(json.dumps({
            "cycle_id": "current-cycle", "stage": 1, "status": "complete",
            "artifact_sha256": sha256(self.spec),
        }), encoding="utf-8")
        stage2_event = json.loads(self.stage2_event.read_text(encoding="utf-8"))
        stage2_event.update({
            "cycle_id": "current-cycle", "stage": 2, "status": "complete",
            "verification": stage2_handoff["expected_verification"],
            "verification_sha256": "VERIFICATION",
            "score_version": "MINATO_BASE_SCORE_V2.0",
            "tester_ini_artifacts": tester_ini_artifacts,
        })
        self.stage2_event.write_text(json.dumps(stage2_event), encoding="utf-8")
        stage2_verification_path = Path(stage2_handoff["expected_verification"])
        stage2_verification = {
            "schema_version": 2, "cycle_id": "current-cycle", "stage": 2,
            "ea": "FixtureEA", "passed": True, "score_version": "MINATO_BASE_SCORE_V2.0",
            "source": str(self.source.resolve()), "source_sha256": sha256(self.source),
            "ex5": str(self.ex5.resolve()), "ex5_sha256": sha256(self.ex5),
            "canonical_minato": str(minato_path.resolve()),
            "canonical_minato_sha256": sha256(minato_path),
            "stage1_spec": str(self.spec.resolve()), "stage1_spec_sha256": sha256(self.spec),
            "approved_tester_config_sha256": stage2_handoff["approved_tester_config_sha256"],
        }
        stage2_verification_path.write_text(json.dumps(stage2_verification), encoding="utf-8")
        stage2_event["verification_sha256"] = sha256(stage2_verification_path)
        stage2_event["stage1_spec_sha256"] = sha256(self.spec)
        stage2_event["approved_tester_config_sha256"] = stage2_handoff["approved_tester_config_sha256"]
        stage2_event["mq5_sha256"] = sha256(self.source)
        stage2_event["ex5_sha256"] = sha256(self.ex5)
        stage2_event["canonical_minato_sha256"] = sha256(minato_path)
        self.stage2_event.write_text(json.dumps(stage2_event), encoding="utf-8")
        stage2_handoff_path = self.root / "full-cycle-stage2-handoff.json"
        stage2_handoff_path.write_text(json.dumps(stage2_handoff), encoding="utf-8")
        with mock.patch.multiple(
            preparer,
            ROOT=self.root,
            STATE=state_path,
            STAGE2_HANDOFF=stage2_handoff_path,
        ):
            event, _ = preparer.validate_stage2(state, stage2_handoff)
        self.assertEqual("complete", event["status"])

    def test_gui_progress_can_attach_watcher_only_to_exact_bound_live_run(self):
        runtime = {
            "cycle_id": "cycle-1", "ea": "EA", "symbol": "ETHUSD",
            "timeframe": "M5", "ini_sha256": "INI-HASH",
            "start_button_invoked": True, "max_passes": 11000,
            "input_schema_binding": {
                "status": "INPUTS_BOUND", "timeframe": "M5",
                "ini_sha256": "INI-HASH",
            },
        }
        handoff = {
            "cycle_id": "cycle-1", "ea": "EA", "symbol": "ETHUSD",
            "timeframe_order": ["M5", "M15", "H1"],
            "ini_sha256": {"M5": "INI-HASH"},
        }
        counter = {
            "ok": True, "observer_route": "bounded_gui_helper_subprocess",
            "button_state": "STOP", "current": 72, "total": 10496,
        }
        self.assertTrue(launcher.authenticated_live_gui_progress(
            runtime, handoff, counter, [{"pid": 1001}]))
        self.assertFalse(launcher.authenticated_live_gui_progress(
            runtime, handoff, dict(counter, button_state="START"), [{"pid": 1001}]))
        self.assertFalse(launcher.authenticated_live_gui_progress(
            runtime, handoff, dict(counter, current=0), [{"pid": 1001}]))
        wrong_binding = dict(runtime, input_schema_binding={
            "status": "INPUTS_BOUND", "timeframe": "M5", "ini_sha256": "OTHER"})
        self.assertFalse(launcher.authenticated_live_gui_progress(
            wrong_binding, handoff, counter, [{"pid": 1001}]))
        self.assertFalse(launcher.authenticated_live_gui_progress(
            runtime, handoff, counter, []))

    def test_release_preflight_allows_one_final_repair_retry_without_stage3_handoff(self):
        with tempfile.TemporaryDirectory() as folder:
            state = {
                "schema_version": 2, "cycle_id": "current-cycle", "current_stage": "stage3_pending",
                "stage_status": "stage2_complete_held", "hold_after_stage": 2,
                "next_action": "audit_stage2_before_stage3",
                "run_authorization": {
                    "schema_version": 1, "scope": "current_cycle_stages_1_to_5",
                    "cycle_id": "current-cycle", "authorized_by": "user_confirmed",
                    "allowed_stages": [1, 2, 3, 4, 5], "hold_after_stage": 5,
                },
                "stage2_release_attempt": {
                    "cycle_id": "current-cycle", "status": "failed", "failure_phase": "execution",
                    "failure_detail": "one-run Stage 2 release authorization authorization_id mismatch",
                },
                "stage2_release_attempt_history": [],
            }
            handoff = Path(folder) / "Stage3_v2.json"
            handoff.write_text(json.dumps({"cycle_id": "previous-cycle"}), encoding="utf-8")
            with mock.patch.object(driver, "HANDOFF", handoff):
                self.assertTrue(driver.should_attempt_stage2_release(state))
                state["stage2_release_attempt_history"] = [
                    {"cycle_id": "current-cycle"}, {"cycle_id": "current-cycle"}
                ]
                self.assertTrue(driver.should_attempt_stage2_release(state))
                state["stage2_release_attempt_history"].append({"cycle_id": "current-cycle"})
                self.assertFalse(driver.should_attempt_stage2_release(state))

    def test_stale_lease_is_archived_only_after_recorded_processes_are_dead(self):
        lease = self.root / "stage3.lock.json"
        lease.write_text(json.dumps({"cycle_id": "old-cycle"}), encoding="utf-8")
        runtime = self.root / "old-cycle.stage3.runtime.json"
        runtime.write_text(json.dumps({"schema_version": 2, "stage": 3, "cycle_id": "old-cycle",
                                       "ea": "EA", "status": "TIMEFRAME_COMPLETE",
                                       "worker_pids": [99999999]}), encoding="utf-8")
        archive = preparer.reconcile_stale_lease(lease, self.cycle, self.root)
        self.assertFalse(lease.exists()); self.assertTrue(archive.is_file())

    def test_stale_lease_with_live_recorded_process_fails_closed(self):
        lease = self.root / "stage3.lock.json"
        lease.write_text(json.dumps({"cycle_id": "old-cycle"}), encoding="utf-8")
        runtime = self.root / "old-cycle.stage3.runtime.json"
        runtime.write_text(json.dumps({"schema_version": 2, "stage": 3, "cycle_id": "old-cycle",
                                       "ea": "EA", "status": "OPTIMIZING",
                                       "worker_pids": [os.getpid()]}), encoding="utf-8")
        process = mock.Mock()
        process.name.return_value = "metatester64.exe"
        process.exe.return_value = r"C:\MT5\metatester64.exe"
        with mock.patch.object(preparer.psutil, "Process", return_value=process):
            with self.assertRaisesRegex(ValueError, "authenticated MT5 processes"):
                preparer.reconcile_stale_lease(lease, self.cycle, self.root)
        self.assertTrue(lease.exists())

    def test_watcher_can_wake_zero_token_supervisor_immediately(self):
        commands = []
        def fake_runner(command, **_kwargs):
            commands.append(command)
            return subprocess.CompletedProcess(command, 0, stdout='{"status":"WAITING_STAGE3"}', stderr="")
        result = watcher.wake_supervisor(self.root / "supervisor.py", fake_runner)
        self.assertTrue(result["accepted"]); self.assertEqual("--show", commands[0][-1])

    def test_launch_builder_is_inert_and_registration_is_single_flight(self):
        terminal = self.root / "terminal64.exe"; terminal.write_bytes(b"binary")
        ini = self.root / "M5.ini"; ini.write_text("ini", encoding="utf-8")
        handoff = {"timeframe_order": ["M5"], "ini_files": {"M5": str(ini)}, "cycle_id": self.cycle, "ea": "EA",
                   "symbol": "ETHUSD", "from_date": "2025.01.10", "to_date": "2026.01.10",
                   "poll_seconds": 10, "normal_settle_seconds": 20, "fallback_settle_seconds": 60,
                   "runtime_manifest": str(self.root / "runtime.json")}
        with mock.patch.object(launcher, "STAGE3_PORTABLE_TERMINAL", terminal.resolve()):
            with self.assertRaisesRegex(ValueError, "prepared INI path does not encode"):
                launcher.build_command(handoff, "M5", terminal)
        handoff_path = self.root / "launch-handoff.json"; handoff_path.write_text(json.dumps(handoff), encoding="utf-8")
        log = self.root / "tester.log"; log.write_bytes(b"old")
        with mock.patch.object(launcher, "build_command", return_value=[str(terminal.resolve())]):
            runtime = launcher.register(handoff_path, "M5", terminal, self.root / "new.opt", log, [123],
                                        self.root / "runtime.json", terminal_pid=456)
            self.assertEqual("STARTING", runtime["status"]); self.assertEqual(3, runtime["tester_log_start_offset"])
            self.assertEqual(456, runtime["terminal_pid"])
            self.assertEqual((600, 900, 180, 900), (runtime["first_pass_budget_seconds"],
                                             runtime["first_pass_hard_limit_seconds"],
                                             runtime["progress_stall_budget_seconds"],
                                             runtime["timeframe_budget_seconds"]))
            with self.assertRaises(ValueError):
                launcher.register(handoff_path, "M5", terminal, self.root / "new.opt", log, [124], self.root / "runtime.json")

    def test_launch_builder_rejects_stale_profile_over_20_wide_intervals(self):
        terminal = self.root / "terminal64.exe"; terminal.write_bytes(b"binary")
        self.source.write_text("input double InpPeriod = 20;\n", encoding="utf-8")
        stage1 = json.loads(self.spec.read_text(encoding="utf-8"))
        stage1["tester_config"]["inputs"]["InpPeriod"]["type"] = "double"
        self.spec.write_text(json.dumps(stage1), encoding="utf-8")
        ini = self.root / f"{self.cycle}.EA.ETHUSD.M5.20250110_20260110.wide.ini"
        ini.write_text(
            "[Tester]\nEA_NAME=EA\nExpert=Kiyosaki\\EA.ex5\nSymbol=ETHUSD\n"
            "Period=M5\nOptimization=2\nFromDate=2025.01.10\nToDate=2026.01.10\n"
            "[TesterInputs]\nInpPeriod=20||11||0.9||29.9||Y\n", encoding="utf-8")
        handoff = {"timeframe_order": ["M5"], "ini_files": {"M5": str(ini)},
                   "ini_sha256": {"M5": sha256(ini)}, "cycle_id": self.cycle,
                   "ea": "EA", "symbol": "ETHUSD", "from_date": "2025.01.10",
                   "to_date": "2026.01.10", "grid_mode": "default",
                   "stage1_spec": str(self.spec), "stage1_spec_sha256": sha256(self.spec)}
        with mock.patch.object(launcher, "STAGE3_PORTABLE_TERMINAL", terminal.resolve()), \
             mock.patch.object(launcher, "validate_stage3_launch_lineage", return_value={}):
            with self.assertRaisesRegex(ValueError, r"InpPeriod has 21 MT5 steps \(22 values\); invalid for default"):
                launcher.build_command(handoff, "M5", terminal)

    def test_stop_prefers_authenticated_tester_stop_button(self):
        expected = {"stopped": True, "pid": 77, "method": "tester_stop_button"}
        with mock.patch.object(stage3_stop, "_click_named_stop_button", return_value=expected), \
             mock.patch.object(stage3_stop.subprocess, "run") as run:
            self.assertEqual(expected, stage3_stop.click_stop(77))
            run.assert_not_called()

    def make_opt(self, path, launch_epoch):
        header_size, record_size = 436, 232
        data = bytearray(header_size + record_size)
        data[428:432] = header_size.to_bytes(4, "little"); data[432:436] = record_size.to_bytes(4, "little")
        path.write_bytes(data); os.utime(path, (launch_epoch + 1, launch_epoch + 1))

    def test_gui_counter_uses_bounded_helper_process(self):
        completed = mock.Mock(returncode=0, stdout="{'ok': True, 'current': 7, 'total': 50}\n", stderr="")
        hermes_python = self.root / "hermes" / "hermes-agent" / "venv" / "Scripts" / "python.exe"
        hermes_python.parent.mkdir(parents=True)
        hermes_python.write_bytes(b"test interpreter marker")
        with mock.patch.dict(watcher.os.environ, {"LOCALAPPDATA": str(self.root)}):
            with mock.patch.object(watcher.subprocess, "run", return_value=completed) as run:
                result = watcher.read_gui_counter(456)
        self.assertTrue(result["ok"])
        self.assertEqual(7, result["current"])
        self.assertEqual("bounded_gui_helper_subprocess", result["observer_route"])
        self.assertEqual(30, run.call_args.kwargs["timeout"])
        self.assertEqual(str(hermes_python.resolve()), run.call_args.args[0][0])

    def test_gui_counter_timeout_is_nonfatal_and_bounded(self):
        with mock.patch.object(watcher.subprocess, "run", side_effect=subprocess.TimeoutExpired("ui", 30)):
            result = watcher.read_gui_counter(456)
        self.assertFalse(result["ok"])
        self.assertEqual("bounded_gui_helper_subprocess", result["observer_route"])
        self.assertIn("timed out", result["detail"].lower())

    def test_watcher_finishes_on_authoritative_log_without_waiting_for_workers(self):
        launch = 1_700_000_000.0
        opt, log = self.root / "run.opt", self.root / "tester.log"
        self.make_opt(opt, launch)
        log.write_text("genetic optimization finished on pass 10\n100 new records saved to cache file", encoding="utf-8")
        os.utime(log, (launch + 1, launch + 1))
        runtime = {"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "EA", "timeframe": "M5", "status": "OPTIMIZING",
                   "opt_path": str(opt), "tester_log": str(log), "tester_log_start_offset": 0, "worker_pids": [123],
                   "terminal_pid": 456,
                   "launched_utc": datetime.fromtimestamp(launch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "last_opt_size": opt.stat().st_size, "last_opt_growth_epoch": launch + 1, "normal_settle_seconds": 20,
                   "fallback_settle_seconds": 60, "timeout_seconds": 1000, "timeframe_event": str(self.root / "tf-event.json"),
                   "opt_size_before": None, "opt_mtime_before": None}
        with mock.patch.object(watcher, "read_gui_counter",
                               return_value={"ok": True, "current": 10, "total": 10, "button_state": "START"}):
            result = watcher.evaluate_snapshot(runtime, launch + 22, alive_pids=[123])
        self.assertEqual("TIMEFRAME_COMPLETE", result["status"])
        self.assertEqual("authoritative_log_opt_settled", result["completion_reason"])

    def test_watcher_finishes_on_full_stopped_ui_counter_without_genetic_finish_line(self):
        launch = 1_700_000_000.0
        opt, log = self.root / "full-ui.opt", self.root / "full-ui-tester.log"
        self.make_opt(opt, launch)
        runtime = {"schema_version": 2, "cycle_id": self.cycle, "stage": 3,
                   "ea": "EA", "timeframe": "M5", "status": "OPTIMIZING",
                   "opt_path": str(opt), "tester_log": str(log),
                   "tester_log_start_offset": 0, "worker_pids": [123], "terminal_pid": 456,
                   "launched_utc": datetime.fromtimestamp(launch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "last_opt_size": opt.stat().st_size, "last_opt_growth_epoch": launch + 1,
                   "normal_settle_seconds": 20, "timeout_seconds": 1000,
                   "timeframe_event": str(self.root / "full-ui-event.json"),
                   "opt_size_before": None, "opt_mtime_before": None}
        with mock.patch.object(watcher, "read_gui_counter", return_value={
                "ok": True, "current": 100, "total": 100, "button_state": "START"}):
            result = watcher.evaluate_snapshot(runtime, launch + 22, alive_pids=[123], active_pids=[])
        self.assertEqual("TIMEFRAME_COMPLETE", result["status"])
        self.assertEqual("full_tester_pass_counter_opt_settled", result["completion_reason"])
        self.assertEqual("tester_pass_counter", result["completion_basis"])

    def test_partial_label_reconciles_only_with_full_stopped_ui_counter_and_bound_artifacts(self):
        launch = 1_700_000_000.0
        opt, log = self.root / "reconcile.opt", self.root / "reconcile-tester.log"
        runtime_path = self.root / "reconcile-runtime.json"
        event_path = self.root / "reconcile-event.json"
        self.make_opt(opt, launch)
        log_bytes = b"clean tester segment without genetic finish/cache markers"
        log.write_bytes(log_bytes)
        log_hash = __import__("hashlib").sha256(log_bytes).hexdigest().upper()
        opt_hash = sha256(opt)
        event = {
            "schema_version": 2, "stage": 3, "cycle_id": self.cycle,
            "timeframe": "M5", "status": "TIMEFRAME_COMPLETE",
            "completion_mode": "partial_90_percent", "completion_ratio": 1.0,
            "ui_passes_complete": True,
            "ui_pass_counter": {"ok": True, "current": 100, "total": 100, "button_state": "START"},
            "tester_log": str(log), "tester_log_start_offset": 0,
            "tester_log_end_offset": len(log_bytes), "tester_log_sha256": log_hash,
            "tester_log_completion_evidence": {"clean": True, "error_lines": [],
                "byte_count": len(log_bytes), "sha256": log_hash},
            "opt_path": str(opt), "opt_sha256": opt_hash,
        }
        event_path.write_text(json.dumps(event), encoding="utf-8")
        runtime = {"status": "PARTIAL_TIMEFRAME_COMPLETE", "cycle_id": self.cycle,
                   "ea": "EA", "timeframe": "M5", "timeframe_event": str(event_path),
                   "opt_path": str(opt), "opt_sha256": opt_hash,
                   "launched_epoch": launch, "opt_structurally_complete": True,
                   "stable_seconds": 30, "normal_settle_seconds": 20}
        runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
        with mock.patch.object(driver, "STATE", self.root / "kcos_cycle_state_v2.json"):
            receipt = driver.reconcile_full_ui_counter_partial(
                runtime, runtime_path, {"cycle_id": self.cycle, "ea": "EA"})
        self.assertIsNotNone(receipt)
        updated = json.loads(runtime_path.read_text(encoding="utf-8"))
        self.assertEqual("TIMEFRAME_COMPLETE", updated["status"])
        self.assertEqual("full", updated["completion_mode"])
        self.assertTrue(updated["full_ui_counter_reconciliation"]["receipt_sha256"])

    def test_progress_supersedes_only_matching_partial_hold_with_verified_full_counter_receipt(self):
        opt = self.root / "progress-reconcile.opt"
        self.make_opt(opt, 1_700_000_000.0)
        event_path = self.root / "progress-reconcile-event.json"
        event = {"schema_version": 2, "stage": 3, "cycle_id": self.cycle,
                 "timeframe": "M5", "status": "TIMEFRAME_COMPLETE",
                 "completion_mode": "partial_90_percent", "completion_ratio": 1.0,
                 "ui_passes_complete": True,
                 "ui_pass_counter": {"ok": True, "current": 100, "total": 100,
                                     "button_state": "START"},
                 "opt_path": str(opt), "opt_sha256": sha256(opt)}
        event_path.write_text(json.dumps(event), encoding="utf-8")
        receipt_path = self.root / "diagnostics" / f"{self.cycle}.stage3.M5.full_ui_counter_reconciliation.json"
        receipt_path.parent.mkdir(parents=True)
        receipt_path.write_text(json.dumps({
            "status": "FULL_UI_COUNTER_RECONCILED", "cycle_id": self.cycle,
            "ea": "EA", "timeframe": "M5", "event": str(event_path),
            "event_sha256": sha256(event_path), "opt_path": str(opt), "opt_sha256": sha256(opt),
        }), encoding="utf-8")
        failure_detail = ("Stage 3 M5 produced a partial result; the timeframe must be fully "
                          "completed and parsed before advancing; full-counter reconciliation was rejected")
        failure_path = self.root / "events" / f"{self.cycle}.stage3.failed.json"
        failure_path.parent.mkdir(parents=True)
        failure_path.write_text(json.dumps({
            "event_id": f"{self.cycle}:stage3:failed", "cycle_id": self.cycle,
            "stage": 3, "status": "failed", "rotation_eligible": False,
            "error": failure_detail,
        }), encoding="utf-8")
        state_path = self.root / "kcos_cycle_state_v2.json"
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "current_stage": "stage3_running",
            "current_ea": "EA", "stage_status": "stage3_infrastructure_failure",
            "stage3_failure_class": "infrastructure", "stage3_failure_detail": failure_detail,
            "stage3_failure_event": str(failure_path),
        }), encoding="utf-8")
        updated = driver.update_progress(state_path, self.cycle, "EA",
                                         "stage3_m15_running", "watch_stage3_m15")
        self.assertEqual("stage3_m15_running", updated["stage_status"])
        self.assertNotIn("stage3_failure_event", updated)
        self.assertNotIn("stage3_failure_detail", updated)
        self.assertEqual(str(failure_path), updated["stage3_superseded_failures"][-1]["failure_event"])
        self.assertTrue(failure_path.is_file())

    def test_watcher_finishes_on_authoritative_log_when_gui_counter_unavailable(self):
        launch = 1_700_000_000.0
        opt, log = self.root / "no-ui.opt", self.root / "no-ui-tester.log"
        self.make_opt(opt, launch)
        log.write_text("genetic calculation is over\ngenetic optimization finished on pass 10\n100 new records saved to cache file", encoding="utf-8")
        os.utime(log, (launch + 1, launch + 1))
        runtime = {"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "EA", "timeframe": "M5", "status": "OPTIMIZING",
                   "opt_path": str(opt), "tester_log": str(log), "tester_log_start_offset": 0, "worker_pids": [123],
                   "terminal_pid": 456,
                   "launched_utc": datetime.fromtimestamp(launch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "last_opt_size": opt.stat().st_size, "last_opt_growth_epoch": launch + 1, "normal_settle_seconds": 20,
                   "timeout_seconds": 1000, "timeframe_event": str(self.root / "no-ui-tf-event.json"),
                   "opt_size_before": None, "opt_mtime_before": None}
        with mock.patch.object(watcher, "read_gui_counter", return_value={"ok": False, "detail": "UI unavailable"}):
            result = watcher.evaluate_snapshot(runtime, launch + 22, alive_pids=[123], active_pids=[])
        self.assertEqual("TIMEFRAME_COMPLETE", result["status"])
        self.assertEqual("authoritative_genetic_journal_opt_settled", result["completion_reason"])

    def test_pass_limit_accepts_clean_run_bound_genetic_finish_without_ui_ceiling(self):
        digest = "A" * 64
        final = {
            "completion_basis": "fresh_genetic_journal",
            "fresh_log_completion": True,
            "fresh_log_sha256": digest,
            "tester_log_start_offset": 172530,
            "fresh_log_end_offset": 172730,
            "ui_counter_valid": True,
            "ui_passes_complete": False,
            "ui_completed_passes": 11233,
            "fresh_log_evidence": {
                "early": True, "final": True, "cache_saved": True, "clean": True,
                "error_lines": [], "byte_count": 200, "sha256": digest,
            },
        }
        self.assertTrue(watcher.pass_limit_completion_proven(final, 12544))

        final["fresh_log_evidence"]["error_lines"] = ["Tester aborted"]
        self.assertFalse(watcher.pass_limit_completion_proven(final, 12544))
        final["fresh_log_evidence"]["error_lines"] = []
        final["fresh_log_end_offset"] += 1
        self.assertFalse(watcher.pass_limit_completion_proven(final, 12544))
        final["fresh_log_end_offset"] -= 1
        final["fresh_log_evidence"]["sha256"] = "B" * 64
        self.assertFalse(watcher.pass_limit_completion_proven(final, 12544))

    def test_early_genetic_marker_is_not_authoritative_completion(self):
        launch = 1_700_000_000.0
        opt, log = self.root / "early.opt", self.root / "early.log"
        self.make_opt(opt, launch); log.write_text("genetic calculation is over", encoding="utf-8")
        os.utime(log, (launch + 1, launch + 1))
        runtime = {"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "status": "OPTIMIZING",
                   "opt_path": str(opt), "tester_log": str(log), "tester_log_start_offset": 0,
                   "worker_pids": [123], "launched_utc": datetime.fromtimestamp(launch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "last_opt_size": opt.stat().st_size, "last_opt_growth_epoch": launch + 1,
                   "normal_settle_seconds": 20, "fallback_settle_seconds": 60, "timeout_seconds": 1000,
                   "opt_size_before": None, "opt_mtime_before": None}
        result = watcher.evaluate_snapshot(runtime, launch + 70, alive_pids=[123])
        self.assertEqual("OPTIMIZING", result["status"])
        self.assertTrue(result["fresh_log_evidence"]["early"])
        self.assertFalse(result["fresh_log_completion"])

    def test_watcher_never_completes_quiet_file_while_worker_alive(self):
        launch = 1_700_000_000.0
        opt, log = self.root / "run.opt", self.root / "missing.log"; self.make_opt(opt, launch)
        runtime = {"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "status": "OPTIMIZING", "opt_path": str(opt),
                   "tester_log": str(log), "tester_log_start_offset": 0, "worker_pids": [123],
                   "launched_utc": datetime.fromtimestamp(launch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "last_opt_size": opt.stat().st_size, "last_opt_growth_epoch": launch + 1, "normal_settle_seconds": 20,
                   "fallback_settle_seconds": 60, "timeout_seconds": 1000, "opt_size_before": None, "opt_mtime_before": None}
        self.assertEqual("OPTIMIZING", watcher.evaluate_snapshot(runtime, launch + 100, alive_pids=[123])["status"])
        self.assertEqual("OPTIMIZING", watcher.evaluate_snapshot(runtime, launch + 100, alive_pids=[])["status"])

    def test_timeout_is_failure_not_completion(self):
        launch = 1_700_000_000.0
        runtime = {"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "status": "OPTIMIZING", "opt_path": str(self.root / "none.opt"),
                   "tester_log": str(self.root / "none.log"), "worker_pids": [123], "launched_utc": datetime.fromtimestamp(launch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "timeout_seconds": 10}
        self.assertEqual("COMPUTE_BUDGET_EXCEEDED", watcher.evaluate_snapshot(runtime, launch + 11, alive_pids=[123])["status"])

    def test_agent_log_registration_records_paths_and_offsets(self):
        data_dir = self.root / "MetaQuotes" / "Terminal" / "terminal-id"
        log_name = datetime.fromtimestamp(1_700_000_000.0).strftime("%Y%m%d.log")
        log = self.root / "MetaQuotes" / "Tester" / "terminal-id" / "Agent-127.0.0.1-3014" / "logs" / log_name
        log.parent.mkdir(parents=True); log.write_bytes(b"existing")
        with mock.patch.object(launcher, "worker_address", return_value="127.0.0.1:3014"):
            found = launcher.agent_log_registrations([123], data_dir, 1_700_000_000.0,
                                                     {"127.0.0.1:3014": 4})
        self.assertEqual([{"worker_pid": 123, "address": "127.0.0.1:3014",
                           "path": str(log.resolve()), "start_offset": 4}], found)

    def test_fresh_local_worker_discovery_uses_launch_registry_not_fixed_port_ceiling(self):
        def fake_process(created, address):
            return mock.Mock(
                create_time=mock.Mock(return_value=created),
                cmdline=mock.Mock(return_value=["metatester64.exe", "/local", f"/address:{address}"]),
                exe=mock.Mock(return_value=r"C:\portable\metatester64.exe"),
            )

        processes = {
            101: fake_process(1001, "127.0.0.1:3025"),
            102: fake_process(1001, "127.0.0.1:3024"),
        }
        with (mock.patch.object(launcher, "process_ids", return_value=[101, 102, 103]),
              mock.patch.object(launcher.psutil, "Process", side_effect=lambda pid: processes[pid])):
            workers = launcher.fresh_local_workers(
                [103], 1000, expected_addresses=["127.0.0.1:3025"])
        self.assertEqual([101], [worker["pid"] for worker in workers])
        self.assertEqual("127.0.0.1:3025", workers[0]["address"])

    def _write_stale_runtime_retry_fixture(self):
        ini = self.root / "M15.ini"
        ini.write_text("[TesterInputs]\nInpPeriod=20||10||1||30||Y\n", encoding="utf-8")
        opt = self.root / "M15.opt"
        opt.write_bytes(b"stale partial optimization evidence")
        os.utime(opt, (time.time() - 900, time.time() - 900))
        runtime_path = self.root / "M15.runtime.json"
        failure_path = self.root / "stage3.failed.json"
        failure_path.write_text(json.dumps({
            "cycle_id": self.cycle, "stage": 3, "status": "failed",
            "rotation_eligible": False,
        }), encoding="utf-8")
        handoff = {
            "cycle_id": self.cycle, "ea": "EA", "timeframe_order": ["M5", "M15", "H1"],
            "runtime_manifest": str(runtime_path.resolve()),
            "ini_files": {"M15": str(ini.resolve())},
            "ini_sha256": {"M15": sha256(ini)},
        }
        runtime = {
            "cycle_id": self.cycle, "ea": "EA", "symbol": "ETHUSD", "timeframe": "M15",
            "status": "OPTIMIZING", "terminal_pid": 47520, "worker_pids": [],
            "worker_process_evidence": [], "watcher_pid": None,
            "launched_epoch": time.time() - 900, "startup_grace_seconds": 300,
            "worker_baseline_pids": [], "agent_offsets_before": {"127.0.0.1:3025": 0},
            "command": [str(launcher.STAGE3_PORTABLE_TERMINAL), "/portable"],
            "ini": str(ini.resolve()), "ini_sha256": sha256(ini), "opt_path": str(opt.resolve()),
            "timeframe_event": str((self.root / "M15.complete.json").resolve()),
        }
        runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
        handoff_path = self.root / "current-handoff.json"
        handoff_path.write_text(json.dumps(handoff), encoding="utf-8")
        state_path = self.root / "current-state.json"
        state_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "current_ea": "EA",
            "current_stage": "stage3_running", "stage_status": "stage3_infrastructure_failure",
            "next_action": "inspect_stage3_infrastructure_failure",
            "stage3_failure_detail": "Stage 3 M15 runtime registration stayed incomplete beyond its 300-second startup grace",
            "stage3_failure_event": str(failure_path.resolve()),
            "stage3_auto_timeframe_retry_history": [],
        }), encoding="utf-8")
        return state_path, handoff_path, runtime_path, runtime, opt

    def test_stale_runtime_restart_requires_dead_processes_and_preserves_same_timeframe(self):
        state_path, handoff_path, runtime_path, runtime, opt = self._write_stale_runtime_retry_fixture()
        state = json.loads(state_path.read_text(encoding="utf-8"))
        handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
        with (mock.patch.object(launcher, "opt_candidates", return_value=[opt]),
              mock.patch.object(launcher, "fresh_local_workers", return_value=[]),
              mock.patch.object(driver, "_matching_stage3_watcher", return_value=False),
              mock.patch.object(driver.psutil, "pid_exists", return_value=False)):
            self.assertTrue(driver.stale_runtime_restart_is_ready(state, handoff, runtime, []))
            state["stage3_auto_timeframe_retry_history"] = [{
                "cycle_id": self.cycle, "timeframe": "M15", "status": "started"}]
            self.assertFalse(driver.stale_runtime_restart_is_ready(state, handoff, runtime, []))

    def test_unstarted_terminal_retry_is_one_shot_and_requires_no_run_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ini = root / "H1.ini"
            ini.write_text("[TesterInputs]\nInpPeriod=20||10||1||30||Y\n", encoding="utf-8")
            runtime_path, handoff_path = root / "runtime.json", root / "handoff.json"
            failure_path = root / "failed.json"
            failure_path.write_text(json.dumps({
                "cycle_id": self.cycle, "stage": 3, "status": "failed",
                "rotation_eligible": False,
            }), encoding="utf-8")
            handoff = {
                "cycle_id": self.cycle, "ea": "EA", "timeframe_order": ["M5", "M15", "H1"],
                "runtime_manifest": str(runtime_path), "ini_files": {"H1": str(ini)},
                "ini_sha256": {"H1": sha256(ini)},
            }
            runtime = {
                "cycle_id": self.cycle, "ea": "EA", "timeframe": "H1",
                "status": "INFRASTRUCTURE_FAILURE", "failure_kind": "EIGHTCAP_TERMINAL_NOT_FRESH",
                "infrastructure_only": True, "terminal_pid": 87531, "worker_pids": [],
                "worker_process_evidence": [], "ini": str(ini.resolve()),
                "ini_sha256": sha256(ini), "launched_epoch": time.time() - 900,
                "startup_grace_seconds": 300, "journal_diagnostic": {"fresh_lines": [], "error_lines": []},
                "opt_candidates_before": {},
            }
            runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
            handoff_path.write_text(json.dumps(handoff), encoding="utf-8")
            state = {
                "cycle_id": self.cycle, "current_ea": "EA", "current_stage": "stage3_running",
                "stage_status": "stage3_infrastructure_failure",
                "next_action": "inspect_stage3_infrastructure_failure",
                "stage3_failure_event": str(failure_path), "stage3_auto_timeframe_retry_history": [],
            }
            with (mock.patch.object(launcher, "process_ids", return_value=[]),
                  mock.patch.object(driver.psutil, "pid_exists", return_value=False),
                  mock.patch.object(launcher, "opt_candidates", return_value=[])):
                self.assertTrue(driver.unstarted_terminal_retry_is_ready(
                    state, handoff, runtime, []))
                state["stage3_auto_timeframe_retry_history"] = [{
                    "cycle_id": self.cycle, "timeframe": "H1", "attempt": 1}]
                self.assertFalse(driver.unstarted_terminal_retry_is_ready(
                    state, handoff, runtime, []))

    def test_unbound_new_expert_allows_one_authenticated_parameter_profile_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ini = root / "M5.ini"
            ini.write_text("[TesterInputs]\nInpPeriod=20||10||1||30||Y\n", encoding="utf-8")
            runtime_path, handoff_path = root / "runtime.json", root / "handoff.json"
            failure_path = root / "failed.json"
            failure_path.write_text(json.dumps({
                "cycle_id": self.cycle, "stage": 3, "status": "failed",
                "rotation_eligible": False,
            }), encoding="utf-8")
            handoff = {
                "cycle_id": self.cycle, "ea": "EA", "timeframe_order": ["M5", "M15", "H1"],
                "runtime_manifest": str(runtime_path), "ini_files": {"M5": str(ini)},
                "ini_sha256": {"M5": sha256(ini)},
            }
            runtime = {
                "cycle_id": self.cycle, "ea": "EA", "timeframe": "M5",
                "status": "INFRASTRUCTURE_FAILURE",
                "failure_kind": "EXPERT_INPUT_BINDING_UNAVAILABLE",
                "infrastructure_only": True, "terminal_pid": 0, "worker_pids": [],
                "worker_process_evidence": [], "watcher_pid": None,
                "ini": str(ini.resolve()), "ini_sha256": sha256(ini),
                "launched_epoch": time.time() - 900, "startup_grace_seconds": 300,
                "start_button_attempt_count": 8, "start_button_delivery_count": 0,
                "start_button_invoked": False,
                "input_schema_binding": {
                    "status": "EXPERT_NOT_READY", "exact_matches": 0,
                    "navigator_tested": False,
                },
                "journal_diagnostic": {"fresh_lines": [], "error_lines": []},
                "terminal_log_progress": {"authenticated": False},
                "opt_candidates_before": {},
            }
            runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
            handoff_path.write_text(json.dumps(handoff), encoding="utf-8")
            detail = (
                "Stage 3 M5 Start handoff produced no authenticated tester, "
                "agent-pass, or growing-OPT progress within its startup grace")
            state = {
                "cycle_id": self.cycle, "current_ea": "EA", "current_stage": "stage3_running",
                "stage_status": "stage3_infrastructure_failure",
                "next_action": "inspect_stage3_infrastructure_failure",
                "stage3_failure_detail": detail, "stage3_failure_event": str(failure_path),
                "stage3_auto_timeframe_retry_history": [{
                    "cycle_id": self.cycle, "timeframe": "M5", "attempt": 1,
                    "reason": "terminal_lost_before_authenticated_run_evidence",
                    "status": "started", "result_status": "START_REQUIRED",
                    "ini_sha256": sha256(ini),
                    "runtime_input_schema_binding": {
                        "status": "EXPERT_NOT_READY", "navigator_tested": False,
                    },
                }],
            }
            with (mock.patch.object(launcher, "process_ids", return_value=[]),
                  mock.patch.object(driver.psutil, "pid_exists", return_value=False),
                  mock.patch.object(launcher, "opt_candidates", return_value=[]),
                  mock.patch.object(launcher, "fresh_local_workers", return_value=[]),
                  mock.patch.object(driver, "_matching_stage3_watcher", return_value=False)):
                self.assertTrue(driver.unstarted_terminal_retry_is_ready(
                    state, handoff, runtime, []))
                state["stage3_auto_timeframe_retry_history"][0]["recovery_profile"] = "already-used.ini"
                self.assertFalse(driver.unstarted_terminal_retry_is_ready(
                    state, handoff, runtime, []))
                state["stage3_auto_timeframe_retry_history"] = []
                runtime["journal_diagnostic"]["fresh_lines"] = ["optimization started"]
                self.assertFalse(driver.unstarted_terminal_retry_is_ready(
                    state, handoff, runtime, []))

    def test_stale_runtime_restart_dispatches_same_timeframe_once(self):
        state_path, handoff_path, runtime_path, runtime, opt = self._write_stale_runtime_retry_fixture()
        observed_state = {}
        def launch_same_timeframe(path, timeframe):
            self.assertEqual("M15", timeframe)
            observed_state.update(json.loads(state_path.read_text(encoding="utf-8")))
            fresh = dict(runtime, status="OPTIMIZING", terminal_pid=9876,
                         launched_epoch=time.time(), worker_pids=[777])
            runtime_path.write_text(json.dumps(fresh), encoding="utf-8")
            return fresh
        with (mock.patch.object(launcher, "opt_candidates", return_value=[opt]),
              mock.patch.object(launcher, "fresh_local_workers", return_value=[]),
              mock.patch.object(driver, "_matching_stage3_watcher", return_value=False),
              mock.patch.object(driver.psutil, "pid_exists", return_value=False)):
            result = driver.restart_stale_timeframe(state_path, handoff_path, launch_same_timeframe)
        self.assertEqual("STAGE3_TIMEFRAME_RETRY_STARTED", result["status"])
        self.assertEqual("stage3_m15_running", observed_state["stage_status"])
        self.assertEqual("watch_stage3_m15", observed_state["next_action"])
        self.assertEqual("launching", observed_state["stage3_retry_launch_in_progress"]["status"])
        updated = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual("stage3_m15_running", updated["stage_status"])
        self.assertEqual("started", updated["stage3_auto_timeframe_retry_history"][0]["status"])
        self.assertEqual("started", updated["stage3_retry_launch_in_progress"]["status"])

    def test_stage3_cron_selects_bounded_stale_restart_only_without_terminal(self):
        driver_module = mock.Mock()
        driver_module.stage3_repair_retry_is_ready.return_value = False
        driver_module.unstarted_terminal_retry_is_ready.return_value = False
        driver_module.stale_runtime_restart_is_ready.return_value = True
        state = {"current_stage": "stage3_running", "cycle_id": self.cycle}
        runtime = {"status": "OPTIMIZING", "terminal_pid": 47520}
        handoff = {"cycle_id": self.cycle}
        self.assertEqual("RESTART_STALE", stage3_cron.choose_action(
            state, runtime, [], driver_module, handoff))
        self.assertEqual("ALREADY_RUNNING_EXTERNAL", stage3_cron.choose_action(
            state, runtime, [888], driver_module, handoff))

    def test_ten_minute_ui_eta_cutoff_is_generic_and_strictly_over_four_hours(self):
        timing = duration_policy.parse_mt5_elapsed_estimate(
            "Tester | 821 of 10496 - 00:30:18 / 06:27:22 Stop"
        )
        self.assertEqual(1818, timing["elapsed_seconds"])
        self.assertEqual(23242, timing["estimated_total_seconds"])
        checkpoint = duration_policy.UI_ETA_CHECKPOINT_SECONDS
        self.assertEqual(600, checkpoint)
        self.assertFalse(duration_policy.eta_cutoff_triggered(checkpoint - 1, 6 * 3600))
        self.assertFalse(duration_policy.eta_cutoff_triggered(checkpoint, 4 * 3600))
        self.assertTrue(duration_policy.eta_cutoff_triggered(checkpoint, 4 * 3600 + 1))

    def test_retry_opt_snapshot_uses_content_bound_name_and_preserves_prior_attempt(self):
        old = self.root / f"{self.cycle}.stage3.M15.opt"
        old.write_bytes(b"prior timeframe attempt")
        source = self.root / "current-M15.opt"
        source.write_bytes(b"fresh timeframe attempt")
        runtime_path = self.root / "runtime.json"
        runtime = {"cycle_id": self.cycle, "timeframe": "M15"}
        snapshot = watcher.immutable_opt_snapshot_path(runtime_path, runtime, source)
        self.assertNotEqual(old, snapshot)
        self.assertIn(sha256(source)[:12], snapshot.name)
        self.assertEqual(b"prior timeframe attempt", old.read_bytes())

    def test_four_hour_ui_eta_stops_after_ten_minute_checkpoint_and_emits_evidence(self):
        runtime_path = self.root / "eta-runtime.json"
        heartbeat = self.root / "eta-heartbeat.json"
        event = self.root / "eta-event.json"
        runtime_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "stage": 3,
            "ea": "AnyEA", "timeframe": "M5", "status": "OPTIMIZING",
            "terminal_pid": 456, "grid_mode": "default",
            "timeframe_event": str(event),
        }), encoding="utf-8")
        observed = {
            "status": "OPTIMIZING", "elapsed_seconds": 601,
            "completed_passes": 200, "ui_total_passes": 500,
            "seconds_since_last_pass": 0, "compute_qualification": "qualified",
            "first_pass_seconds": 1, "first_pass_censored_seconds": None,
            "first_pass_budget_seconds": 120, "first_pass_hard_limit_seconds": 200,
            "progress_stall_budget_seconds": 180, "timeframe_budget_seconds": 32400,
            "active_worker_pids": [123], "agent_log_evidence": [],
            "ui_button_state": "STOP",
            "tester_ui_timing": {
                "source": "authenticated_bound_tester_gui_bottom_right",
                "elapsed_seconds": 601, "estimated_total_seconds": 14401,
                "elapsed_text": "00:10:01", "estimated_total_text": "04:00:01",
            },
        }
        with mock.patch.object(watcher, "evaluate_snapshot", return_value=observed), \
             mock.patch.object(watcher, "_click_stop", return_value={"stopped": True, "method": "tester_stop_button"}):
            result = watcher.run_once(runtime_path, heartbeat, 1_700_000_241, [123], [123])
        self.assertEqual("COMPUTE_BUDGET_EXCEEDED", result["status"])
        self.assertEqual("skip_eta_over_4h", result["duration_policy_action"])
        self.assertTrue(result["duration_projection"]["over_budget"])
        event_value = json.loads(event.read_text(encoding="utf-8"))
        self.assertEqual("eta_over_4h_after_10m", event_value["completion_reason"])
        self.assertEqual("authenticated_bound_tester_gui_bottom_right",
                         event_value["duration_projection"]["source"])
        self.assertTrue(event_value["duration_stop"]["stopped"])

    def test_eta_cutoff_uses_bottom_right_elapsed_clock_not_runtime_elapsed(self):
        runtime_path = self.root / "eta-clock-runtime.json"
        heartbeat = self.root / "eta-clock-heartbeat.json"
        event = self.root / "eta-clock-event.json"
        runtime_path.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "stage": 3,
            "ea": "AnyEA", "timeframe": "M5", "status": "OPTIMIZING",
            "terminal_pid": 456, "grid_mode": "default",
            "timeframe_event": str(event),
        }), encoding="utf-8")
        observed = {
            "status": "OPTIMIZING", "elapsed_seconds": 301,
            "completed_passes": 20, "ui_total_passes": 500,
            "seconds_since_last_pass": 0, "compute_qualification": "qualified",
            "active_worker_pids": [123], "agent_log_evidence": [],
            "ui_button_state": "STOP",
            "tester_ui_timing": {
                "source": "authenticated_bound_tester_gui_bottom_right",
                "elapsed_seconds": 239, "estimated_total_seconds": 6 * 3600,
                "elapsed_text": "00:03:59", "estimated_total_text": "06:00:00",
            },
        }
        with mock.patch.object(watcher, "evaluate_snapshot", return_value=observed), \
             mock.patch.object(watcher, "_click_stop") as stop:
            result = watcher.run_once(runtime_path, heartbeat, 1_700_000_301, [123], [123])
        self.assertEqual("OPTIMIZING", result["status"])
        self.assertIsNone(result.get("duration_policy_action"))
        stop.assert_not_called()

    def test_compute_budgets_count_new_passes_and_emit_immutable_event(self):
        launch = 1_700_000_000.0
        agent = self.root / "agent.log"; prefix = "old OnTester result 1 : passed in 1s\n"
        agent.write_text(prefix, encoding="utf-8")
        runtime_path, heartbeat, event = self.root / "budget.json", self.root / "heartbeat.json", self.root / "event.json"
        runtime = {"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "EA", "timeframe": "M5",
                   "status": "OPTIMIZING", "opt_path": str(self.root / "none.opt"), "tester_log": str(self.root / "none.log"),
                   "worker_pids": [123], "launched_utc": datetime.fromtimestamp(launch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "agent_logs": [{"worker_pid": 123, "path": str(agent), "start_offset": len(prefix.encode())}],
                   "first_pass_budget_seconds": 120, "first_pass_hard_limit_seconds": 200,
                   "progress_stall_budget_seconds": 180,
                   "timeframe_budget_seconds": 900, "startup_grace_seconds": 200,
                   "timeframe_event": str(event)}
        runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
        result = watcher.run_once(runtime_path, heartbeat, launch + 121, [123], [123])
        self.assertEqual("OPTIMIZING", result["status"])
        result = watcher.run_once(runtime_path, heartbeat, launch + 200, [123], [123])
        self.assertEqual("COMPUTE_BUDGET_EXCEEDED", result["status"])
        self.assertEqual("compute_budget_exceeded", result["failure_mode"])
        self.assertEqual(0, result["completed_passes"])
        event_value = json.loads(event.read_text())
        self.assertEqual(f"{self.cycle}:stage3:M5:compute_budget_exceeded", event_value["event_id"])
        self.assertEqual(0, event_value["metrics"]["completed_passes"])
        self.assertEqual("censored_no_first_pass", event_value["metrics"]["compute_qualification"])
        self.assertEqual(200, event_value["metrics"]["first_pass_censored_seconds"])
        original = event.read_bytes()
        watcher.run_once(runtime_path, heartbeat, launch + 130, [123], [123])
        self.assertEqual(original, event.read_bytes())

        # Only bytes after the registered offset count; progress resets the rolling budget.
        event.unlink(); runtime["timeframe_event"] = str(event)
        agent.write_text(prefix + "new OnTester result 2 : passed in 0:00:01\n", encoding="utf-8")
        progressed = watcher.evaluate_snapshot(runtime, launch + 60, [123], [123])
        self.assertEqual(1, progressed["completed_passes"])
        stalled = watcher.evaluate_snapshot(progressed, launch + 241, [123], [123])
        self.assertEqual(("COMPUTE_BUDGET_EXCEEDED", "compute_stalled"),
                         (stalled["status"], stalled["failure_mode"]))

    def test_watcher_cli_treats_compute_skip_as_successful_evidence(self):
        result = {
            "status": "COMPUTE_BUDGET_EXCEEDED",
            "completion_reason": "compute_budget_exceeded",
        }
        argv = ["kcos_stage3_watch_v2.py", "--runtime", str(self.root / "runtime.json"),
                "--heartbeat", str(self.root / "heartbeat.json")]
        with mock.patch("sys.argv", argv), \
                mock.patch.object(watcher, "wait_until_optimizing", return_value={"status": "OPTIMIZING"}), \
                mock.patch.object(watcher, "run_once", return_value=result), \
                mock.patch.object(watcher, "wake_supervisor", return_value={"accepted": True}):
            self.assertEqual(0, watcher.main())

    def test_compute_budget_requires_active_workers_and_supports_legacy_runtime(self):
        launch = 1_700_000_000.0
        runtime = {"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "status": "OPTIMIZING",
                   "opt_path": str(self.root / "none.opt"), "tester_log": str(self.root / "none.log"),
                   "worker_pids": [123], "launched_utc": datetime.fromtimestamp(launch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
        self.assertEqual("OPTIMIZING", watcher.evaluate_snapshot(runtime, launch + 121, [123], [123])["status"])
        self.assertEqual("OPTIMIZING", watcher.evaluate_snapshot(runtime, launch + 901, [123], [])["status"])
        self.assertEqual("OPTIMIZING", watcher.evaluate_snapshot(runtime, launch + 901, [], [])["status"])
        self.assertEqual("COMPUTE_BUDGET_EXCEEDED",
                         watcher.evaluate_snapshot(runtime, launch + 901, [123], [123])["status"])

    def make_parseable_opt(self, path, include_fixed=True):
        header_size, record_size = 1800, 304
        data = bytearray(header_size + record_size * 3)
        struct.pack_into("<II", data, 428, header_size, record_size)
        declarations = [(1446, "InpPeriod", True)]
        declarations.append((1622, "InpFixed" if include_fixed else "InpFrameworkFixed", False))
        for absolute, name, optimized in declarations:
            encoded = (name + "\0").encode("utf-16le"); data[absolute:absolute + len(encoded)] = encoded
            struct.pack_into("<I", data, absolute + 132, 82)
            struct.pack_into("<I", data, absolute + 140, 280)
            struct.pack_into("<I", data, absolute + 144, 4)
            struct.pack_into("<qqq", data, absolute + 152, 10, 1 if optimized else 0, 30 if optimized else 10)
        for index, period in enumerate((10, 20, 30)):
            record = header_size + index * record_size
            struct.pack_into("<q", data, record, index + 1)
            struct.pack_into("<d", data, record + 8, 10000.0)
            struct.pack_into("<d", data, record + 24, 100.0)
            struct.pack_into("<d", data, record + 152, 5.0)
            struct.pack_into("<d", data, record + 176, 1.0)
            struct.pack_into("<d", data, record + 184, 1.2)
            struct.pack_into("<d", data, record + 192, 1.0)
            struct.pack_into("<d", data, record + 200, 1.0)
            struct.pack_into("<d", data, record + 208, 120.0)
            struct.pack_into("<d", data, record + 216, 16.0)
            struct.pack_into("<i", data, record + 228, 500)
            struct.pack_into("<I", data, record + 280, period)
        path.write_bytes(data)

    def test_strict_parser_uses_header_schema_and_rejects_partial_tail(self):
        opt = self.root / "schema.opt"; self.make_parseable_opt(opt)
        event = self.root / "tf-complete.json"
        event.write_text(json.dumps({"cycle_id": self.cycle, "timeframe": "M5", "status": "TIMEFRAME_COMPLETE",
                                     "opt_sha256": sha256(opt)}), encoding="utf-8")
        evidence = stage3_parser.parse_opt(self.handoff, event, opt, self.root / "parsed.csv", self.root / "parsed.json", "M5")
        self.assertEqual(3, evidence["records"]); self.assertIn("InpPeriod", evidence["optimized_parameters"])
        with (self.root / "parsed.csv").open(newline="", encoding="utf-8") as handle:
            self.assertEqual("120.0", next(csv.DictReader(handle))["MarginLevel"])
        opt.write_bytes(opt.read_bytes() + b"x")
        event.write_text(json.dumps({"cycle_id": self.cycle, "timeframe": "M5", "status": "TIMEFRAME_COMPLETE",
                                     "opt_sha256": sha256(opt)}), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "incomplete opt record tail"):
            stage3_parser.parse_opt(self.handoff, event, opt, self.root / "bad.csv", self.root / "bad.json", "M5")

    def test_parser_accepts_only_source_declared_fixed_tester_window_metadata(self):
        self.source.write_text(
            'input int InpPeriod = 20;\n'
            'input string InpFromDate = "2025.01.10";\n'
            'input string InpToDate = "2026.01.10";\n',
            encoding="utf-8",
        )
        ini = self.inis["M5"]
        stage1_spec = json.loads(self.spec.read_text(encoding="utf-8"))
        stage2_config = preparer.stage2_config_from_spec(
            stage1_spec, source_text=self.source.read_text(encoding="utf-8"))
        period = stage2_config["inputs"]["InpPeriod"]
        period_row = (f"InpPeriod={period['default']:g}||{period['start']:g}||"
                      f"{period['step']:g}||{period['stop']:g}||Y")
        ini.write_text(
            '[Tester]\nFromDate=2025.01.10\nToDate=2026.01.10\n'
            f'[TesterInputs]\n{period_row}\n'
            'InpFromDate="2025.01.10"||"2025.01.10"||1||"2025.01.10"||N\n'
            'InpToDate="2026.01.10"||"2026.01.10"||1||"2026.01.10"||N\n',
            encoding="utf-8",
        )
        handoff = json.loads(self.handoff.read_text(encoding="utf-8"))
        handoff.update({"source": str(self.source.resolve()), "source_sha256": sha256(self.source)})
        handoff["approved_tester_config_sha256"] = preparer.config_sha256(stage2_config)
        handoff["ini_sha256"]["M5"] = sha256(ini)
        self.handoff.write_text(json.dumps(handoff), encoding="utf-8")
        opt = self.root / "window-metadata.opt"
        self.make_parseable_opt(opt)
        opt_bytes = bytearray(opt.read_bytes())
        for index, period_value in enumerate((11, 20, 30)):
            struct.pack_into("<I", opt_bytes, 1800 + index * 304 + 280, period_value)
        opt.write_bytes(opt_bytes)
        event = self.root / "window-metadata.complete.json"
        event.write_text(json.dumps({"cycle_id": self.cycle, "timeframe": "M5",
                                     "status": "TIMEFRAME_COMPLETE", "opt_sha256": sha256(opt)}),
                         encoding="utf-8")
        evidence = stage3_parser.parse_opt(
            self.handoff, event, opt, self.root / "window.csv", self.root / "window.json", "M5")
        self.assertEqual({"InpFromDate", "InpToDate"},
                         set(evidence["post_stage2_infrastructure_parameters"]))

        ini.write_text(ini.read_text(encoding="utf-8").replace(
            'InpToDate="2026.01.10"||"2026.01.10"||1||"2026.01.10"||N',
            'InpToDate="2026.01.11"||"2026.01.11"||1||"2026.01.11"||N'),
            encoding="utf-8")
        handoff["ini_sha256"]["M5"] = sha256(ini)
        self.handoff.write_text(json.dumps(handoff), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "InpToDate must be a fixed value"):
            stage3_parser.parse_opt(
                self.handoff, event, opt, self.root / "bad-window.csv", self.root / "bad-window.json", "M5")

    def test_parser_accepts_fixed_ini_parameter_omitted_from_opt_header(self):
        spec = {"strategy": {"parameters": ["InpPeriod = 20", "InpFixed = PERIOD_H1"]},
                "tester_config": {"schema_version": 1, "symbol": "ETHUSD", "timeframes": ["M5", "M15", "H1"],
                                  "from_date": "2025.01.10", "to_date": "2026.01.10",
                                  "inputs": {"InpPeriod": {"default": 20, "start": 10, "step": 1, "stop": 30, "optimize": "Y"},
                                             "InpFixed": {"default": "PERIOD_H1", "start": 0, "step": 0, "stop": 0, "optimize": "N"}}}}
        self.spec.write_text(json.dumps(spec), encoding="utf-8")
        ini = self.inis["M5"]
        ini.write_text("[TesterInputs]\nInpPeriod=20||10||1||30||Y\nInpFixed=PERIOD_H1||0||0||0||N\n", encoding="utf-8")
        handoff = json.loads(self.handoff.read_text())
        handoff["stage1_spec_sha256"] = sha256(self.spec)
        handoff["ini_sha256"]["M5"] = sha256(ini)
        self.handoff.write_text(json.dumps(handoff), encoding="utf-8")
        opt = self.root / "fixed-omitted.opt"; self.make_parseable_opt(opt, include_fixed=False)
        event = self.root / "fixed-omitted.complete.json"
        event.write_text(json.dumps({"cycle_id": self.cycle, "timeframe": "M5", "status": "TIMEFRAME_COMPLETE",
                                     "opt_sha256": sha256(opt)}), encoding="utf-8")
        evidence = stage3_parser.parse_opt(self.handoff, event, opt, self.root / "fixed.csv", self.root / "fixed.json", "M5")
        self.assertEqual(["InpFixed"], evidence["fixed_parameters_omitted_from_opt_header"])

    def test_parser_rejects_ini_and_opt_optimization_surface_mismatch(self):
        ini = self.inis["M5"]
        ini.write_text("[TesterInputs]\nInpPeriod=20||10||1||30||N\n", encoding="utf-8")
        handoff = json.loads(self.handoff.read_text()); handoff["ini_sha256"]["M5"] = sha256(ini)
        self.handoff.write_text(json.dumps(handoff), encoding="utf-8")
        opt = self.root / "mismatch.opt"; self.make_parseable_opt(opt)
        event = self.root / "mismatch.complete.json"
        event.write_text(json.dumps({"cycle_id": self.cycle, "timeframe": "M5", "status": "TIMEFRAME_COMPLETE",
                                     "opt_sha256": sha256(opt)}), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "optimized parameter surface differs"):
            stage3_parser.parse_opt(self.handoff, event, opt, self.root / "mismatch.csv", self.root / "mismatch.json", "M5")

    def test_parser_accepts_stage3_wide_grid_adjusted_from_stage1_estimate(self):
        spec = {"tester_config": {"inputs": {
            "InpMaxTradeBars": {
                "default": 12, "start": 6, "step": 1, "stop": 48,
                "optimize": "Y", "type": "int",
                "domain_provenance": {"method": "source_comment", "source_domain": [6, 48, 1]},
            },
        }}}
        ini = self.root / "stage3-wide.ini"
        ini.write_text("[TesterInputs]\nInpMaxTradeBars=12||6||1||22||Y\n", encoding="utf-8")
        stage3_parser.validate_ini_surface_against_spec(ini, spec, {"InpMaxTradeBars"})

    def test_parser_accepts_default_aligned_quarter_step_regime_grid(self):
        spec = {"tester_config": {"inputs": {
            "InpRegime_ExtFearLot": {
                "default": 0.125, "start": 0, "step": 0.25, "stop": 5,
                "optimize": "Y", "type": "double",
            },
        }, "input_constraints": []}}
        ini = self.root / "off-phase-default-regime.ini"
        ini.write_text(
            "[TesterInputs]\nInpRegime_ExtFearLot=0.125||0.125||0.25||4.875||Y\n",
            encoding="utf-8",
        )
        stage3_parser.validate_ini_surface_against_spec(ini, spec, {"InpRegime_ExtFearLot"})
        ini.write_text(
            "[TesterInputs]\nInpRegime_ExtFearLot=0.125||0.125||0.3||4.925||Y\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "default-aligned quarter-step grid"):
            stage3_parser.validate_ini_surface_against_spec(ini, spec, {"InpRegime_ExtFearLot"})

    def test_parser_accepts_only_constraint_authenticated_regime_floor_grid(self):
        spec = {"tester_config": {
            "inputs": {"InpRegime_ExtFear_Lot_Mult": {
                "default": 1.0, "start": 0.0, "step": 0.25, "stop": 5.0,
                "optimize": "Y", "type": "double"}},
            "input_constraints": [{"id": "no_zero_extfear_exposure", "scope": "single",
                "coefficients": {"InpRegime_ExtFear_Lot_Mult": 1.0},
                "operator": ">=", "limit": 0.1}],
        }}
        ini = self.root / "constraint-floor.ini"
        ini.write_text("[TesterInputs]\nInpRegime_ExtFear_Lot_Mult=1||0.25||0.25||5||Y\n", encoding="utf-8")
        stage3_parser.validate_ini_surface_against_spec(ini, spec, {"InpRegime_ExtFear_Lot_Mult"})
        ini.write_text("[TesterInputs]\nInpRegime_ExtFear_Lot_Mult=1||0.5||0.25||5||Y\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Stage 3 regime grid mismatch"):
            stage3_parser.validate_ini_surface_against_spec(ini, spec, {"InpRegime_ExtFear_Lot_Mult"})

    def test_parser_rejects_invalid_stage3_grid(self):
        spec = {"tester_config": {"inputs": {
            "InpMaxTradeBars": {"default": 12, "start": 6, "step": 1, "stop": 48, "optimize": "Y"},
        }}}
        ini = self.root / "invalid-stage3-wide.ini"
        ini.write_text("[TesterInputs]\nInpMaxTradeBars=12||22||0||6||Y\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Stage 3 optimized range is invalid"):
            stage3_parser.validate_ini_surface_against_spec(ini, spec, {"InpMaxTradeBars"})

    def test_mt5_final_reachable_grid_step_is_not_discarded(self):
        inp = {"name": "InpBaseLot", "type": "d", "optimized": True,
               "start": 0.01, "step": 0.25, "stop": 5.0}
        self.assertAlmostEqual(5.01, stage3_parser.mt5opt.effective_grid_stop(inp))
        row = [0, 1.0, 1.0, 1.0, 1.0, 1.0, 120.0, 1.0, 1,
               0.0, 0.0, 10000.0, 5.01]
        self.assertEqual([], stage3_parser.mt5opt.validate_parameter_rows([row], [inp]))

    def test_h1_skip_and_selection_use_only_four_gates(self):
        m5 = self.write_result("M5", profit=100, cf=20, trades=500, dd=10)
        m15 = self.write_result("M15", profit=100, cf=19, trades=300, dd=10)
        decision = evaluator.evaluate(self.handoff, [m5, m15], self.root / "decision.json")
        self.assertEqual("READY_FOR_STAGE4", decision["verdict"]); self.assertEqual("M5", decision["selected_tf"])
        self.assertNotIn("sharpe", decision["gates"]); self.assertNotIn("profit_factor", decision["gates"])

    def test_split_metrics_runs_h1_and_all_fail_has_no_selected_tf(self):
        m5 = self.write_result("M5", profit=100, cf=20, trades=300, dd=10)
        m15 = self.write_result("M15", profit=100, cf=22, trades=450, dd=10)
        partial = evaluator.evaluate(self.handoff, [m5, m15], self.root / "partial.json")
        self.assertEqual("NEXT_TIMEFRAME", partial["verdict"]); self.assertEqual("H1", partial["next_timeframe"])
        m5 = self.write_result("M5", profit=100, cf=22.27, trades=333, dd=15, name="M5-fail.json")
        m15 = self.write_result("M15", profit=100, cf=25.06, trades=295, dd=12, name="M15-fail.json")
        h1 = self.write_result("H1", profit=100, cf=21.14, trades=168, dd=9, name="H1-fail.json")
        decision = evaluator.evaluate(self.handoff, [m5, m15, h1], self.root / "failure.json")
        self.assertEqual("FAIL_ROTATE", decision["verdict"]); self.assertIsNone(decision["selected_tf"])
        self.assertEqual("M15", decision["highest_cf_tf"]); self.assertEqual("M5", decision["highest_tpm_tf"])

    def test_zero_trade_in_completed_timeframe_routes_to_repair_before_next_tf(self):
        m5_zero = self.write_result("M5", profit=0, cf=0, trades=0, dd=0, name="M5-early-zero.json")
        decision_path = self.root / "early-zero.json"
        decision = evaluator.evaluate(self.handoff, [m5_zero], decision_path)
        self.assertEqual("REPAIR_REQUIRED", decision["verdict"])
        self.assertIsNone(decision["next_timeframe"])
        self.assertEqual({"kind": "completed_timeframe_zero_trades", "timeframe": "M5"},
                         decision["repair_trigger"])
        self.assertTrue(evaluator.requires_stage2_repair(decision["timeframes"]))
        verification = verifier.verify(self.handoff, decision_path, self.root / "early-zero-verification.json")
        self.assertTrue(verification["passed"], verification)

        m5_active = self.write_result("M5", profit=100, cf=20, trades=300, dd=10, name="M5-active.json")
        m15_zero = self.write_result("M15", profit=0, cf=0, trades=0, dd=0, name="M15-early-zero.json")
        m15_decision = evaluator.evaluate(self.handoff, [m5_active, m15_zero], self.root / "m15-zero.json")
        self.assertEqual("REPAIR_REQUIRED", m15_decision["verdict"])
        self.assertEqual("M15", m15_decision["repair_trigger"]["timeframe"])
        self.assertIsNone(m15_decision["next_timeframe"])
        self.assertTrue(verifier.verify(
            self.handoff, self.root / "m15-zero.json", self.root / "m15-zero-verification.json"
        )["passed"])

    def test_zero_trades_across_all_timeframes_requires_repair(self):
        m5 = self.write_result("M5", profit=0, cf=0, trades=0, dd=0, name="M5-zero.json")
        m15 = self.write_result("M15", profit=0, cf=0, trades=0, dd=0, name="M15-zero.json")
        h1 = self.write_result("H1", profit=0, cf=0, trades=0, dd=0, name="H1-zero.json")
        decision = evaluator.evaluate(self.handoff, [m5, m15, h1], self.root / "zero.json")
        self.assertEqual("REPAIR_REQUIRED", decision["verdict"]); self.assertIsNone(decision["selected_tf"])

    def test_stage3_verifier_recomputes_metrics_from_csv_and_preserves_hash_failure(self):
        evidence = [self.write_result(tf, profit=100, cf=20, trades=500, dd=10)
                    for tf in ("M5", "M15")]
        decision_path = self.root / "decision.json"
        evaluator.evaluate(self.handoff, evidence, decision_path)
        decision = json.loads(decision_path.read_text(encoding="utf-8"))
        decision["timeframes"]["M5"]["cf"] = 99
        decision_path.write_text(json.dumps(decision), encoding="utf-8")
        verification = verifier.verify(self.handoff, decision_path, self.root / "verify.json")
        self.assertFalse(verification["checks"]["metrics_recomputed"], verification)

    def test_stage3_evaluator_rejects_parse_evidence_for_wrong_ea(self):
        evidence = self.write_result("M5", profit=100, cf=20, trades=500, dd=10)
        data = json.loads(evidence.read_text(encoding="utf-8")); data["ea"] = "WRONG_EA"
        evidence.write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "parse evidence mismatch"):
            evaluator.load_completed_results(json.loads(self.handoff.read_text(encoding="utf-8")), [evidence])

    def test_zero_token_driver_routes_completed_zero_trade_to_stage2_without_launching_next_tf(self):
        opt = self.root / "M5-zero-driver.opt"; opt.write_bytes(b"complete-opt")
        event = self.root / "M5-zero-driver.complete.json"
        journal_hash = "A" * 64
        event.write_text(json.dumps({
            "cycle_id": self.cycle, "timeframe": "M5", "status": "TIMEFRAME_COMPLETE",
            "completion_mode": "full", "completion_basis": "fresh_genetic_journal",
            "ui_passes_complete": False, "completion_ratio": None,
            "ui_pass_counter": {"ok": False, "current": 0, "total": None},
            "tester_log_completion_evidence": {
                "early": True, "final": False, "cache_saved": False, "clean": False,
                "error_lines": [], "sha256": journal_hash,
            },
            "tester_log_sha256": journal_hash, "opt_sha256": sha256(opt), "consumed": False,
        }), encoding="utf-8")
        runtime = self.root / "runtime-zero.json"
        runtime.write_text(json.dumps({
            "cycle_id": self.cycle, "ea": "EA", "timeframe": "M5",
            "status": "TIMEFRAME_COMPLETE", "timeframe_event": str(event), "opt_path": str(opt),
        }), encoding="utf-8")
        decision = self.root / "decisions" / "decision.json"
        handoff_data = json.loads(self.handoff.read_text())
        handoff_data.update({
            "runtime_manifest": str(runtime), "decision": str(decision),
            "event": str(self.root / "stage3.complete.json"),
            "failed_notes": str(self.root / "failed.md"), "source_notes": str(self.root / "source.md"),
            "passing_notes": str(self.root / "passing.md"), "lease": str(self.root / "lease.json"),
        })
        self.handoff.write_text(json.dumps(handoff_data), encoding="utf-8")
        state = self.root / "state-zero-driver.json"
        state.write_text(json.dumps({
            "schema_version": 2, "cycle_id": self.cycle, "current_stage": "stage3_running",
            "current_ea": "EA", "stage_status": "stage3_m5_running",
        }), encoding="utf-8")

        def fake_parse(_handoff, _event, _opt, csv_path, evidence_path, timeframe, _runtime_path=None):
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            csv_path.write_text(
                "Pass,Profit,CustomFitness,Trades,EquityDrawdownPercent,SharpeRatio,ProfitFactor,MarginLevel\n"
                "1,0,0,0,0,0,0,1.7976931348623157e308\n",
                encoding="utf-8",
            )
            evidence = {
                "schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "EA",
                "timeframe": timeframe, "passed": True,
                "from_date": "2025.01.10", "to_date": "2026.01.10",
                "csv": str(csv_path), "csv_sha256": sha256(csv_path),
                "opt": str(opt), "opt_sha256": sha256(opt),
            }
            evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
            return evidence

        def refresh_completion(*_args, **_kwargs):
            refreshed = json.loads(event.read_text(encoding="utf-8"))
            refreshed["tester_log_completion_evidence"].update({
                "final": True, "cache_saved": True, "clean": True,
                "byte_count": 200, "sha256": journal_hash,
            })
            refreshed.update({"tester_log_start_offset": 100,
                              "tester_log_end_offset": 300,
                              "tester_log_sha256": journal_hash})
            event.write_text(json.dumps(refreshed), encoding="utf-8")

        launched = []
        def fake_launch(_handoff, timeframe):
            launched.append(timeframe)
            return {"watcher_pid": 999}

        with (mock.patch.object(driver.parser, "parse_opt", side_effect=fake_parse),
              mock.patch.object(driver.watcher, "run_once", side_effect=refresh_completion) as refreshed_once,
              mock.patch.object(driver.launcher, "cleanup_runtime_manifest"),
              mock.patch.object(driver, "LEASE", self.root / "missing-lease.json")):
            result = driver.process_completed_timeframe(state, self.handoff, fake_launch)
        self.assertEqual("REPAIR_REQUIRED", result["status"])
        self.assertEqual(1, refreshed_once.call_count)
        self.assertEqual([], launched)
        state_after = json.loads(state.read_text(encoding="utf-8"))
        self.assertEqual("stage2_pending", state_after["current_stage"])
        self.assertEqual("dispatch_stage2", state_after["next_action"])

    def test_zero_token_driver_parses_then_launches_next_timeframe(self):
        opt = self.root / "M5-driver.opt"; opt.write_bytes(b"complete-opt")
        event = self.root / "M5-driver.complete.json"
        journal_hash = "B" * 64
        event.write_text(json.dumps({
            "cycle_id": self.cycle, "timeframe": "M5", "status": "TIMEFRAME_COMPLETE",
            "completion_mode": "full", "ui_passes_complete": True, "completion_ratio": 1.0,
            "ui_pass_counter": {"ok": True, "current": 10, "total": 10},
            "tester_log_completion_evidence": {
                "final": True, "cache_saved": True, "clean": True, "error_lines": [],
                "sha256": journal_hash,
            },
            "tester_log_sha256": journal_hash, "opt_sha256": sha256(opt), "consumed": False,
        }), encoding="utf-8")
        runtime = self.root / "runtime.json"
        runtime.write_text(json.dumps({"cycle_id": self.cycle, "ea": "EA", "timeframe": "M5", "status": "TIMEFRAME_COMPLETE",
                                       "timeframe_event": str(event), "opt_path": str(opt)}), encoding="utf-8")
        decision = self.root / "decision.json"
        handoff_data = json.loads(self.handoff.read_text())
        handoff_data.update({"runtime_manifest": str(runtime), "decision": str(decision),
                             "event": str(self.root / "stage3.complete.json"),
                             "failed_notes": str(self.root / "failed.md"), "source_notes": str(self.root / "source.md"),
                             "passing_notes": str(self.root / "passing.md"), "lease": str(self.root / "lease.json")})
        self.handoff.write_text(json.dumps(handoff_data), encoding="utf-8")
        state = self.root / "state-driver.json"
        state.write_text(json.dumps({"schema_version": 2, "cycle_id": self.cycle, "current_stage": "stage3_running",
                                     "current_ea": "EA", "stage_status": "stage3_m5_running"}), encoding="utf-8")

        def fake_parse(_handoff, _event, _opt, csv_path, evidence_path, timeframe, _runtime_path=None):
            csv_path.write_text("Pass,Profit,CustomFitness,Trades,DrawdownPercent,SharpeRatio,ProfitFactor,MarginLevel\n1,100,20,300,10,1,1,120\n", encoding="utf-8")
            evidence = {"cycle_id": self.cycle, "timeframe": timeframe, "passed": True, "csv": str(csv_path),
                        "csv_sha256": sha256(csv_path), "opt": str(opt), "opt_sha256": sha256(opt),
                        "from_date": "2025.01.10", "to_date": "2026.01.10"}
            evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
            return evidence

        def fake_evaluate(_handoff, _evidence, output):
            result = {"verdict": "NEXT_TIMEFRAME", "next_timeframe": "M15"}
            output.parent.mkdir(parents=True, exist_ok=True); output.write_text(json.dumps(result), encoding="utf-8")
            return result

        launched = []
        def fake_launch(_handoff, timeframe):
            launched.append(timeframe)
            return {"watcher_pid": 999}

        with mock.patch.object(driver.parser, "parse_opt", side_effect=fake_parse), \
             mock.patch.object(driver.evaluator, "evaluate", side_effect=fake_evaluate):
            result = driver.process_completed_timeframe(state, self.handoff, fake_launch)
        self.assertEqual("NEXT_TIMEFRAME_STARTED", result["status"])
        self.assertEqual(["M15"], launched)
        self.assertEqual("stage3_m15_running", json.loads(state.read_text())["stage_status"])
        self.assertFalse(json.loads(event.read_text())["consumed"])

    def test_clean_fresh_genetic_finish_receipt_proves_full_without_ui_counter(self):
        journal_hash = "A" * 64
        event = {
            "completion_basis": "fresh_genetic_journal",
            "ui_passes_complete": False,
            "ui_pass_counter": {"ok": False, "current": 0, "total": None},
            "tester_log_start_offset": 100,
            "tester_log_end_offset": 300,
            "tester_log_sha256": journal_hash,
            "tester_log_completion_evidence": {
                "early": True, "final": True, "cache_saved": True,
                "clean": True, "error_lines": [], "byte_count": 200,
                "sha256": journal_hash,
            },
        }
        self.assertTrue(driver.stage3_completion_proves_full(event))

        event["tester_log_completion_evidence"]["byte_count"] = 199
        self.assertFalse(driver.stage3_completion_proves_full(event))
        event["tester_log_completion_evidence"]["byte_count"] = 200
        event["tester_log_completion_evidence"]["error_lines"] = ["Tester aborted"]
        self.assertFalse(driver.stage3_completion_proves_full(event))

    def test_failure_publication_requires_and_hashes_both_notes(self):
        decision_path = self.root / "decision.json"
        decision_path.write_text(json.dumps({"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "EA", "verdict": "FAIL_ROTATE",
                                             "selected_tf": None, "reason": "all failed", "from_date": "2025.01.10", "to_date": "2026.01.10",
                                             "timeframes": {"M5": {"cf": 10, "tpm": 20, "profit": 1, "drawdown_percent": 5, "passed": False}}}), encoding="utf-8")
        verification = self.root / "verify.json"
        verification.write_text(json.dumps({"cycle_id": self.cycle, "passed": True,
                                             "decision_sha256": sha256(decision_path),
                                             "stage1_spec": str(self.spec), "stage1_spec_sha256": sha256(self.spec),
                                             "stage2_event": str(self.stage2_event),
                                             "stage2_event_sha256": sha256(self.stage2_event)}), encoding="utf-8")
        state = self.root / "state.json"; state.write_text(json.dumps({"schema_version": 2, "cycle_id": self.cycle, "current_stage": "stage3_running",
                                                                       "current_ea": "EA", "queue_full": True,
                                                                       "queue_exhausted": True, "research_allowed": True}), encoding="utf-8")
        failed, source, passing = self.root / "failed.md", self.root / "sources.md", self.root / "passing.md"
        failed.write_text("failed", encoding="utf-8"); source.write_text("sources", encoding="utf-8")
        fragments = self.root / "fragments"
        event_path = self.root / "event.json"
        event = publisher.publish(state, decision_path, verification, event_path, failed, source, passing,
                                  self.root / "lease.json", fragment_dir=fragments)
        failed_fragment = publisher.fragment_path(self.cycle, "failed", fragments)
        source_fragment = publisher.fragment_path(self.cycle, "source", fragments)
        self.assertEqual(2, len(event["note_hashes"]))
        self.assertIn(self.cycle, failed_fragment.read_text())
        self.assertIn(self.cycle, source_fragment.read_text())
        self.assertEqual("failed", failed.read_text()); self.assertEqual("sources", source.read_text())
        updated = json.loads(state.read_text()); self.assertEqual("stage1_pending", updated["current_stage"]); self.assertEqual("research_new", updated["next_mode"])
        self.assertEqual(event, publisher.publish(state, decision_path, verification, event_path, failed, source, passing,
                                                  self.root / "lease.json", fragment_dir=fragments))

    def test_stage3_pass_is_idempotently_enqueued_for_stage4(self):
        decision_path = self.root / "pass-decision.json"
        decision_path.write_text(json.dumps({"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "EA",
                                             "verdict": "READY_FOR_STAGE4", "selected_tf": "M15", "reason": "passed",
                                             "from_date": "2025.01.10", "to_date": "2026.01.10", "timeframes": {}}), encoding="utf-8")
        verification = self.root / "pass-verify.json"
        verification.write_text(json.dumps({"cycle_id": self.cycle, "passed": True,
                                             "decision_sha256": sha256(decision_path), "stage1_spec": str(self.spec),
                                             "stage1_spec_sha256": sha256(self.spec), "stage2_event": str(self.stage2_event),
                                             "stage2_event_sha256": sha256(self.stage2_event)}), encoding="utf-8")
        state = self.root / "pass-state.json"
        state.write_text(json.dumps({"schema_version": 2, "cycle_id": self.cycle, "current_stage": "stage3_running", "current_ea": "EA",
                                     "run_authorization": {"schema_version": 1, "cycle_id": self.cycle,
                                                           "scope": "current_cycle_stages_1_to_5",
                                                           "authorized_by": "user_confirmed", "allowed_stages": [1, 2, 3, 4, 5],
                                                           "hold_after_stage": 5,
                                                           "reason": "User approved this full cycle through all five stages",
                                                           "authorized_utc": "2026-10-09T15:34:06Z"}}), encoding="utf-8")
        queue = self.root / "stage4-queue.json"
        fragments = self.root / "fragments"
        event_path = self.root / "pass-event.json"
        event = publisher.publish(state, decision_path, verification, event_path, self.root / "failed.md", self.root / "source.md",
                                  self.root / "passing.md", self.root / "lease.json", queue, fragments)
        self.assertEqual(5, json.loads(state.read_text())["hold_after_stage"])
        passing_fragment = publisher.fragment_path(self.cycle, "passing", fragments)
        original_fragment = passing_fragment.read_bytes()
        self.assertEqual("verified_pending", stage4_queue.entry_for_ea(queue, "EA")["status"])
        self.assertEqual(event, publisher.publish(state, decision_path, verification, event_path, self.root / "failed.md", self.root / "source.md",
                                                  self.root / "passing.md", self.root / "lease.json", queue, fragments))
        self.assertEqual(original_fragment, passing_fragment.read_bytes())
        self.assertEqual(1, len(list(fragments.glob("*.passing.md"))))

    def test_note_write_failure_blocks_rotation_and_event(self):
        decision_path = self.root / "decision-blocked.json"
        decision_path.write_text(json.dumps({"schema_version": 2, "cycle_id": self.cycle, "stage": 3, "ea": "EA", "verdict": "FAIL_ROTATE",
                                             "selected_tf": None, "reason": "all failed", "from_date": "2025.01.10", "to_date": "2026.01.10", "timeframes": {}}), encoding="utf-8")
        verification = self.root / "verify-blocked.json"
        verification.write_text(json.dumps({"cycle_id": self.cycle, "passed": True,
                                             "decision_sha256": sha256(decision_path),
                                             "stage1_spec": str(self.spec), "stage1_spec_sha256": sha256(self.spec),
                                             "stage2_event": str(self.stage2_event),
                                             "stage2_event_sha256": sha256(self.stage2_event)}), encoding="utf-8")
        state = self.root / "state-blocked.json"
        original = {"schema_version": 2, "cycle_id": self.cycle, "current_stage": "stage3_running", "current_ea": "EA"}
        state.write_text(json.dumps(original), encoding="utf-8")
        fragments = self.root / "fragments"
        failed_fragment = publisher.fragment_path(self.cycle, "failed", fragments)
        source_fragment = publisher.fragment_path(self.cycle, "source", fragments)
        event = self.root / "must-not-exist.json"
        real_atomic_bytes = publisher.atomic_bytes

        def fail_second_fragment(path, data):
            if Path(path) == source_fragment:
                raise OSError("synthetic second fragment failure")
            return real_atomic_bytes(path, data)

        with mock.patch.object(publisher, "atomic_bytes", side_effect=fail_second_fragment):
            with self.assertRaisesRegex(OSError, "second fragment failure"):
                publisher.publish(state, decision_path, verification, event, self.root / "failed.md", self.root / "source.md",
                                  self.root / "passing.md", self.root / "lease.json", fragment_dir=fragments)
        self.assertFalse(failed_fragment.exists()); self.assertFalse(source_fragment.exists())
        self.assertEqual(original, json.loads(state.read_text())); self.assertFalse(event.exists())


if __name__ == "__main__":
    unittest.main()
