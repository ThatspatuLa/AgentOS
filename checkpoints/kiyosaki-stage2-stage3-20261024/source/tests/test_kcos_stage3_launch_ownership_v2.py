"""Ownership-safety tests for the Stage 3 launcher."""
import sys
from pathlib import Path
import unittest
from unittest import mock
import json
import tempfile

ROOT = Path(r"C:\Users\Tyson\Projects\kiyosaki_stage2b")
sys.path.insert(0, str(ROOT / "state"))

import kcos_stage3_launch_v2 as launcher
import kcos_stage3_watch_v2 as watcher


class Stage3LaunchOwnershipTests(unittest.TestCase):

    def test_launcher_uses_hash_bound_stage2_grid_instead_of_stage1_rebuild(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stage2_event = root / "stage2-event.json"
            stage2_event.write_text("{}", encoding="utf-8")
            config = {"inputs": {"InpLookback": {
                "type": "int", "default": 25, "start": 15, "step": 1,
                "stop": 35, "optimize": "Y",
                "domain_provenance": {"method": "stage2_owned_optimization_grid"},
            }}}
            digest = launcher.preparer.config_sha256(config)
            stage2_handoff_path = root / "Stage2_v2.json"
            stage2_handoff_path.write_text(json.dumps({
                "schema_version": 2, "stage": 2, "cycle_id": "cycle", "ea": "EA",
                "expected_event": str(stage2_event), "approved_tester_config": config,
                "approved_tester_config_sha256": digest,
            }), encoding="utf-8")
            stage3_handoff = {
                "cycle_id": "cycle", "ea": "EA", "stage2_event": str(stage2_event),
                "approved_tester_config_sha256": digest,
            }
            event = {"approved_tester_config_sha256": digest}
            verification = {"approved_tester_config_sha256": digest}
            with mock.patch.object(launcher.preparer, "STAGE2_HANDOFF", stage2_handoff_path):
                approved = launcher.authenticated_stage2_config_for_launch(
                    stage3_handoff, event, verification)
            self.assertEqual(config, approved)
            self.assertEqual(20, approved["inputs"]["InpLookback"]["stop"] -
                             approved["inputs"]["InpLookback"]["start"])

            verification["approved_tester_config_sha256"] = "0" * 64
            with mock.patch.object(launcher.preparer, "STAGE2_HANDOFF", stage2_handoff_path):
                with self.assertRaisesRegex(ValueError, "lineage mismatch"):
                    launcher.authenticated_stage2_config_for_launch(
                        stage3_handoff, event, verification)

    def test_minato_diagnostic_offsets_capture_existing_and_empty_agent_files(self):
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            existing = (data_root / "Tester" / "Agent-127.0.0.1-3001" / "MQL5" /
                        "Files" / "minato_v2_opt_results.csv")
            existing.parent.mkdir(parents=True)
            existing.write_bytes(b"old diagnostic bytes\n")
            (data_root / "Tester" / "Agent-127.0.0.1-3002").mkdir()

            offsets = launcher.minato_diagnostic_file_offsets(data_root)

        self.assertEqual(2, len(offsets))
        self.assertEqual(len(b"old diagnostic bytes\n"), offsets[str(existing.resolve())]["size"])
        self.assertEqual(launcher.hashlib.sha256(b"old diagnostic bytes\n").hexdigest().upper(),
                         offsets[str(existing.resolve())]["prefix_sha256"])
        empty = next(value for path, value in offsets.items()
                     if Path(path).parents[2].name.endswith("3002"))
        self.assertEqual(0, empty["size"])
        self.assertEqual(launcher.hashlib.sha256(b"").hexdigest().upper(), empty["prefix_sha256"])

    def test_fresh_agent_pass_evidence_requires_exact_run_inputs_and_symbol(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "agent.log"
            run_started = launcher.time.time() - 10
            startup_time = launcher.datetime.fromtimestamp(run_started + 3).strftime("%H:%M:%S.%f")[:-3]
            content = (
                f"CS\t0\t{startup_time}\tStartup\tMetaTester 5 build 6230\n"
                "CS\t0\t23:41:25.051\tTester\texpert file added: "
                "Experts\\Kiyosaki\\EA.ex5. 123 bytes loaded\n"
                "CS\t0\t23:41:25.051\tNetwork\toptimized inputs info loaded\n"
                "CS\t0\t23:41:25.059\tSymbols\tETHUSD: symbol synchronized\n"
                "CS\t0\t23:41:25.052\tTester\toptimization pass 201 started\n"
                "CS\t0\t23:41:26.000\tTester\tOnTester result 12.3 : passed in 0:00:01\n"
            )
            log.write_text(content, encoding="utf-8")
            handoff = {"cycle_id": "cycle", "ea": "EA", "symbol": "ETHUSD",
                       "ini_sha256": {"M15": "A" * 64}}
            runtime = {"timeframe": "M15", "launched_epoch": run_started,
                       "start_button_invoked": True,
                       "agent_offsets_before": {"127.0.0.1:3001": 0},
                       "input_schema_binding": {"status": "INPUTS_BOUND",
                                                "timeframe": "M15",
                                                "ini_sha256": "A" * 64}}
            registration = {"worker_pid": 7, "address": "127.0.0.1:3001",
                            "path": str(log), "start_offset": 0}
            with mock.patch.object(launcher, "agent_log_registrations",
                                   return_value=[registration]), \
                 mock.patch.object(launcher, "fresh_local_workers",
                                   return_value=[{"pid": 7}]):
                result = launcher.fresh_agent_optimization_progress(
                    handoff, runtime, [{"pid": 7}], Path(directory))
            self.assertEqual("authenticated_fresh_local_agent_logs", result["source"])
            self.assertTrue(result["evidence"][0]["optimization_pass_succeeded"])

            runtime["agent_offsets_before"] = {}
            log.write_bytes(b"\xff\xfe" + content.encode("utf-16-le"))
            with mock.patch.object(launcher, "agent_log_registrations",
                                   return_value=[registration]), \
                 mock.patch.object(launcher, "fresh_local_workers",
                                   return_value=[{"pid": 7}]):
                recovered = launcher.fresh_agent_optimization_progress(
                    handoff, runtime, [{"pid": 7}], Path(directory))
            self.assertIsNotNone(recovered)
            self.assertEqual("authenticated_fresh_local_agent_logs", recovered["source"])

            log.write_text(content.replace("EA.ex5", "OtherEA.ex5"), encoding="utf-8")
            with mock.patch.object(launcher, "agent_log_registrations",
                                   return_value=[registration]), \
                 mock.patch.object(launcher, "fresh_local_workers",
                                   return_value=[{"pid": 7}]):
                rejected = launcher.fresh_agent_optimization_progress(
                    handoff, runtime, [{"pid": 7}], Path(directory))
            self.assertIsNone(rejected)

    def test_fresh_agent_progress_rejects_unbound_or_wrong_ini(self):
        handoff = {"cycle_id": "cycle", "ea": "EA", "symbol": "ETHUSD",
                   "ini_sha256": {"M15": "A" * 64}}
        runtime = {"timeframe": "M15", "launched_epoch": 100,
                   "start_button_invoked": True,
                   "input_schema_binding": {"status": "INPUTS_BOUND",
                                            "timeframe": "M15",
                                            "ini_sha256": "B" * 64}}
        with mock.patch.object(launcher, "agent_log_registrations") as registrations:
            result = launcher.fresh_agent_optimization_progress(
                handoff, runtime, [{"pid": 7}], Path("."))
        self.assertIsNone(result)
        registrations.assert_not_called()

    def test_agent_log_resolver_uses_portable_tester_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            date_name = launcher.datetime.fromtimestamp(launcher.time.time()).strftime("%Y%m%d.log")
            log = root / "Tester" / "Agent-127.0.0.1-3001" / "logs" / date_name
            log.parent.mkdir(parents=True)
            log.write_bytes(b"prior")
            evidence = [{"pid": 7, "address": "127.0.0.1:3001"}]
            with mock.patch.object(launcher, "worker_address", return_value=None):
                registrations = launcher.agent_log_registrations(
                    [7], root, launcher.time.time(), {}, evidence)
            self.assertEqual(log.resolve(), Path(registrations[0]["path"]).resolve())
            self.assertEqual(5, launcher.agent_log_offsets(root, launcher.time.time())["127.0.0.1:3001"])

    def test_tester_timeframe_combo_is_set_and_verified(self):
        controls = launcher._TesterControls.__new__(launcher._TesterControls)
        controls.children = [10]
        controls.gui = mock.Mock()
        controls.gui.GetClassName.return_value = "ComboBox"
        controls.gui.GetDlgCtrlID.return_value = 77
        controls.gui.GetParent.return_value = 50
        values = ["M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12", "M15",
                  "M20", "M30", "H1", "H2", "H3", "H4", "H6", "H8", "H12",
                  "Daily", "Weekly", "Monthly"]
        selected = {"index": 4}

        def send(hwnd, message, wparam, lparam):
            if message == 0x0146: return len(values)
            if message == 0x0148:
                launcher.ctypes.cast(lparam, launcher.ctypes.POINTER(
                    launcher.ctypes.c_wchar * 128)).contents.value = values[wparam]
                return len(values[wparam])
            if message == 0x014E:
                selected["index"] = wparam
                return wparam
            if message == 0x0147: return selected["index"]
            return 1

        controls.user = mock.Mock()
        controls.user.SendMessageW.side_effect = send
        self.assertTrue(controls.select_tester_timeframe("M15"))
        self.assertEqual(8, selected["index"])
        self.assertEqual("expected M15; selected M15", controls.timeframe_binding_detail)

    def test_parent_journal_rejects_wrong_timeframe_for_exact_ea(self):
        handoff = {"ea": "EA", "symbol": "ETHUSD"}
        wrong = {"run_identity_lines": [
            'Tester Experts\\Kiyosaki\\EA.ex5 on ETHUSD,M5 from 2025.01.10 to 2026.01.10']}
        correct = {"run_identity_lines": [
            'Tester Experts\\Kiyosaki\\EA.ex5 on ETHUSD,M15 from 2025.01.10 to 2026.01.10']}
        self.assertIsNotNone(launcher.tester_timeframe_mismatch(wrong, handoff, "M15"))
        self.assertIsNone(launcher.tester_timeframe_mismatch(correct, handoff, "M15"))

    def test_only_hash_authenticated_deferred_expert_notice_is_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            startup = Path(directory) / "deferred.ini"
            startup.write_text("[Tester]\nSymbol=ETHUSD\nPeriod=M15\n",
                               encoding="utf-8")
            runtime = {"startup_ini": str(startup),
                       "startup_ini_sha256": launcher.sha256(startup)}
            notice = "Tester no expert specified in the start config"
            other = "Tester optimization cannot be started"
            journal = {"code": "TESTER_JOURNAL_ERROR",
                       "error_lines": [notice, other]}
            filtered = launcher.filter_authenticated_no_expert_notice(journal, runtime)
            self.assertEqual([other], filtered["error_lines"])
            self.assertEqual([notice], filtered["benign_startup_lines"])
            runtime["startup_ini_sha256"] = "0" * 64
            unchanged = launcher.filter_authenticated_no_expert_notice(journal, runtime)
            self.assertEqual([notice, other], unchanged["error_lines"])

    def test_bound_inputs_cannot_pass_when_period_selection_fails(self):
        class Controls:
            def stopped(self): return True
            def expert_choices(self): return [(10, 0, "Kiyosaki\\EA.ex5")]
            def select_expert(self, *_): pass
            def select_tester_timeframe(self, expected):
                self.expected = expected
                return False

        with tempfile.TemporaryDirectory() as directory:
            ini = Path(directory) / "prepared.ini"
            ini.write_text("[Tester]\nExpert=Kiyosaki\\EA.ex5\nPeriod=M15\n"
                           "[TesterInputs]\nInpA=1||1||1||2||Y\n", encoding="utf-8")
            controls = Controls()
            result = launcher.bind_tester_input_schema(1, ini, controls)
        self.assertEqual("TIMEFRAME_NOT_READY", result["status"])
        self.assertEqual("M15", controls.expected_timeframe if hasattr(controls, "expected_timeframe")
                         else controls.expected)

    def test_expert_reselect_sends_end_ok_before_change_and_closeup(self):
        controls = launcher._TesterControls.__new__(launcher._TesterControls)
        controls.user = mock.Mock()
        controls.gui = mock.Mock()
        controls.gui.GetDlgCtrlID.return_value = 42
        controls.gui.GetParent.return_value = 100

        with mock.patch.object(launcher.time, "sleep"):
            controls.select_expert(200, 3)

        self.assertEqual(
            [(200, 0x014E, 3, 0), (100, 0x0111, (9 << 16) | 42, 200),
             (100, 0x0111, (1 << 16) | 42, 200), (100, 0x0111, (8 << 16) | 42, 200)],
            [call.args for call in controls.user.SendMessageW.call_args_list],
        )

    def test_schema_binding_requires_selected_inputs_tab_and_near_exact_row_count(self):
        class Controls:
            def __init__(self, rows): self.rows = rows; self.selected = False
            def stopped(self): return True
            def expert_choices(self): return [(10, 0, "Kiyosaki\\EA.ex5")]
            def select_expert(self, *_): pass
            def select_inputs_tab(self): self.selected = True; return True
            def input_grid_stats(self):
                return {"rows": self.rows if self.selected else 36}

        with tempfile.TemporaryDirectory() as directory:
            ini = Path(directory) / "prepared.ini"
            ini.write_text("[Tester]\nExpert=Kiyosaki\\EA.ex5\n[TesterInputs]\n"
                           "InpA=1||1||1||2||Y\nInpB=1||1||1||2||Y\n", encoding="utf-8")
            exact = launcher.bind_tester_input_schema(1, ini, Controls(2))
            journal_false_positive = launcher.bind_tester_input_schema(1, ini, Controls(36))

        self.assertEqual("INPUTS_BOUND", exact["status"])
        self.assertEqual("INPUTS_NOT_READY", journal_false_positive["status"])

    def test_schema_binding_refreshes_navigator_once_before_reporting_missing_expert(self):
        class Controls:
            def __init__(self, appears_after_refresh):
                self.refreshes = 0
                self.selected = False
                self.appears_after_refresh = appears_after_refresh
            def stopped(self): return True
            def expert_choices(self):
                if self.appears_after_refresh and self.refreshes:
                    return [(10, 0, "Kiyosaki\\EA.ex5")]
                return []
            def refresh_navigator(self): self.refreshes += 1; return True
            def select_expert(self, *_): pass
            def select_inputs_tab(self): self.selected = True; return True
            def input_grid_stats(self): return {"rows": 1}

        with tempfile.TemporaryDirectory() as directory:
            ini = Path(directory) / "prepared.ini"
            ini.write_text("[Tester]\nExpert=Kiyosaki\\EA.ex5\n[TesterInputs]\n"
                           "InpA=1||1||1||2||Y\n", encoding="utf-8")
            refreshed = Controls(True)
            bound = launcher.bind_tester_input_schema(1, ini, refreshed)
            still_missing = Controls(False)
            missing = launcher.bind_tester_input_schema(1, ini, still_missing)

        self.assertEqual("INPUTS_BOUND", bound["status"])
        self.assertEqual(1, refreshed.refreshes)
        self.assertEqual("EXPERT_NOT_READY", missing["status"])
        self.assertEqual(1, still_missing.refreshes)

    def test_truncated_navigator_ea_label_requires_unique_hashed_filename_prefix(self):
        stem = "B1_1_DCC_GARCH_Multivariate_Correlation_Regime_v8_dcc_garch_4regime_20261009_research_new_continued"
        visible = stem[:61]
        self.assertTrue(launcher._TesterControls._navigator_prefix_is_unique(
            visible, stem, [stem, stem.replace("v8_", "v7_"),
                            stem.replace("v8_", "v6_")]))
        self.assertFalse(launcher._TesterControls._navigator_prefix_is_unique(
            visible, stem, [stem, stem + "_second_candidate"]))
        self.assertFalse(launcher._TesterControls._navigator_prefix_is_unique(
            "B1_1_DCC", stem, [stem]))

    def test_schema_binding_uses_hashed_navigator_test_then_requires_exact_combo(self):
        digest = "A" * 64
        class Controls:
            def __init__(self): self.tested = False; self.selected = False
            def stopped(self): return True
            def expert_choices(self):
                return ([(10, 0, "Kiyosaki\\EA.ex5")] if self.tested else [])
            def test_exact_navigator_expert(self, expected, expected_sha256):
                self.asserted = (expected, expected_sha256)
                self.tested = True
                return True
            def select_expert(self, *_): pass
            def select_inputs_tab(self): self.selected = True; return True
            def input_grid_stats(self): return {"rows": 1}

        with tempfile.TemporaryDirectory() as directory:
            ini = Path(directory) / "prepared.ini"
            ini.write_text("[Tester]\nExpert=Kiyosaki\\EA.ex5\n[TesterInputs]\n"
                           "InpA=1||1||1||2||Y\n", encoding="utf-8")
            controls = Controls()
            result = launcher.bind_tester_input_schema(
                1, ini, controls, expected_ex5_sha256=digest)

        self.assertEqual("INPUTS_BOUND", result["status"])
        self.assertEqual(("kiyosaki\\ea.ex5", digest), controls.asserted)
        self.assertTrue(result["navigator_tested"])

    def test_inputs_tab_uses_authenticated_remote_rect_then_verifies_selection(self):
        controls = launcher._TesterControls.__new__(launcher._TesterControls)
        controls.children = [11]
        controls.gui = mock.Mock()
        controls.gui.GetClassName.return_value = "SysTabControl32"
        controls.gui.IsWindowVisible.return_value = True
        calls = {"selection": 0}
        def send(hwnd, message, *_args):
            if message == 0x1304: return 5
            if message == 0x130B:
                calls["selection"] += 1
                return 0 if calls["selection"] == 1 else 2
            return 1
        controls.user = mock.Mock()
        controls.user.SendMessageW.side_effect = send
        rect = launcher.wintypes.RECT(0, 0, 100, 20)
        controls._remote_item_rect = mock.Mock(return_value=rect)

        self.assertTrue(controls.select_inputs_tab())
        controls._remote_item_rect.assert_called_once_with(11, 0x130A, 2)
        self.assertIn("selected tab index 2", controls.input_tab_detail)

    def test_authenticated_no_autostart_notice_is_not_a_tester_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "cycle.EA.ETHUSD.M5.20250110_20260110.wide.ini"
            canonical.write_text("[Tester]\nExpert=Kiyosaki\\EA.ex5\nOptimization=2\n"
                                "[TesterInputs]\nInpA=1||1||1||2||Y\n", encoding="utf-8")
            startup = launcher.create_non_autostart_ini({}, "M5", canonical)
            log = root / "tester.log"
            log.write_text("JS\t2\t12:00:00.000\tTester\tno expert specified in the start config\n",
                           encoding="utf-8")
            runtime = {"terminal_pid": 123, "startup_ini": str(startup),
                       "startup_ini_sha256": launcher.sha256(startup),
                       "tester_log": str(log), "tester_log_start_offset": 0}
            with mock.patch.object(launcher, "bind_tester_input_schema",
                                   return_value={"status": "INPUTS_BOUND"}), \
                 mock.patch.object(launcher, "auto_click_strategy_tester_start",
                                   return_value=True):
                started = launcher.prepare_tester_schema_start(runtime, canonical)

        self.assertTrue(started)
        notice = runtime["journal_non_autostart_notice"]
        self.assertEqual("TESTER_JOURNAL_ERROR", notice["original_code"])
        self.assertIsNone(notice["code"])
        self.assertEqual([], notice["error_lines"])
        self.assertEqual(1, len(notice["benign_startup_lines"]))

    def test_schema_start_uses_runtime_authenticated_portable_terminal_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "narrow.ini"
            canonical.write_text(
                "[Tester]\nExpert=Kiyosaki\\EA.ex5\nPeriod=H1\n"
                "[TesterInputs]\nInpA=1||1||1||2||Y\n", encoding="utf-8")
            startup = launcher.create_non_autostart_ini({}, "H1", canonical)
            journal = root / "tester.log"
            journal.write_text("", encoding="utf-8")
            expected_terminal = root / "portable" / "terminal64.exe"
            runtime = {
                "terminal_pid": 123,
                "terminal_path": str(expected_terminal),
                "startup_ini": str(startup),
                "startup_ini_sha256": launcher.sha256(startup),
                "tester_log": str(journal),
                "tester_log_start_offset": 0,
            }
            with mock.patch.object(launcher, "bind_tester_input_schema",
                                   return_value={"status": "INPUTS_BOUND"}) as bind, \
                 mock.patch.object(launcher, "auto_click_strategy_tester_start",
                                   return_value=True):
                started = launcher.prepare_tester_schema_start(runtime, canonical)

        self.assertTrue(started)
        self.assertEqual(expected_terminal,
                         bind.call_args.kwargs["expected_terminal_path"])

    def test_schema_binding_fails_closed_when_inputs_tab_cannot_be_selected(self):
        class Controls:
            def stopped(self): return True
            def expert_choices(self): return [(10, 0, "Kiyosaki\\EA.ex5")]
            def select_expert(self, *_): pass
            def select_inputs_tab(self): return False

        with tempfile.TemporaryDirectory() as directory:
            ini = Path(directory) / "prepared.ini"
            ini.write_text("[Tester]\nExpert=Kiyosaki\\EA.ex5\n[TesterInputs]\n"
                           "InpA=1||1||1||2||Y\n", encoding="utf-8")
            result = launcher.bind_tester_input_schema(1, ini, Controls())

        self.assertEqual("INPUTS_TAB_NOT_READY", result["status"])

    def test_non_autostart_ini_preserves_authenticated_grid_and_removes_only_expert(self):
        with tempfile.TemporaryDirectory() as directory:
            canonical = Path(directory) / "cycle.EA.ETHUSD.M5.20250110_20260110.wide.ini"
            original = ("[Tester]\nExpert=Kiyosaki\\EA.ex5\nSymbol=ETHUSD\n"
                        "Optimization=2\n[TesterInputs]\nInpA=1||1||1||2||Y\n")
            canonical.write_text(original, encoding="utf-8")

            startup = launcher.create_non_autostart_ini({}, "M5", canonical)
            expected = original.replace("Expert=Kiyosaki\\EA.ex5\n", "")
            runtime = {"startup_ini": str(startup.resolve()),
                       "startup_ini_sha256": launcher.sha256(startup)}
            authenticated = launcher.authenticated_startup_ini({}, "M5", canonical, runtime)

            self.assertEqual(original, canonical.read_text(encoding="utf-8"))
            self.assertEqual(expected, startup.read_text(encoding="utf-8-sig"))
            self.assertEqual(startup.resolve(), authenticated)
            runtime["startup_ini_sha256"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "startup INI hash mismatch"):
                launcher.authenticated_startup_ini({}, "M5", canonical, runtime)

    def test_cached_profile_sync_matches_mt5_truncated_long_ea_prefix(self):
        ea = "B1_1_DCC_GARCH_Multivariate_Correlation_Regime_v5_closed_bar_20261009_research_new"
        truncated = ea[:59]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir = root / "portable"
            profile_dir = data_dir / "MQL5" / "Profiles" / "Tester"
            profile_dir.mkdir(parents=True)
            cached = profile_dir / f"{truncated}.ETHUSD.M5.20250110_20260110.120.ini"
            cached.write_text("[Tester]\n[TesterInputs]\nInpWindow=1||1||1||2||N\n", encoding="utf-8")
            canonical = root / "authenticated.wide.ini"
            canonical.write_text(
                "[Tester]\nSymbol=ETHUSD\nPeriod=M5\nFromDate=2025.01.10\nToDate=2026.01.10\n"
                "[TesterInputs]\nInpWindow=2||1||1||3||Y\n",
                encoding="utf-8",
            )

            records = launcher.sync_cached_tester_profiles(data_dir, ea, canonical)

            ini_record = next(record for record in records if record.get("profile_type") != "set")
            set_record = next(record for record in records if record.get("profile_type") == "set")
            self.assertEqual(1, ini_record["optimized_rows"])
            self.assertEqual("seeded_from_authenticated_ini", set_record["action"])
            self.assertIn("InpWindow=2||1||1||3||Y", cached.read_text(encoding="utf-8-sig"))
            self.assertTrue(Path(ini_record["backup"]).is_file())

    def test_cached_profile_sync_seeds_exact_truncated_set_from_authenticated_ini(self):
        ea = "B1_1_DCC_GARCH_Multivariate_Correlation_Regime_v5_closed_bar_20261009_research_new"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir = root / "portable"
            (data_dir / "MQL5" / "Profiles" / "Tester").mkdir(parents=True)
            canonical = root / "authenticated.wide.ini"
            canonical.write_text(
                "[Tester]\nExpert=Kiyosaki\\EA.ex5\nSymbol=ETHUSD\nPeriod=M5\n"
                "FromDate=2025.01.10\nToDate=2026.01.10\n[TesterInputs]\n"
                "InpWindow=2||1||1||3||Y\nInpFixed=7||7||0||7||N\n",
                encoding="utf-8",
            )

            records = launcher.sync_cached_tester_profiles(data_dir, ea, canonical)

            set_path = data_dir / "MQL5" / "Profiles" / "Tester" / f"{ea[:59]}.set"
            ini_path = data_dir / "MQL5" / "Profiles" / "Tester" / (
                f"{ea[:59]}.ETHUSD.M5.20250110_20260110.120.ini")
            self.assertTrue(set_path.is_file())
            self.assertTrue(ini_path.is_file())
            self.assertEqual(["InpWindow=2||1||1||3||Y"], [
                line.strip() for line in set_path.read_bytes().decode("utf-16").splitlines()
                if line.strip().endswith("||Y")
            ])
            self.assertEqual("InpWindow=2||1||1||3||Y", next(
                line for line in ini_path.read_text(encoding="utf-8-sig").splitlines()
                if line.startswith("InpWindow=")))
            self.assertEqual(2, len([record for record in records
                                     if record.get("action") == "seeded_from_authenticated_ini"]))
            self.assertTrue(any(record.get("action") == "seeded_from_authenticated_ini"
                                for record in records))

    def test_tester_journal_parser_captures_new_mt5_optimization_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "20260912.log"
            prefix = "old journal content\r\n".encode("utf-16le")
            failure = (
                "LQ\t2\t22:59:56.351\tTester\tno optimized parameter selected, "
                "please check input(s) to be optimized and set start, step and stop values\r\n"
            ).encode("utf-16le")
            log.write_bytes(prefix + failure)

            diagnostic = launcher.read_fresh_tester_journal(log, len(prefix))

        self.assertEqual("NO_OPTIMIZED_PARAMETERS", diagnostic["code"])
        self.assertEqual(1, len(diagnostic["error_lines"]))
        self.assertIn("no optimized parameter selected", diagnostic["error_lines"][0])

    def test_recovery_profile_uses_hash_verified_wide_ini_not_a_saved_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template = root / "EA.wide.ini"
            template.write_text(
                "[Tester]\nEA_NAME=EA\nExpert=Kiyosaki\\EA.ex5\nSymbol=ETHUSD\n"
                "Period=M5\nFromDate=2025.01.10\nToDate=2026.01.10\n"
                "Optimization=2\nOptimizationCriterion=6\nVisual=0\n"
                "[TesterInputs]\nInpWindow=2||1||1||3||Y\n",
                encoding="utf-8",
            )
            ini_text = launcher.preparer.render_ini(template.read_text(encoding="utf-8"), "EA", "M5")
            expected_hash = launcher.hashlib.sha256(ini_text.encode("utf-8")).hexdigest().upper()
            ini = root / "prepared.ini"
            ini.write_text("corrupted cached values", encoding="utf-8")
            handoff = {
                "cycle_id": "cycle", "ea": "EA", "template_ini": str(template),
                "template_ini_sha256": launcher.sha256(template), "ini_files": {"M5": str(ini)},
                "ini_sha256": {"M5": expected_hash}, "runtime_manifest": str(root / "runtime.json"),
            }

            recovered = launcher.authoritative_recovery_ini(handoff, "M5")

            self.assertEqual(expected_hash, launcher.sha256(ini))
            self.assertEqual(expected_hash, launcher.sha256(recovered))
            self.assertIn("InpWindow=2||1||1||3||Y", recovered.read_text(encoding="utf-8"))
            self.assertNotEqual(ini.resolve(), recovered.resolve())

    def test_no_optimized_parameter_journal_error_gets_one_verified_wide_profile_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            terminal = root / "terminal64.exe"; terminal.write_bytes(b"exe")
            template = root / "EA.wide.ini"
            template.write_text(
                "[Tester]\nEA_NAME=EA\nExpert=Kiyosaki\\EA.ex5\nSymbol=ETHUSD\n"
                "Period=M5\nFromDate=2025.01.10\nToDate=2026.01.10\n"
                "Optimization=2\nOptimizationCriterion=6\nVisual=0\n"
                "[TesterInputs]\nInpWindow=2||1||1||3||Y\n",
                encoding="utf-8",
            )
            ini_text = launcher.preparer.render_ini(template.read_text(encoding="utf-8"), "EA", "M5")
            expected_hash = launcher.hashlib.sha256(ini_text.encode("utf-8")).hexdigest().upper()
            ini = root / "prepared.ini"; ini.write_bytes(ini_text.encode("utf-8"))
            runtime = root / "runtime.json"
            handoff_path = root / "handoff.json"
            handoff_path.write_text(json.dumps({
                "cycle_id": "cycle", "ea": "EA", "timeframe_order": ["M5"],
                "ini_files": {"M5": str(ini)}, "ini_sha256": {"M5": expected_hash},
                "template_ini": str(template), "template_ini_sha256": launcher.sha256(template),
                "from_date": "2025.01.01", "to_date": "2026.01.01", "symbol": "ETHUSD",
                "runtime_manifest": str(runtime), "heartbeat": str(root / "heartbeat.json"),
            }), encoding="utf-8")
            log = root / "Tester" / "logs" / f"{launcher.datetime.now().strftime('%Y%m%d')}.log"
            terminal_processes = iter([mock.Mock(pid=101), mock.Mock(pid=102)])

            def launch_process(command, **_kwargs):
                process = next(terminal_processes)
                if process.pid == 101:
                    log.parent.mkdir(parents=True, exist_ok=True)
                    with log.open("ab") as handle:
                        handle.write((
                            "LQ\t2\t22:59:56.351\tTester\tno optimized parameter selected, "
                            "please check input(s) to be optimized and set start, step and stop values\r\n"
                        ).encode("utf-16le"))
                return process

            with mock.patch.object(launcher, "process_ids", return_value=[]), \
                 mock.patch.object(launcher.subprocess, "Popen", side_effect=launch_process) as popen, \
                 mock.patch.object(launcher, "cleanup_previous_runtime"):
                with self.assertRaisesRegex(ValueError, "did not start any new metatester64 workers") as caught:
                    launcher.launch_and_register(
                        handoff_path, "M5", terminal=terminal, data_dir=root,
                        runtime_path=runtime, startup_timeout=0, startup_poll_seconds=0)

            self.assertEqual(2, popen.call_count)
            retry_command = popen.call_args_list[1].args[0]
            self.assertIn("journal-retry1.wide.ini", retry_command[1])
            manifest = json.loads(runtime.read_text(encoding="utf-8"))
            self.assertEqual(1, manifest["journal_recovery_attempt"])
            self.assertEqual("NO_OPTIMIZED_PARAMETERS", manifest["journal_recovery_history"][0]["code"])
            self.assertIn("no optimized parameter selected", str(caught.exception))

    def test_cleanup_uses_only_runtime_owned_pids_even_in_exclusive_mode(self):
        previous = {
            "cycle_id": "owned-cycle",
            "terminal_pid": 11,
            "watcher_pid": 12,
            "worker_pids": [13, 14],
            "launched_utc": "2026-09-12T00:00:00Z",
            "agent_logs": [
                {"worker_pid": 13, "address": "127.0.0.1:3000"},
                {"worker_pid": 14, "address": "127.0.0.1:3001"},
            ],
        }
        ownership = [{"pid": pid} for pid in (11, 12, 13, 14)]
        with mock.patch.object(launcher, "capture_owned_process", side_effect=ownership), \
             mock.patch.object(launcher, "terminate_verified_processes") as stop:
            launcher.cleanup_previous_runtime(previous, Path(r"C:\MT5\terminal64.exe"), Path("runtime.json"))
        stop.assert_called_once_with(ownership)

    def test_cleanup_includes_workers_from_authenticated_process_evidence(self):
        terminal = Path(r"C:\isolated\terminal64.exe")
        runtime_path = Path(r"C:\isolated\runtime.json")
        worker_exe = Path(r"C:\isolated\metatester64.exe")
        previous = {
            "status": "START_REQUIRED", "terminal_pid": 11, "watcher_pid": None,
            "worker_pids": [], "launched_utc": "2026-09-12T00:00:00Z",
            "worker_process_evidence": [{"pid": 21, "address": "127.0.0.1:3001",
                                          "exe": str(worker_exe), "create_time": 100.0}],
        }
        ownership = [{"pid": 11}, {"pid": 21}]
        with (
            mock.patch.object(launcher, "capture_owned_process", side_effect=ownership) as capture,
            mock.patch.object(launcher, "terminate_verified_processes") as stop,
        ):
            launcher.cleanup_previous_runtime(previous, terminal, runtime_path, include_worker_process_evidence=True)
        self.assertEqual(2, capture.call_count)
        self.assertEqual(21, capture.call_args_list[1].args[0])
        self.assertEqual("127.0.0.1:3001", capture.call_args_list[1].args[5])
        self.assertEqual(worker_exe, capture.call_args_list[1].args[6])
        stop.assert_called_once_with(ownership)

    def test_worker_capture_rejects_a_nonportable_installation_worker(self):
        worker = mock.Mock()
        worker.name.return_value = "metatester64.exe"
        worker.exe.return_value = r"C:\Program Files\EightCap\metatester64.exe"
        worker.cmdline.return_value = [worker.exe.return_value, "/local", "/address:127.0.0.1:3001"]
        worker.create_time.return_value = 100.0
        with (
            mock.patch.object(launcher.psutil, "pid_exists", return_value=True),
            mock.patch.object(launcher.psutil, "Process", return_value=worker),
        ):
            with self.assertRaisesRegex(ValueError, "ambiguous or reused process identity"):
                launcher.capture_owned_process(
                    21, "worker", Path(r"C:\isolated\terminal64.exe"), Path("runtime.json"),
                    100.0, "127.0.0.1:3001", expected_executable=Path(r"C:\isolated\metatester64.exe"))

    def test_capture_accepts_only_the_exact_portable_worker_identity(self):
        worker_exe = Path(r"C:\isolated\metatester64.exe")
        worker = mock.Mock()
        worker.name.return_value = "metatester64.exe"
        worker.exe.return_value = str(worker_exe)
        worker.cmdline.return_value = [str(worker_exe), "/local", "/address:127.0.0.1:3001", "/portable"]
        worker.create_time.return_value = 100.0
        with (
            mock.patch.object(launcher.psutil, "pid_exists", return_value=True),
            mock.patch.object(launcher.psutil, "Process", return_value=worker),
        ):
            captured = launcher.capture_owned_process(
                21, "worker", Path(r"C:\isolated\terminal64.exe"), Path("runtime.json"),
                100.0, "127.0.0.1:3001", expected_executable=worker_exe, require_portable=True)
        self.assertIsNotNone(captured)
        self.assertEqual(worker_exe, captured["expected_executable"])

    def test_reused_runtime_pid_fails_closed_before_stop(self):
        previous = {"cycle_id": "owned-cycle", "terminal_pid": 11, "worker_pids": [],
                    "launched_utc": "2026-09-12T00:00:00Z"}
        with mock.patch.object(launcher, "capture_owned_process", side_effect=ValueError("ambiguous identity")), \
             mock.patch.object(launcher, "terminate_verified_processes") as stop:
            with self.assertRaisesRegex(ValueError, "ambiguous identity"):
                launcher.cleanup_previous_runtime(previous, Path(r"C:\MT5\terminal64.exe"), Path("runtime.json"))
        stop.assert_not_called()

    def test_verified_process_is_revalidated_before_terminate(self):
        process = mock.Mock()
        process.pid = 77
        process.name.return_value = "terminal64.exe"
        process.exe.return_value = r"C:\MT5\terminal64.exe"
        process.cmdline.return_value = [r"C:\MT5\terminal64.exe"]
        process.create_time.side_effect = [100.0, 200.0]
        with mock.patch.object(launcher.psutil, "pid_exists", return_value=True), \
             mock.patch.object(launcher.psutil, "Process", return_value=process):
            ownership = launcher.capture_owned_process(
                77, "terminal", Path(r"C:\MT5\terminal64.exe"), Path("runtime.json"), 100.0)
            with self.assertRaisesRegex(ValueError, "changed identity"):
                launcher.terminate_verified_processes([ownership])
        process.terminate.assert_not_called()

    def test_process_disappearing_during_capture_is_safe_absence(self):
        with mock.patch.object(launcher.psutil, "pid_exists", return_value=True), \
             mock.patch.object(launcher.psutil, "Process", side_effect=launcher.psutil.NoSuchProcess(77)):
            self.assertIsNone(launcher.capture_owned_process(
                77, "terminal", Path(r"C:\MT5\terminal64.exe"), Path("runtime.json"), 100.0))

    def test_process_disappearing_during_revalidation_is_safe_absence(self):
        process = mock.Mock()
        process.pid = 77
        process.create_time.side_effect = launcher.psutil.NoSuchProcess(77)
        ownership = {
            "process": process, "pid": 77, "role": "terminal", "create_time": 100.0,
            "terminal": Path(r"C:\MT5\terminal64.exe"), "runtime_path": Path("runtime.json"),
            "launched_epoch": 100.0, "expected_address": None,
        }
        launcher.terminate_verified_processes([ownership])
        process.terminate.assert_not_called()

    def test_watcher_waits_for_atomic_optimizing_transition(self):
        values = iter([{"status": "STARTING"}, {"status": "OPTIMIZING"}])
        result = watcher.wait_until_optimizing(
            Path("runtime.json"), timeout_seconds=1, poll_seconds=0,
            loader=lambda _path: next(values))
        self.assertEqual("OPTIMIZING", result["status"])

    def test_watcher_ready_handshake_is_pid_and_runtime_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime_path = Path(directory) / "runtime.json"
            ready_path = Path(directory) / "ready.json"
            runtime_path.write_text(json.dumps({
                "status": "STARTING", "watcher_pid": 55,
            }), encoding="utf-8")
            with mock.patch.object(watcher.os, "getpid", return_value=55):
                watcher.publish_watcher_ready(runtime_path, ready_path)
            ready = launcher.wait_for_watcher_ready(ready_path, 55, runtime_path, timeout_seconds=0.1)
            self.assertEqual("WATCHER_READY", ready["status"])

    def test_launch_startup_failure_leaves_starting_manifest_with_terminal_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            terminal = root / "terminal64.exe"; terminal.write_bytes(b"exe")
            ini = root / "M5.ini"; ini.write_text("ini", encoding="utf-8")
            runtime = root / "runtime.json"
            handoff_path = root / "handoff.json"
            handoff_path.write_text(json.dumps({
                "cycle_id": "cycle", "ea": "EA", "timeframe_order": ["M5"],
                "ini_files": {"M5": str(ini)}, "from_date": "2025.01.01",
                "to_date": "2026.01.01", "symbol": "ETHUSD",
                "runtime_manifest": str(runtime), "heartbeat": str(root / "heartbeat.json"),
            }), encoding="utf-8")
            terminal_process = mock.Mock(pid=101)
            with mock.patch.object(launcher, "process_ids", return_value=[]), \
                 mock.patch.object(launcher.subprocess, "Popen", return_value=terminal_process), \
                 mock.patch.object(launcher, "cleanup_previous_runtime") as cleanup:
                with self.assertRaisesRegex(ValueError, r"Expert|workers"):
                    launcher.launch_and_register(
                        handoff_path, "M5", terminal=terminal, data_dir=root,
                        runtime_path=runtime, startup_timeout=0)
            manifest = json.loads(runtime.read_text())
            self.assertEqual("STARTING", manifest["status"])
            self.assertEqual(101, manifest["terminal_pid"])
            self.assertEqual([], manifest["worker_pids"])
            self.assertEqual(2, cleanup.call_count)
            self.assertEqual("STARTING", cleanup.call_args.args[0]["status"])

    def test_launch_interrupt_after_worker_discovery_persists_worker_ownership(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            terminal = root / "terminal64.exe"; terminal.write_bytes(b"exe")
            ini = root / "M5.ini"; ini.write_text("ini", encoding="utf-8")
            runtime = root / "runtime.json"
            handoff_path = root / "handoff.json"
            handoff_path.write_text(json.dumps({
                "cycle_id": "cycle", "ea": "EA", "timeframe_order": ["M5"],
                "ini_files": {"M5": str(ini)}, "from_date": "2025.01.01",
                "to_date": "2026.01.01", "symbol": "ETHUSD",
                "runtime_manifest": str(runtime), "heartbeat": str(root / "heartbeat.json"),
            }), encoding="utf-8")
            terminal_process = mock.Mock(pid=101)
            ids_seen = []

            def fake_process_ids(name):
                if name == "terminal64.exe":
                    return []
                ids_seen.append(name)
                return [] if len(ids_seen) == 1 else [202]

            original_atomic_json = launcher.atomic_json

            def interrupt_after_worker_write(path, payload):
                original_atomic_json(path, payload)
                if payload.get("worker_pids") == [202] and payload.get("agent_logs"):
                    raise KeyboardInterrupt()

            with mock.patch.object(launcher, "process_ids", side_effect=fake_process_ids), \
                 mock.patch.object(launcher.subprocess, "Popen", return_value=terminal_process), \
                 mock.patch.object(launcher, "agent_log_registrations",
                                   return_value=[{"worker_pid": 202, "address": "127.0.0.1:3000"}]), \
                 mock.patch.object(launcher, "atomic_json", side_effect=interrupt_after_worker_write), \
                 mock.patch.object(launcher.time, "sleep", return_value=None):
                with self.assertRaises(KeyboardInterrupt):
                    launcher.launch_and_register(
                        handoff_path, "M5", terminal=terminal, data_dir=root,
                        runtime_path=runtime, startup_timeout=1, startup_poll_seconds=0)
            manifest = json.loads(runtime.read_text())
            self.assertEqual("STARTING", manifest["status"])
            self.assertEqual(101, manifest["terminal_pid"])
            self.assertEqual([202], manifest["worker_pids"])
            self.assertEqual([{"worker_pid": 202, "address": "127.0.0.1:3000"}], manifest["agent_logs"])

    def test_launch_interrupt_during_worker_address_discovery_keeps_worker_pids(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            terminal = root / "terminal64.exe"; terminal.write_bytes(b"exe")
            ini = root / "M5.ini"; ini.write_text("ini", encoding="utf-8")
            runtime = root / "runtime.json"
            handoff_path = root / "handoff.json"
            handoff_path.write_text(json.dumps({
                "cycle_id": "cycle", "ea": "EA", "timeframe_order": ["M5"],
                "ini_files": {"M5": str(ini)}, "from_date": "2025.01.01",
                "to_date": "2026.01.01", "symbol": "ETHUSD",
                "runtime_manifest": str(runtime), "heartbeat": str(root / "heartbeat.json"),
            }), encoding="utf-8")
            terminal_process = mock.Mock(pid=101)
            ids_seen = []

            def fake_process_ids(name):
                if name == "terminal64.exe":
                    return []
                ids_seen.append(name)
                return [] if len(ids_seen) == 1 else [202]

            with mock.patch.object(launcher, "process_ids", side_effect=fake_process_ids), \
                 mock.patch.object(launcher.subprocess, "Popen", return_value=terminal_process), \
                 mock.patch.object(launcher, "agent_log_registrations", side_effect=KeyboardInterrupt()), \
                 mock.patch.object(launcher.time, "sleep", return_value=None):
                with self.assertRaises(KeyboardInterrupt):
                    launcher.launch_and_register(
                        handoff_path, "M5", terminal=terminal, data_dir=root,
                        runtime_path=runtime, startup_timeout=1, startup_poll_seconds=0)
            manifest = json.loads(runtime.read_text())
            self.assertEqual("STARTING", manifest["status"])
            self.assertEqual(101, manifest["terminal_pid"])
            self.assertEqual([202], manifest["worker_pids"])
            self.assertEqual([], manifest["agent_logs"])


    def test_start_required_allows_one_bounded_retry_after_a_delivered_click(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ini = root / "M5.ini"
            ini.write_text("[Tester]\nExpert=Kiyosaki\\EA.ex5\n[TesterInputs]\nInpA=1||1||1||2||Y\n", encoding="utf-8")
            startup = launcher.create_non_autostart_ini({}, "M5", ini)
            log = root / "tester.log"
            log.write_bytes(b"")
            runtime_path = root / "runtime.json"
            handoff_path = root / "handoff.json"
            ini_hash = launcher.sha256(ini)
            handoff = {
                "cycle_id": "cycle", "ea": "EA", "symbol": "ETHUSD",
                "from_date": "2025.01.01", "to_date": "2026.01.01",
                "timeframe_order": ["M5"], "ini_files": {"M5": str(ini)},
                "ini_sha256": {"M5": ini_hash},
                "runtime_manifest": str(runtime_path), "heartbeat": str(root / "heartbeat.json"),
            }
            runtime = {
                "status": "START_REQUIRED", "cycle_id": "cycle", "ea": "EA",
                "symbol": "ETHUSD", "from_date": "2025.01.01", "to_date": "2026.01.01",
                "timeframe": "M5", "ini": str(ini.resolve()), "ini_sha256": ini_hash,
                "startup_ini": str(startup.resolve()),
                "startup_ini_sha256": launcher.sha256(startup),
                "command": [str(launcher.STAGE3_PORTABLE_TERMINAL), "/portable", f"/config:{startup}"],
                "terminal_pid": 42, "launched_epoch": 1.0, "launched_utc": "2026-01-01T00:00:00Z",
                "worker_baseline_pids": [], "worker_process_evidence": [], "worker_pids": [],
                "start_button_attempted": True, "start_button_invoked": True,
                "start_button_attempt_count": 1, "start_button_delivery_count": 1,
                "start_retry_deadline_epoch": 301.0,
                "tester_log": str(log), "tester_log_start_offset": 0,
                "journal_ui_observation_started_epoch": 1.0,
                "journal_ui_last_sample_epoch": 1.0,
                "opt_candidates_before": {}, "opt_growth_observation": {},
            }
            handoff_path.write_text(json.dumps(handoff), encoding="utf-8")
            runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
            terminal_process = mock.Mock()
            terminal_process.exe.return_value = str(launcher.EIGHTCAP_TERMINAL)
            terminal_process.cmdline.return_value = [
                str(launcher.STAGE3_PORTABLE_TERMINAL), "/portable", f"/config:{startup.resolve()}"]
            with (
                mock.patch.object(launcher, "verify_eightcap_terminal", return_value={"pid": 42}),
                mock.patch.object(launcher, "process_ids", return_value=[42]),
                mock.patch.object(launcher.psutil, "Process", return_value=terminal_process),
                mock.patch.object(launcher, "bind_tester_input_schema",
                                  return_value={"status": "INPUTS_BOUND"}),
                mock.patch.object(launcher, "fresh_local_workers", return_value=[]),
                mock.patch.object(launcher, "ensure_portable_agent_registry", return_value={"ok": True}),
                mock.patch.object(launcher, "auto_click_strategy_tester_start", return_value=True) as click,
                mock.patch.object(launcher, "resolve_tester_journal_path", return_value=log),
                mock.patch.object(launcher, "read_fresh_tester_journal",
                                  return_value={"fresh_lines": [], "error_lines": [], "code": None}),
                mock.patch.object(launcher, "journal_snapshot", return_value={}),
                mock.patch.object(launcher, "opt_candidates", return_value=[]),
                mock.patch.object(launcher.time, "time", return_value=2.0),
            ):
                result = launcher.resume_start_required(handoff_path, runtime_path)

            click.assert_called_once_with(42)
            self.assertTrue(result["start_button_invoked"])
            self.assertEqual("START_REQUIRED", result["status"])

    def test_start_required_does_not_spend_click_budget_on_undelivered_ui_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ini = root / "H1.ini"
            ini.write_text("[Tester]\nExpert=Kiyosaki\\EA.ex5\n[TesterInputs]\nInpA=1||1||1||2||Y\n", encoding="utf-8")
            startup = launcher.create_non_autostart_ini({}, "H1", ini)
            log = root / "tester.log"
            log.write_bytes(b"")
            runtime_path, handoff_path = root / "runtime.json", root / "handoff.json"
            ini_hash = launcher.sha256(ini)
            handoff_path.write_text(json.dumps({
                "cycle_id": "cycle", "ea": "EA", "symbol": "ETHUSD",
                "from_date": "2025.01.01", "to_date": "2026.01.01",
                "timeframe_order": ["H1"], "ini_files": {"H1": str(ini)},
                "ini_sha256": {"H1": ini_hash}, "runtime_manifest": str(runtime_path),
                "heartbeat": str(root / "heartbeat.json"),
            }), encoding="utf-8")
            runtime_path.write_text(json.dumps({
                "status": "START_REQUIRED", "cycle_id": "cycle", "ea": "EA",
                "symbol": "ETHUSD", "from_date": "2025.01.01", "to_date": "2026.01.01",
                "timeframe": "H1", "ini": str(ini.resolve()), "ini_sha256": ini_hash,
                "startup_ini": str(startup.resolve()), "startup_ini_sha256": launcher.sha256(startup),
                "command": [str(launcher.STAGE3_PORTABLE_TERMINAL), "/portable", f"/config:{startup.resolve()}"],
                "terminal_pid": 42, "launched_epoch": 1.0, "launched_utc": "2026-01-01T00:00:00Z",
                "startup_grace_seconds": 300,
                "start_button_attempt_count": 2, "start_button_delivery_count": 0,
                "start_button_attempted": True, "start_button_invoked": False,
                "start_button_next_attempt_epoch": 1.0,
                "input_schema_binding_attempts": 3,
                "worker_baseline_pids": [], "worker_process_evidence": [], "worker_pids": [],
                "tester_log": str(log), "tester_log_start_offset": 0,
                "journal_ui_observation_started_epoch": 1.0, "journal_ui_last_sample_epoch": 1.0,
                "opt_candidates_before": {}, "opt_growth_observation": {},
            }), encoding="utf-8")
            terminal_process = mock.Mock()
            terminal_process.exe.return_value = str(launcher.EIGHTCAP_TERMINAL)
            terminal_process.cmdline.return_value = [
                str(launcher.STAGE3_PORTABLE_TERMINAL), "/portable", f"/config:{startup.resolve()}"]
            with (
                mock.patch.object(launcher, "verify_eightcap_terminal", return_value={"pid": 42}),
                mock.patch.object(launcher, "process_ids", return_value=[42]),
                mock.patch.object(launcher.psutil, "Process", return_value=terminal_process),
                mock.patch.object(launcher, "prepare_tester_schema_start", return_value=False) as prepare,
                mock.patch.object(launcher, "fresh_local_workers", return_value=[]),
                mock.patch.object(launcher, "resolve_tester_journal_path", return_value=log),
                mock.patch.object(launcher, "read_fresh_tester_journal",
                                  return_value={"fresh_lines": [], "error_lines": [], "code": None}),
                mock.patch.object(launcher, "journal_snapshot", return_value={}),
                mock.patch.object(launcher, "opt_candidates", return_value=[]),
                # This legacy manifest was paused well beyond its launch-time
                # grace. Resume gets one persisted, bounded migration window.
                mock.patch.object(launcher.time, "time", return_value=1000.0),
            ):
                result = launcher.resume_start_required(handoff_path, runtime_path)

            prepare.assert_called_once()
            self.assertEqual("START_REQUIRED", result["status"])
            self.assertEqual(3, result["start_button_attempt_count"])
            self.assertEqual(0, result["start_button_delivery_count"])
            self.assertEqual("not_delivered", result["start_button_last_result"])
            self.assertEqual(1600.0, result["start_retry_deadline_epoch"])
            self.assertEqual("legacy_start_required_bounded_recovery",
                             result["start_retry_migration"])


    def test_start_click_does_not_toggle_open_tester_pane_closed(self):
        state = {"pane_open": True, "posts": []}
        u = mock.Mock()
        u.GetForegroundWindow.return_value = 99
        u.IsWindow.return_value = True
        def enum_windows(callback, _):
            callback(launcher.wintypes.HWND(1), 0)
            return 1
        def enum_child_windows(parent, callback, _):
            if state["pane_open"]:
                callback(launcher.wintypes.HWND(2), 0)
            return 1
        def get_pid(hwnd, out):
            launcher.ctypes.cast(out, launcher.ctypes.POINTER(launcher.wintypes.DWORD)).contents.value = 42
            return 1
        def set_text(hwnd, buffer, _):
            buffer.value = "Start"
            return 5
        def set_class(hwnd, buffer, _):
            buffer.value = "Button"
            return 6
        def get_rect(hwnd, out):
            rect = launcher.ctypes.cast(out, launcher.ctypes.POINTER(launcher.wintypes.RECT)).contents
            rect.left, rect.top, rect.right, rect.bottom = 0, 0, 100, 30
            return True
        def send_timeout(hwnd, message, wparam, lparam, flags, timeout_ms, result):
            state["posts"].append((int(getattr(hwnd, "value", hwnd) or 0), message,
                                   wparam, lparam, flags, timeout_ms))
            return 1
        u.EnumWindows.side_effect = enum_windows
        u.EnumChildWindows.side_effect = enum_child_windows
        u.GetWindowThreadProcessId.side_effect = get_pid
        u.GetWindowTextW.side_effect = set_text
        u.GetClassNameW.side_effect = set_class
        u.GetParent.return_value = launcher.wintypes.HWND(1)
        u.GetWindowRect.side_effect = get_rect
        u.SendMessageTimeoutW.side_effect = send_timeout
        dll = mock.Mock(user32=u, kernel32=mock.Mock())
        with (mock.patch.object(launcher.ctypes, "windll", dll),
              mock.patch.object(launcher.time, "sleep", return_value=None)):
            invoked = launcher.auto_click_strategy_tester_start(42)
            u.SendMessageTimeoutW.side_effect = None
            u.SendMessageTimeoutW.return_value = 0
            timed_out = launcher.auto_click_strategy_tester_start(42)
        self.assertTrue(invoked)
        self.assertFalse(timed_out)
        self.assertFalse(any(msg == 0x0100 and key in (0x11, 0x52)
                             for _, msg, key, _, _, _ in state["posts"]))
        self.assertEqual([(2, 0x00F5, 0, 0, 0x0002, 2000)], state["posts"])


if __name__ == "__main__":
    unittest.main()
