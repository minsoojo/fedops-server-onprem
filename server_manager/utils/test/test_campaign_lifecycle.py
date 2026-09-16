import unittest
import ast
from pathlib import Path
from types import SimpleNamespace as NS
from typing import Optional
from fastapi import HTTPException
from utils import campaign_lifecycle as lifecycle


class CampaignLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.server = NS(Campaign_run_id=None, Campaign_status=None, Campaign_ended_at=None,
                         FLSeReady=False, GL_Model_V=4, Target_GL_Model_V=None)
        self.runtime = {"status": "FL Server created", "port": 40028}
        self.clients = [NS(FL_task_ID="task", Device_training=True),
                        NS(FL_task_ID="other", Device_training=True)]
        self.request = NS(runId="run-a", releaseId="release-a", baseGlobalModelVersion=4,
                          targetGlobalModelVersion=5, campaign=NS(dict=lambda: {"rounds": 2}))

    def begin(self):
        lifecycle.begin(self.server, self.runtime, self.request, self.clients, "task")

    def finish(self, **kwargs):
        lifecycle.finish(self.server, self.runtime, self.clients, "task", **kwargs)

    def test_completion_clears_ready_and_client_training_once(self):
        self.begin()
        self.server.FLSeReady = True
        self.finish()
        ended = self.server.Campaign_ended_at
        self.assertFalse(self.server.FLSeReady)
        self.assertFalse(self.clients[0].Device_training)
        self.assertTrue(self.clients[1].Device_training)
        self.assertEqual(self.server.Campaign_status, "completed")
        self.finish()
        self.assertEqual(self.server.GL_Model_V, 5)
        self.assertEqual(self.server.Campaign_ended_at, ended)

    def test_next_campaign_clears_all_old_terminal_metadata(self):
        self.begin()
        self.finish()
        self.server.FLSeReady = True  # Simulate a historical inconsistent terminal snapshot.
        self.request.runId = "run-b"
        self.begin()
        self.assertFalse(self.server.FLSeReady)
        self.assertIsNone(self.server.Campaign_ended_at)
        self.assertNotIn("campaign_ended_at", self.runtime)
        self.assertEqual(self.runtime["campaign_status"], "starting")
        self.assertEqual(self.runtime["port"], 40028)

    def test_waiting_starting_and_running_campaigns_reject_overlap(self):
        self.begin()
        with self.assertRaises(ValueError):
            self.begin()
        self.server.Campaign_status = "running"
        self.server.FLSeReady = True
        with self.assertRaises(ValueError):
            self.begin()

    def test_stale_finish_or_stop_cannot_touch_new_campaign(self):
        self.begin()
        for action in [lambda: self.finish(run_id="old-run"),
                       lambda: lifecycle.stop(self.server, self.runtime, self.clients, "task", run_id="old-run")]:
            with self.assertRaises(ValueError):
                action()
        self.assertEqual(self.server.Campaign_status, "starting")

    def test_explicit_stop_is_not_completed_by_finally_callback(self):
        self.begin()
        lifecycle.stop(self.server, self.runtime, self.clients, "task", run_id="run-a", phase="request")
        self.finish()
        self.assertEqual(self.server.Campaign_status, "stopping")
        self.assertIsNone(self.server.Campaign_ended_at)
        with self.assertRaises(ValueError):
            self.begin()
        lifecycle.stop(self.server, self.runtime, self.clients, "task", run_id="run-a")
        self.finish()
        self.assertEqual(self.server.Campaign_status, "stopped")
        self.assertFalse(self.server.FLSeReady)

    def test_stop_after_success_preserves_completed(self):
        self.begin()
        self.finish()
        ended = self.server.Campaign_ended_at
        lifecycle.stop(self.server, self.runtime, self.clients, "task", run_id="run-a", phase="request")
        lifecycle.stop(self.server, self.runtime, self.clients, "task", run_id="run-a")
        self.assertEqual(self.server.Campaign_status, "completed")
        self.assertEqual(self.server.Campaign_ended_at, ended)

    def test_stop_after_manager_restart_binds_only_unknown_identity(self):
        lifecycle.stop(self.server, self.runtime, self.clients, "task", run_id="run-a", phase="request")
        self.assertEqual(self.server.Campaign_run_id, "run-a")
        self.assertEqual(self.server.Campaign_status, "stopping")
        with self.assertRaises(ValueError):
            lifecycle.stop(self.server, self.runtime, self.clients, "task", run_id="run-b", phase="request")
        lifecycle.stop(self.server, self.runtime, self.clients, "task", run_id="run-a")
        self.assertEqual(self.server.Campaign_status, "stopped")

    def test_legacy_completion_does_not_double_increment_on_retry(self):
        self.finish()
        self.finish()
        self.assertEqual(self.server.GL_Model_V, 5)

    def test_http_callbacks_do_not_reopen_terminal_or_stopping_campaign(self):
        # Exercise the real endpoint bodies without creating a Kubernetes client.
        path = Path(__file__).resolve().parents[2] / "app.py"
        selected = {"update_status", "update_ready", "scalable_server_running", "end_campaign"}
        tree = ast.parse(path.read_text())
        body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in selected]
        app = NS(put=lambda *_: lambda f: f, post=lambda *_: lambda f: f)
        scope = dict(app=app, campaign_lifecycle=lifecycle, Optional=Optional,
                     HTTPException=HTTPException, ServerStatus=NS, EndCampaignRequest=NS,
                     FLSe_dict={"task": self.server}, fl_server_status={"task": self.runtime},
                     FL_task_list=self.clients, get_or_create_FLSe=lambda _: self.server,
                     ensure_runtime_status=lambda _: self.runtime)
        exec(compile(ast.Module(body=body, type_ignores=[]), str(path), "exec"), scope)
        self.server.to_json = lambda: {}
        self.begin()
        self.finish()
        for name, args in [("update_status", (NS(Campaign_run_id="run-a", FLSeReady=True),)),
                           ("update_ready", (True,)), ("scalable_server_running", ())]:
            scope[name]("task", *args)
            self.assertFalse(self.server.FLSeReady)
            self.assertEqual(self.runtime["status"], "FL Server Finished")
        with self.assertRaises(HTTPException) as error:
            scope["update_ready"]("task", False, "old-run")
        self.assertEqual(error.exception.status_code, 409)
        self.request.runId = "run-b"
        self.begin()
        scope["end_campaign"]("task", NS(runId="run-b", phase="request"))
        scope["update_ready"]("task", False)
        self.assertEqual(self.server.Campaign_status, "stopping")
        scope["end_campaign"]("task", NS(runId="run-b", phase="finished"))
        self.assertEqual(self.server.Campaign_status, "stopped")


if __name__ == "__main__":
    unittest.main()
