"""Campaign state transitions, independent of Kubernetes and HTTP transport."""
from datetime import datetime, timezone


def now():
    return datetime.now(timezone.utc).isoformat()


def assert_campaign(server, run_id):
    if run_id and run_id != server.Campaign_run_id:
        raise ValueError("Campaign changed; refusing a stale lifecycle update")


def begin(server, runtime, request, clients, task_id):
    terminal = bool(server.Campaign_ended_at) or server.Campaign_status in {"completed", "stopped", "failed"}
    if server.Campaign_run_id and not terminal and (
        server.FLSeReady or server.Campaign_status in {"starting", "running", "stopping"}
        or runtime.get("status") in {"Campaign Starting", "FL Server Running", "FL Server Stopping"}
    ):
        raise ValueError("Stop the active Federated Learning Campaign before starting another one.")
    started = now()
    server.FLServer_start = started
    server.FLSeReady = False
    server.GL_Model_V = request.targetGlobalModelVersion
    server.Task_status = None
    server.Campaign_run_id = request.runId
    server.Base_GL_Model_V = request.baseGlobalModelVersion
    server.Target_GL_Model_V = request.targetGlobalModelVersion
    server.Campaign_config = request.campaign.dict()
    server.Campaign_started_at = started
    server.Campaign_ended_at = None
    server.Campaign_status = "starting"
    runtime.pop("campaign_ended_at", None)
    runtime.update({
        "status": "Campaign Starting", "campaign_status": "starting",
        "campaign_run_id": request.runId, "campaign_release_id": request.releaseId,
        "base_global_model_version": request.baseGlobalModelVersion,
        "target_global_model_version": request.targetGlobalModelVersion,
        "campaign_config": request.campaign.dict(), "campaign_started_at": started,
    })
    for item in clients:
        if item.FL_task_ID == task_id:
            item.Device_training = False


def finish(server, runtime, clients, task_id, *, run_id=None):
    assert_campaign(server, run_id)
    server.FLSeReady = False
    # A delayed finally callback after an explicit Stop is not successful training.
    if server.Campaign_status in {"stopping", "stopped", "failed"}:
        return
    already_finished = runtime.get("status") == "FL Server Finished"
    runtime["status"] = "FL Server Finished"
    if server.Campaign_run_id:
        server.GL_Model_V = server.Target_GL_Model_V or server.GL_Model_V
        server.Campaign_ended_at = server.Campaign_ended_at or now()
        server.Campaign_status = "completed"
        runtime.update(campaign_ended_at=server.Campaign_ended_at, campaign_status="completed")
    elif not already_finished:
        server.GL_Model_V += 1
    for item in clients:
        if item.FL_task_ID == task_id:
            item.Device_training = False


def stop(server, runtime, clients, task_id, *, run_id, phase="finished"):
    # Manager restarts recover the Pod allocation, not process-local Campaign
    # metadata. An authorized Web Stop may bind its current run in that case.
    # Never replace a known identity (which could belong to a newer Campaign).
    if not server.Campaign_run_id and phase == "request":
        server.Campaign_run_id = run_id
    assert_campaign(server, run_id)
    server.FLSeReady = False
    if server.Campaign_status != "completed" and not server.Campaign_ended_at:
        server.Campaign_status = "stopping" if phase == "request" else "stopped"
        runtime.update(status="FL Server Stopping" if phase == "request" else "FL Server Stopped",
                       campaign_status=server.Campaign_status)
        if phase != "request":
            server.Campaign_ended_at = now()
            runtime["campaign_ended_at"] = server.Campaign_ended_at
    for item in clients:
        if item.FL_task_ID == task_id:
            item.Device_training = False
