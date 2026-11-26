#!/usr/bin/env python3
"""
Run a BlazeMeter performance test from CI and fail the job when its failure criteria fail.

    python blazemeter_pipeline.py validate          check config and API credentials
    python blazemeter_pipeline.py create-test       resolve TEST_ID, or find/create TEST_NAME
    python blazemeter_pipeline.py run-test          upload files, apply load profile, start
    python blazemeter_pipeline.py monitor-test      wait for the run to end
    python blazemeter_pipeline.py collect-results   download reports and logs, decide pass/fail
    python blazemeter_pipeline.py generate-report   write performance_report.html
    python blazemeter_pipeline.py cleanup           remove state files
    python blazemeter_pipeline.py all               all of the above in one process

--force-create makes create-test create a new test (or duplicate TEST_ID) instead of reusing one.
Stages share state through files in OUTPUT_DIR (default ./blazemeter). Settings come from
environment variables or a .env file; see .env.example.

Exit codes: 0 passed, 1 failure criteria violated or run did not end cleanly,
2 configuration or API error.
"""

import argparse
import html
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # .env support is optional; CI can use plain variables
    pass

EXIT_OK, EXIT_TEST_FAILED, EXIT_ERROR = 0, 1, 2
ENDED_PROGRESS = 140  # BlazeMeter master progress code for ENDED


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


class Config:
    def __init__(self) -> None:
        self.api_key = env("BZM_API_KEY")
        self.api_url = env("BZM_API_URL", "https://a.blazemeter.com/api/v4").rstrip("/")
        self.output_dir = Path(env("OUTPUT_DIR", "blazemeter"))

        self.test_name = env("TEST_NAME")
        self.test_id = env("TEST_ID")
        self.project_id = env("PROJECT_ID")

        self.duration = env("TEST_DURATION")
        self.concurrency = env("TEST_CONCURRENCY")
        self.ramp_up = env("TEST_RAMP_UP", "0")
        self.location_id = env("LOCATION_ID")

        self.test_file = env("TEST_FILE_PATH")
        self.data_files = [p.strip() for p in env("TEST_DATA_PATH").split(",") if p.strip()]

        self.poll_interval = int(env("MONITORING_INTERVAL", "30") or 30)
        self.monitor_timeout = env("MONITOR_TIMEOUT")
        self.verbose_monitoring = env_bool("VERBOSE_MONITORING", False)
        self.fail_on_no_criteria = env_bool("FAIL_ON_NO_CRITERIA", False)
        self.download_logs = env_bool("DOWNLOAD_LOGS", True)
        self.apply_overrides = env_bool("APPLY_OVERRIDES", True)

    def timeout_seconds(self) -> int:
        if self.monitor_timeout:
            return int(self.monitor_timeout)
        if self.duration:
            return int((float(self.duration) + float(self.ramp_up or 0)) * 60) + 900
        return 7200  # load profile unknown (APPLY_OVERRIDES=false and no TEST_DURATION)


CFG = Config()


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def log(message: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}", flush=True)


class PipelineError(Exception):
    """Configuration / API / infrastructure failure (exit code 2)."""


class BlazeMeter:
    def __init__(self, cfg: Config) -> None:
        if ":" not in cfg.api_key:
            raise PipelineError("BZM_API_KEY must be in the form <key_id>:<key_secret>")
        key_id, key_secret = cfg.api_key.split(":", 1)
        self.base = cfg.api_url
        self.http = requests.Session()
        self.http.auth = (key_id, key_secret)
        self.http.headers.update({"Accept": "application/json"})
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "PATCH", "PUT", "DELETE"]),  # not POST: start must not double-fire
            raise_on_status=False,
        )
        self.http.mount("https://", HTTPAdapter(max_retries=retry))

    def request(self, method: str, path: str, *, expect_json: bool = True, **kwargs) -> Any:
        url = path if path.startswith("http") else f"{self.base}{path}"
        kwargs.setdefault("timeout", 60)
        resp = self.http.request(method, url, **kwargs)
        if resp.status_code >= 400:
            raise PipelineError(f"{method} {path} -> HTTP {resp.status_code}: {resp.text[:500]}")
        if not expect_json:
            return resp
        try:
            body = resp.json()
        except ValueError:
            raise PipelineError(f"{method} {path} returned non-JSON: {resp.text[:300]}")
        if body.get("error"):
            raise PipelineError(f"{method} {path} API error: {body['error']}")
        return body.get("result")

    def get(self, path: str, **kw) -> Any:
        return self.request("GET", path, **kw)

    def post(self, path: str, **kw) -> Any:
        return self.request("POST", path, **kw)

    def patch(self, path: str, **kw) -> Any:
        return self.request("PATCH", path, **kw)


def out(name: str) -> Path:
    CFG.output_dir.mkdir(parents=True, exist_ok=True)
    return CFG.output_dir / name


def write_json(name: str, data: Any) -> None:
    out(name).write_text(json.dumps(data, indent=2))


def save_state(filename: str, data: Dict[str, Any]) -> None:
    with open(out(filename), "w") as f:
        for key, value in data.items():
            f.write(f"{key}={value}\n")
            os.environ[key] = str(value)


def load_state(filename: str) -> Dict[str, str]:
    path = CFG.output_dir / filename
    data: Dict[str, str] = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                key, value = line.split("=", 1)
                data[key] = value
                os.environ[key] = value
    return data


def require_state(*keys: str) -> Dict[str, str]:
    for f in ("test_id.env", "session_id.env", "test_status.env"):
        load_state(f)
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise PipelineError(f"Missing state {missing}. Run the earlier stage(s) first "
                            f"and make sure {CFG.output_dir}/ is passed as a job artifact.")
    return {k: os.environ[k] for k in keys}


# --------------------------------------------------------------------------- #
# Stage 1: validate
# --------------------------------------------------------------------------- #
def validate_inputs() -> bool:
    log("=== Stage 1: Validating BlazeMeter configuration ===")
    CFG.output_dir.mkdir(parents=True, exist_ok=True)
    errors: List[str] = []

    if not CFG.api_key:
        errors.append("BZM_API_KEY is not set")
    elif ":" not in CFG.api_key:
        errors.append("BZM_API_KEY must be <key_id>:<key_secret>")
    if not CFG.test_id and not CFG.test_name:
        errors.append("Either TEST_ID or TEST_NAME must be provided")
    if not CFG.project_id:
        errors.append("PROJECT_ID is not set")

    if not CFG.apply_overrides:
        log("APPLY_OVERRIDES=false: test will run as configured in BlazeMeter "
            "(no file uploads, load settings ignored)")
        if not CFG.test_id:
            errors.append("APPLY_OVERRIDES=false requires TEST_ID (an existing, fully configured test)")

    for name, val, allow_zero in (("TEST_DURATION", CFG.duration, False),
                                  ("TEST_CONCURRENCY", CFG.concurrency, False),
                                  ("TEST_RAMP_UP", CFG.ramp_up or "0", True)):
        if not val:
            if CFG.apply_overrides:
                errors.append(f"{name} is not set")
            continue
        try:
            num = float(val)
            if num < 0 or (num == 0 and not allow_zero):
                raise ValueError
        except ValueError:
            errors.append(f"{name} must be a positive number (got {val!r})")
    if CFG.concurrency and not CFG.concurrency.isdigit():
        errors.append("TEST_CONCURRENCY must be an integer")

    if CFG.test_file and not Path(CFG.test_file).is_file():
        errors.append(f"TEST_FILE_PATH not found: {CFG.test_file}")
    for p in CFG.data_files:
        if not Path(p).is_file():
            errors.append(f"TEST_DATA_PATH file not found: {p}")
    if CFG.apply_overrides and not CFG.test_id and not CFG.test_file:
        errors.append("TEST_FILE_PATH is required when TEST_ID is not set (a new test needs a script)")

    if errors:
        for e in errors:
            log(f"ERROR: {e}")
        return False

    # Fail fast on bad credentials rather than at the run stage.
    try:
        user = BlazeMeter(CFG).get("/user")
        log(f"Authenticated as {user.get('email', 'unknown user')}")
    except PipelineError as e:
        log(f"ERROR: API authentication check failed: {e}")
        return False

    log("Validation completed successfully")
    return True


# --------------------------------------------------------------------------- #
# Stage 2: create (or reuse) test
# --------------------------------------------------------------------------- #
def find_test_by_name(bzm: BlazeMeter, name: str) -> Optional[str]:
    params = {"projectId": CFG.project_id, "limit": 1000}
    for test in bzm.get("/tests", params=params) or []:
        if test.get("name") == name:
            return str(test["id"])
    return None


def create_test(force_create: bool = False) -> str:
    log("=== Stage 2: Creating / resolving BlazeMeter test ===")
    bzm = BlazeMeter(CFG)

    if CFG.test_id and not force_create:
        log(f"Using existing test ID: {CFG.test_id}")
        save_state("test_id.env", {"TEST_ID": CFG.test_id})
        return CFG.test_id

    if CFG.test_id and force_create:
        # Duplicate keeps scripts, locations and failure criteria of the source test.
        log(f"Duplicating test {CFG.test_id}...")
        result = bzm.post(f"/tests/{CFG.test_id}/duplicate")
        new_id = str(result["id"])
        if CFG.test_name:
            bzm.patch(f"/tests/{new_id}", json={"name": CFG.test_name})
        log(f"Duplicated test -> {new_id}")
        save_state("test_id.env", {"TEST_ID": new_id})
        return new_id

    # Idempotent: reuse a test with the same name in the project instead of piling up duplicates.
    if not force_create:
        existing = find_test_by_name(bzm, CFG.test_name)
        if existing:
            log(f"Found existing test '{CFG.test_name}' with ID {existing}")
            save_state("test_id.env", {"TEST_ID": existing})
            return existing

    script_name = Path(CFG.test_file).name
    script_type = "jmeter" if script_name.lower().endswith(".jmx") else "taurus"
    payload = {
        "name": CFG.test_name,
        "projectId": int(CFG.project_id),
        "configuration": {
            "type": "taurus",
            "filename": script_name,
            "testMode": "script",
            "scriptType": script_type,
        },
    }
    write_json("create_payload.json", payload)
    log(f"Creating test '{CFG.test_name}' (script {script_name}, type {script_type})")
    result = bzm.post("/tests", json=payload)
    write_json("create_response.json", result)

    test_id = str(result["id"])
    log(f"Created test with ID: {test_id}")
    save_state("test_id.env", {"TEST_ID": test_id})
    return test_id


# --------------------------------------------------------------------------- #
# Stage 3: upload files, apply overrides, start
# --------------------------------------------------------------------------- #
def upload_file(bzm: BlazeMeter, test_id: str, path: str) -> None:
    log(f"Uploading {path}")
    with open(path, "rb") as fh:
        # Re-uploading a file with the same name replaces it; no need to delete first.
        bzm.post(f"/tests/{test_id}/files", files={"file": (Path(path).name, fh)}, timeout=300)


def split_by_percent(total: int, percents: Dict[str, Any]) -> Dict[str, int]:
    """Spread `total` VUs over locations by percentage; the counts always add up to `total`."""
    pct = {loc: float(p or 0) for loc, p in percents.items()}
    weight = sum(pct.values()) or 1.0
    exact = {loc: total * p / weight for loc, p in pct.items()}
    counts = {loc: int(v) for loc, v in exact.items()}
    # Hand the remainder to the locations with the largest fractional parts.
    for loc in sorted(exact, key=lambda l: exact[l] - counts[l], reverse=True)[:total - sum(counts.values())]:
        counts[loc] += 1
    return counts


def build_override(current: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    concurrency = int(CFG.concurrency)
    override: Dict[str, Any] = {
        "concurrency": concurrency,
        "holdFor": f"{CFG.duration}m",
        "rampUp": f"{CFG.ramp_up or 0}m",
    }
    if CFG.location_id:
        override["locations"] = {CFG.location_id: concurrency}
        override["locationsPercents"] = {CFG.location_id: 100}
    elif current and current.get("locationsPercents"):
        # BlazeMeter stores `locations` (VU counts) verbatim, so keep the test's location split
        # but rescale the counts to the new concurrency; stale counts would not match it.
        override["locationsPercents"] = current["locationsPercents"]
        override["locations"] = split_by_percent(concurrency, current["locationsPercents"])
    return override


def run_test(test_id: str) -> str:
    log("=== Stage 3: Running BlazeMeter test ===")
    bzm = BlazeMeter(CFG)

    if CFG.apply_overrides:
        apply_test_overrides(bzm, test_id)
    else:
        log("APPLY_OVERRIDES=false: skipping uploads and load overrides; "
            "running the test as configured in BlazeMeter")

    result = bzm.post(f"/tests/{test_id}/start", params={"delayedStart": "false"}, json={})
    write_json("start_response.json", result)

    master_id = str(result["id"])  # first id in the start response is the masterId
    sessions = result.get("sessionsId") or []
    log(f"Test started. Master ID: {master_id}, sessions: {sessions}")
    log(f"Report: https://a.blazemeter.com/app/#/masters/{master_id}")
    save_state("session_id.env", {
        "TEST_ID": test_id,
        "MASTER_ID": master_id,
        "SESSION_IDS": ",".join(sessions),
    })
    return master_id


def apply_test_overrides(bzm: BlazeMeter, test_id: str) -> None:
    """Upload script/data files and apply the load profile from env vars."""
    if CFG.test_file:
        upload_file(bzm, test_id, CFG.test_file)
    for p in CFG.data_files:
        upload_file(bzm, test_id, p)

    test = bzm.get(f"/tests/{test_id}")
    executions = test.get("overrideExecutions") or test.get("executions") or [{}]
    override = build_override(executions[0])
    # Keep existing per-execution keys (executor, scenario...) and override load shape.
    merged = [{**(executions[0] or {}), **override}] + list(executions[1:])
    patch: Dict[str, Any] = {"overrideExecutions": merged}
    if CFG.test_file:
        patch["configuration"] = {**(test.get("configuration") or {}),
                                  "filename": Path(CFG.test_file).name}
    write_json("update_payload.json", patch)
    bzm.patch(f"/tests/{test_id}", json=patch)
    log(f"Applied load profile: {json.dumps(override)}")


# --------------------------------------------------------------------------- #
# Stage 4: monitor
# --------------------------------------------------------------------------- #
_ACTIVE_MASTER: Optional[str] = None


def stop_master(master_id: str, reason: str) -> None:
    log(f"Stopping master {master_id} ({reason})")
    try:
        BlazeMeter(CFG).post(f"/masters/{master_id}/stop")
    except Exception as e:  # best effort
        log(f"WARNING: failed to stop master {master_id}: {e}")


def _on_sigterm(signum, frame):  # GitLab sends SIGTERM when a job is cancelled
    if _ACTIVE_MASTER:
        stop_master(_ACTIVE_MASTER, "job cancelled")
    sys.exit(EXIT_ERROR)


def monitor_test(master_id: str) -> str:
    global _ACTIVE_MASTER
    log("=== Stage 4: Monitoring test execution ===")
    bzm = BlazeMeter(CFG)
    _ACTIVE_MASTER = master_id
    signal.signal(signal.SIGTERM, _on_sigterm)

    deadline = time.monotonic() + CFG.timeout_seconds()
    consecutive_errors, max_errors = 0, 10
    status, last = "UNKNOWN", None

    try:
        while True:
            if time.monotonic() > deadline:
                stop_master(master_id, "monitor timeout")
                save_state("test_status.env", {"FINAL_STATUS": "TIMEOUT"})
                raise PipelineError(f"Test did not finish within {CFG.timeout_seconds()}s")
            try:
                result = bzm.get(f"/masters/{master_id}/status")
                consecutive_errors = 0
            except (PipelineError, requests.RequestException) as e:
                consecutive_errors += 1
                log(f"WARNING: status poll failed ({consecutive_errors}/{max_errors}): {e}")
                if consecutive_errors >= max_errors:
                    raise PipelineError("Too many consecutive status poll failures")
                time.sleep(CFG.poll_interval)
                continue

            status = result.get("status", "UNKNOWN")
            progress = result.get("progress", 0)
            if (status, progress) != last:
                log(f"Status: {status} (progress code {progress})")
                last = (status, progress)
            if CFG.verbose_monitoring:
                log(json.dumps(result, indent=2))

            if status == "ENDED" or (isinstance(progress, int) and progress >= ENDED_PROGRESS):
                write_json("final_status.json", result)
                break
            time.sleep(CFG.poll_interval)
    except KeyboardInterrupt:
        stop_master(master_id, "interrupted")
        raise
    finally:
        _ACTIVE_MASTER = None

    log(f"Test completed with status: {status}")
    save_state("test_status.env", {"FINAL_STATUS": status})
    return status


# --------------------------------------------------------------------------- #
# Stage 5: collect results
# --------------------------------------------------------------------------- #
def _try(label: str, fn):
    try:
        value = fn()
        log(f"Collected {label}")
        return value
    except Exception as e:
        log(f"WARNING: failed to collect {label}: {e}")
        return None


def collect_results(master_id: str, session_ids: List[str], final_status: str) -> bool:
    log("=== Stage 5: Collecting test results ===")
    bzm = BlazeMeter(CFG)

    summary = _try("summary", lambda: bzm.get(f"/masters/{master_id}/reports/default/summary"))
    if summary is not None:
        write_json("test_summary.json", summary)

    aggregate = _try("aggregate report",
                     lambda: bzm.get(f"/masters/{master_id}/reports/aggregatereport/data"))
    if aggregate is not None:
        write_json("test_aggregate.json", aggregate)

    def get_thresholds() -> Dict[str, Any]:
        try:
            return bzm.get(f"/masters/{master_id}/reports/thresholds")
        except PipelineError as e:
            if "HTTP 404" in str(e):  # "No such report": the test has no failure criteria
                return {"data": []}
            raise

    thresholds = _try("failure criteria", get_thresholds)
    if thresholds is not None:
        write_json("test_sla.json", thresholds)

    ci_status = _try("CI status", lambda: bzm.get(f"/masters/{master_id}/ci-status"))
    if ci_status is not None:
        write_json("ci_status.json", ci_status)

    # The master's reportStatus is the only place an aborted or errored run shows up:
    # ci-status reports those as status "success" with empty errors/failures.
    master = _try("master details", lambda: bzm.get(f"/masters/{master_id}"))
    if master is not None:
        write_json("master.json", master)

    # Built locally: /reports/thresholds?format=junit returns a fixed placeholder document
    # (tests="0", 2013 timestamp) whatever the run's results are.
    labels = {a.get("labelId"): a.get("labelName") for a in aggregate or [] if isinstance(a, dict)}
    out("junit.xml").write_text(build_junit(master_id, final_status, thresholds, master, labels, ci_status))

    if CFG.download_logs:
        for sid in session_ids:
            files = _try(f"log index for {sid}", lambda sid=sid: bzm.get(f"/sessions/{sid}/reports/logs"))
            if not files:
                continue
            write_json(f"logs_{sid}.json", files)
            logs_dir = out(f"logs/{sid}")
            logs_dir.mkdir(parents=True, exist_ok=True)
            for item in files.get("data", []) if isinstance(files, dict) else []:
                url, name = item.get("dataUrl"), item.get("filename")
                if not url or not name:
                    continue
                try:
                    # dataUrl is pre-signed; don't send BlazeMeter credentials to it.
                    r = requests.get(url, timeout=120)
                    r.raise_for_status()
                    (logs_dir / Path(name).name).write_bytes(r.content)
                except Exception as e:
                    log(f"WARNING: could not download {name}: {e}")

    return evaluate(final_status, ci_status, master, thresholds)


OPS = {"gt": ">", "lt": "<", "eq": "=", "ne": "!="}


def criterion_violated(c: Dict[str, Any]) -> bool:
    return bool(c.get("violations")) or c.get("success") is False


def criterion_name(c: Dict[str, Any], labels: Dict[str, str]) -> str:
    label = labels.get(c.get("labelId")) or c.get("labelId") or "ALL"
    return f"{c.get('field')} {OPS.get(c.get('op'), c.get('op'))} {c.get('threshold')} [{label}]"


def build_junit(master_id: str, final_status: str, thresholds: Optional[Dict[str, Any]],
                master: Optional[Dict[str, Any]], labels: Dict[str, str],
                ci_status: Optional[Dict[str, Any]] = None) -> str:
    """One testcase for the run itself, plus one per failure criterion."""
    # ci-status failure details look like "ALL: responseTime.avg gt 1 failed with:183.23 "
    ci_details = [f.get("details") or f.get("message") or ""
                  for f in (ci_status or {}).get("failures") or [] if isinstance(f, dict)]
    q = lambda v: html.escape(str(v), quote=True)
    cases: List[str] = []
    report_status = (master or {}).get("reportStatus")
    run_problem = None
    if final_status != "ENDED":
        run_problem = f"Test finished with status {final_status}"
    elif report_status in ("abort", "error"):
        run_problem = f"BlazeMeter reportStatus is '{report_status}'"
    cases.append(f'  <testcase classname="blazemeter.run" name="test run (master {q(master_id)})">'
                 + (f'<failure message="{q(run_problem)}"/>' if run_problem else "") + "</testcase>")

    for c in (thresholds or {}).get("data") or []:
        name = criterion_name(c, labels)
        body = ""
        if criterion_violated(c):
            key = f"{c.get('field')} {c.get('op')} {c.get('threshold')} "
            detail = next((d.strip() for d in ci_details if key in d + " "), "")
            values = [v.get("value") for v in c.get("violations") or []]
            msg = f"Failure criterion violated: {name}"
            if detail:
                msg += f" ({detail})"
            elif values:
                msg += f"; measured {values}"
            text = json.dumps(c.get("violations"), indent=1) if values else detail
            body = f'<failure message="{q(msg)}">{q(text)}</failure>'
        cases.append(f'  <testcase classname="blazemeter.criteria" name="{q(name)}">{body}</testcase>')

    failures = sum("<failure" in c for c in cases)
    return ('<?xml version="1.0" encoding="UTF-8"?>\n'
            f'<testsuites><testsuite name="BlazeMeter failure criteria" tests="{len(cases)}" '
            f'failures="{failures}" errors="0" timestamp="{datetime.now():%Y-%m-%dT%H:%M:%S}">\n'
            + "\n".join(cases) + "\n</testsuite></testsuites>\n")


def evaluate(final_status: str, ci_status: Optional[Dict[str, Any]],
             master: Optional[Dict[str, Any]] = None,
             thresholds: Optional[Dict[str, Any]] = None) -> bool:
    if final_status != "ENDED":
        log(f"FAIL: test finished with status {final_status}")
        return False
    report_status = (master or {}).get("reportStatus")
    if report_status in ("abort", "error"):
        log(f"FAIL: BlazeMeter reportStatus is '{report_status}' (run was stopped or errored)")
        return False
    if ci_status is None and master is None:
        log("WARNING: CI status unavailable; cannot evaluate failure criteria")
        return not CFG.fail_on_no_criteria
    errors = (ci_status or {}).get("errors") or []
    failures = (ci_status or {}).get("failures") or []
    if errors:
        log(f"FAIL: test errors: {json.dumps(errors)[:1000]}")
        return False
    if failures:
        log(f"FAIL: failure criteria violated: {json.dumps(failures)[:1000]}")
        return False
    if report_status == "fail" or (master or {}).get("passed") is False:
        log("FAIL: BlazeMeter marked the run as failed (reportStatus 'fail')")
        return False
    violated = [c for c in (thresholds or {}).get("data") or [] if criterion_violated(c)]
    if violated:
        log(f"FAIL: {len(violated)} failure criteria violated")
        return False
    if not (thresholds or {}).get("data"):
        if CFG.fail_on_no_criteria:
            log("FAIL: no failure criteria defined on the test")
            return False
        log("PASS: test ended normally (no failure criteria defined)")
        return True
    log("PASS: test met its failure criteria")
    return True


# --------------------------------------------------------------------------- #
# Stage 6: HTML report
# --------------------------------------------------------------------------- #
def _read_json(name: str) -> Any:
    p = CFG.output_dir / name
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except ValueError:
        return {"raw": p.read_text()[:5000]}


def generate_report(test_id: str, master_id: str, final_status: str, passed: Optional[bool]) -> None:
    log("=== Stage 6: Generating HTML report ===")
    esc = lambda v: html.escape(str(v if v is not None else ""))
    block = lambda d: esc(json.dumps(d, indent=2))
    verdict = {True: "PASSED", False: "FAILED", None: "UNKNOWN"}[passed]
    if CFG.apply_overrides:
        load_desc = esc(f"{CFG.concurrency} VUs, {CFG.ramp_up}m ramp-up, {CFG.duration}m hold")
    else:
        load_desc = "As configured in BlazeMeter (APPLY_OVERRIDES=false)"

    page = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>BlazeMeter Performance Test Report</title>
<style>
 body {{ font-family: Arial, sans-serif; margin: 20px; }}
 .header {{ background:#f0f0f0; padding:20px; border-radius:5px; }}
 .section {{ margin:20px 0; padding:15px; border:1px solid #ddd; border-radius:5px; }}
 pre {{ background:#f9f9f9; padding:10px; border-radius:3px; overflow-x:auto; max-height:600px; }}
 .PASSED {{ color:#1a7f37; }} .FAILED {{ color:#cf222e; }}
</style></head><body>
<div class="header">
 <h1>BlazeMeter Performance Test Report</h1>
 <p><strong>Verdict:</strong> <span class="{verdict}">{verdict}</span></p>
 <p><strong>Test Name:</strong> {esc(CFG.test_name)} &nbsp; <strong>Test ID:</strong> {esc(test_id)}</p>
 <p><strong>Master ID:</strong> <a href="https://a.blazemeter.com/app/#/masters/{esc(master_id)}">{esc(master_id)}</a></p>
 <p><strong>Status:</strong> {esc(final_status)}</p>
 <p><strong>Load:</strong> {load_desc}</p>
 <p><strong>Generated:</strong> {datetime.now():%Y-%m-%d %H:%M:%S}</p>
</div>
<div class="section"><h2>CI Status</h2><pre>{block(_read_json("ci_status.json"))}</pre></div>
<div class="section"><h2>Summary</h2><pre>{block(_read_json("test_summary.json"))}</pre></div>
<div class="section"><h2>Failure Criteria</h2><pre>{block(_read_json("test_sla.json"))}</pre></div>
<div class="section"><h2>Aggregate Report</h2><pre>{block(_read_json("test_aggregate.json"))}</pre></div>
</body></html>"""
    out("performance_report.html").write_text(page)
    log(f"Report written to {out('performance_report.html')}")


# --------------------------------------------------------------------------- #
# Stage 7: cleanup
# --------------------------------------------------------------------------- #
def cleanup() -> None:
    log("=== Stage 7: Cleaning up state files ===")
    for name in ("test_id.env", "session_id.env", "test_status.env"):
        p = CFG.output_dir / name
        if p.exists():
            p.unlink()
            log(f"Removed: {p}")


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(description="BlazeMeter Performance Test Pipeline")
    parser.add_argument("stage", choices=["validate", "create-test", "run-test", "monitor-test",
                                          "collect-results", "generate-report", "cleanup", "all"])
    parser.add_argument("--force-create", action="store_true",
                        help="Create a new test (duplicating TEST_ID if set) instead of reusing one")
    args = parser.parse_args()

    log(f"Starting BlazeMeter pipeline stage '{args.stage}'")
    try:
        if args.stage == "validate":
            return EXIT_OK if validate_inputs() else EXIT_ERROR

        if args.stage == "create-test":
            if not validate_inputs():
                return EXIT_ERROR
            create_test(args.force_create)
            return EXIT_OK

        if args.stage == "run-test":
            if not CFG.test_id:
                require_state("TEST_ID")
            run_test(os.environ.get("TEST_ID") or CFG.test_id)
            return EXIT_OK

        if args.stage == "monitor-test":
            s = require_state("MASTER_ID")
            monitor_test(s["MASTER_ID"])
            return EXIT_OK

        if args.stage == "collect-results":
            s = require_state("MASTER_ID")
            passed = collect_results(s["MASTER_ID"], [x for x in env("SESSION_IDS").split(",") if x],
                                     env("FINAL_STATUS", "UNKNOWN"))
            save_state("test_status.env", {"FINAL_STATUS": env("FINAL_STATUS", "UNKNOWN"),
                                           "PASSED": str(passed).lower()})
            return EXIT_OK if passed else EXIT_TEST_FAILED

        if args.stage == "generate-report":
            s = require_state("TEST_ID", "MASTER_ID")
            p = env("PASSED")
            generate_report(s["TEST_ID"], s["MASTER_ID"], env("FINAL_STATUS", "UNKNOWN"),
                            None if not p else p == "true")
            return EXIT_OK

        if args.stage == "cleanup":
            cleanup()
            return EXIT_OK

        # all
        if not validate_inputs():
            return EXIT_ERROR
        test_id = create_test(args.force_create)
        master_id = run_test(test_id)
        final_status = monitor_test(master_id)
        sessions = [x for x in env("SESSION_IDS").split(",") if x]
        passed = collect_results(master_id, sessions, final_status)
        generate_report(test_id, master_id, final_status, passed)
        cleanup()
        log("Pipeline finished: " + ("PASSED" if passed else "FAILED"))
        return EXIT_OK if passed else EXIT_TEST_FAILED

    except PipelineError as e:
        log(f"ERROR: {e}")
        return EXIT_ERROR
    except KeyboardInterrupt:
        log("Pipeline interrupted by user")
        return EXIT_ERROR
    except Exception as e:
        log(f"ERROR: unexpected failure: {e}")
        import traceback
        traceback.print_exc()
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
