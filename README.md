# BlazeMeter performance tests in GitLab CI

Example pipeline that runs a BlazeMeter test from GitLab CI, waits for it to finish, publishes the results, and fails the pipeline when the test's failure criteria fail.

| File | Purpose |
|---|---|
| `blazemeter_pipeline.py` | The script. One subcommand per pipeline stage. |
| `.gitlab-ci.yml` | One job per stage, passing `blazemeter/` between jobs as an artifact. |
| `.env.example` | Every setting, with defaults for the sample test. |
| `tests/blazedemo_smoke.jmx` | Sample JMeter script: one GET against blazedemo.com. |

## Quick start (local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env    # set BZM_API_KEY and PROJECT_ID
python blazemeter_pipeline.py validate
python blazemeter_pipeline.py all
```

With the sample settings, this creates a test called "BlazeDemo smoke test" in your project (or reuses it) and runs 2 users for 2 minutes. Results go to `blazemeter/`.

## GitLab setup

1. Copy the files into your repository and put your own script in `tests/`.
2. Under Settings > CI/CD > Variables, either:
   - add `BZM_API_KEY` (masked, protected), `PROJECT_ID` and any other settings from `.env.example`, or
   - upload a filled-in `.env` as a **File** variable named `BZM_ENV_FILE` (protected).
3. Set the `monitor-test` job's `timeout` to longer than ramp-up + duration.
4. Add failure criteria to the test in BlazeMeter. They decide whether the pipeline passes.

## Choosing the test

- `TEST_ID` set: run that test.
- `TEST_ID` empty: reuse the test named `TEST_NAME` in `PROJECT_ID`, or create it from `TEST_FILE_PATH`.
- `create-test --force-create`: create a new test even if one with that name exists. With `TEST_ID` set, it duplicates that test, keeping its files and failure criteria.

By default (`APPLY_OVERRIDES=true`) every run uploads `TEST_FILE_PATH` and `TEST_DATA_PATH` and saves `TEST_DURATION`, `TEST_CONCURRENCY`, `TEST_RAMP_UP` and `LOCATION_ID` to the test. Without `LOCATION_ID`, the test keeps its location split and the users are spread across it.

To run a test exactly as it is configured in the BlazeMeter UI, set `TEST_ID` and `APPLY_OVERRIDES=false`. Nothing is uploaded or changed, and the load settings are not needed. The monitor then waits up to 2 hours; set `MONITOR_TIMEOUT` (seconds) to change that.

## Pass or fail

`collect-results` exits 1 if any of these is true:

- the run did not end (for example, the monitor timed out);
- BlazeMeter marked the run `abort` or `error`;
- a failure criterion was violated;
- the test has no failure criteria and `FAIL_ON_NO_CRITERIA=true`.

| Exit code | Meaning |
|---|---|
| 0 | Test passed |
| 1 | Failure criteria violated, or the run did not end cleanly |
| 2 | Configuration or API error |

To report violated criteria as a warning instead, uncomment `allow_failure: exit_codes: [1]` on `collect-results`.

Cancelling the GitLab job while `monitor-test` runs stops the test in BlazeMeter. A test stopped from the BlazeMeter UI still counts as ended, so it passes unless a failure criterion fails.

## Results (`blazemeter/`)

- `performance_report.html`: summary page, linked from the pipeline as "BlazeMeter results".
- `junit.xml`: shown in GitLab's Tests tab. It has one case for the run and one per failure criterion.
- `master.json`, `ci_status.json`, `test_summary.json`, `test_aggregate.json`, `test_sla.json`: raw API responses.
- `logs/<session_id>/`: engine logs (`bzt.log`, `jmeter.log`, `artifacts.zip`).

## API notes

- Creating a test with `POST /tests` needs a `configuration` block (`type`, `filename`, `testMode`, `scriptType`). Without it, the API returns HTTP 500.
- `GET /masters/{id}/reports/thresholds?format=junit` returns a fixed placeholder, so the script builds `junit.xml` itself.
- `GET /masters/{id}/ci-status` reports terminated runs as `success`. The script also checks the run's `reportStatus`.
