# M10 Multi-Host Backup and Failover Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Deliver cross-host backup, primary/standby failover orchestration, and machine-verifiable DR evidence so the repo can close the current `M10` scope.

**Architecture:** Extend the existing single-host prodlike stack into a two-host topology with explicit primary/standby role metadata, scripted failover, and evidence verification gates. Keep current single-host paths backward compatible while adding new commands, contracts, and tests for multi-host failover drills.

**Tech Stack:** Docker Compose, Bash, Python 3, pytest, existing `scripts/infra/*` and `scripts/ops/*` toolchain, CI workflow gates.

### Task 1: Multi-Host Infra Asset Baseline

**Files:**
- Create: `infra/docker-compose.prodlike.multi-host.yaml`
- Create: `infra/env/prodlike-primary.env`
- Create: `infra/env/prodlike-standby.env`
- Test: `python/tests/test_prodlike_multi_host_infra_assets.py`

**Step 1: Write the failing test**
Add `test_prodlike_multi_host_infra_assets.py` to assert required files exist and include required service keys (`redis`, `timescaledb`, `prometheus`, `alertmanager`, `loki`, `tempo`, `grafana`).

**Step 2: Run test to verify it fails**
Run: `pytest python/tests/test_prodlike_multi_host_infra_assets.py -q`
Expected: FAIL due to missing files.

**Step 3: Write minimal implementation**
Create the three infra files with minimal valid content and explicit `ROLE=primary|standby` metadata.

**Step 4: Run test to verify it passes**
Run: `pytest python/tests/test_prodlike_multi_host_infra_assets.py -q`
Expected: PASS.

**Step 5: Commit**
```bash
git add infra/docker-compose.prodlike.multi-host.yaml infra/env/prodlike-primary.env infra/env/prodlike-standby.env python/tests/test_prodlike_multi_host_infra_assets.py
git commit -m "feat(infra): add multi-host prodlike topology assets"
```

### Task 2: Multi-Host Bootstrap and Health Check

**Files:**
- Create: `scripts/infra/bootstrap_prodlike_multi_host.sh`
- Create: `scripts/infra/check_prodlike_multi_host_health.py`
- Test: `python/tests/test_bootstrap_prodlike_multi_host_script.py`
- Test: `python/tests/test_check_prodlike_multi_host_health_script.py`

**Step 1: Write the failing tests**
Add tests to validate:
- `--dry-run`/`--execute` behavior
- supported actions (`up|down|status`)
- health report JSON contains `host`, `role`, `service`, `status`.

**Step 2: Run tests to verify failure**
Run: `pytest python/tests/test_bootstrap_prodlike_multi_host_script.py python/tests/test_check_prodlike_multi_host_health_script.py -q`
Expected: FAIL (scripts missing).

**Step 3: Write minimal implementation**
Implement bootstrap shell entrypoint and Python health checker that parse compose/service snapshots and output JSON report to `docs/results/prodlike_multi_host_health_report.json`.

**Step 4: Run tests to verify pass**
Run: `pytest python/tests/test_bootstrap_prodlike_multi_host_script.py python/tests/test_check_prodlike_multi_host_health_script.py -q`
Expected: PASS.

**Step 5: Commit**
```bash
git add scripts/infra/bootstrap_prodlike_multi_host.sh scripts/infra/check_prodlike_multi_host_health.py python/tests/test_bootstrap_prodlike_multi_host_script.py python/tests/test_check_prodlike_multi_host_health_script.py
git commit -m "feat(infra): add multi-host bootstrap and health checks"
```

### Task 3: Failover Orchestrator (Primary -> Standby)

**Files:**
- Create: `scripts/ops/failover_orchestrator.py`
- Create: `configs/deploy/environments/prodlike_multi_host.yaml`
- Test: `python/tests/test_failover_orchestrator_script.py`
- Modify: `scripts/ops/rollout_orchestrator.py` (shared helper extraction only if needed)

**Step 1: Write the failing test**
Create tests for sequence correctness:
- precheck -> backup-sync-check -> demote-primary -> promote-standby -> verify
- failure in any step yields `FAILOVER_SUCCESS=false` and records `FAILOVER_FAILED_STEP`.

**Step 2: Run test to verify it fails**
Run: `pytest python/tests/test_failover_orchestrator_script.py -q`
Expected: FAIL.

**Step 3: Write minimal implementation**
Implement orchestrator with `--dry-run` default, `--execute` opt-in, and `.env` evidence output at `docs/results/failover_result.env`.

**Step 4: Run test to verify it passes**
Run: `pytest python/tests/test_failover_orchestrator_script.py -q`
Expected: PASS.

**Step 5: Commit**
```bash
git add scripts/ops/failover_orchestrator.py configs/deploy/environments/prodlike_multi_host.yaml python/tests/test_failover_orchestrator_script.py scripts/ops/rollout_orchestrator.py
git commit -m "feat(ops): add multi-host failover orchestrator"
```

### Task 4: Failover Evidence Verification Gate

**Files:**
- Create: `scripts/ops/verify_failover_evidence.py`
- Test: `python/tests/test_verify_failover_evidence_script.py`
- Modify: `.github/workflows/ci.yml`

**Step 1: Write the failing test**
Add tests for positive and negative evidence:
- required keys missing -> fail
- elapsed failover exceeds threshold -> fail
- success path with required keys -> pass.

**Step 2: Run test to verify it fails**
Run: `pytest python/tests/test_verify_failover_evidence_script.py -q`
Expected: FAIL.

**Step 3: Write minimal implementation**
Implement verifier with checks:
- `FAILOVER_SUCCESS`
- step statuses
- `FAILOVER_DURATION_SECONDS <= --max-failover-seconds`
- `DATA_SYNC_LAG_EVENTS <= --max-data-lag-events`.

**Step 4: Run test to verify it passes**
Run: `pytest python/tests/test_verify_failover_evidence_script.py -q`
Expected: PASS.

**Step 5: Commit**
```bash
git add scripts/ops/verify_failover_evidence.py python/tests/test_verify_failover_evidence_script.py .github/workflows/ci.yml
git commit -m "feat(ci): add failover evidence verification gate"
```

### Task 5: Runbook and Operator Drill Alignment

**Files:**
- Modify: `docs/SYSTEMD_DEPLOYMENT_RUNBOOK.md`
- Modify: `docs/K8S_DEPLOYMENT_RUNBOOK.md`
- Modify: `docs/WAL_RECOVERY_RUNBOOK.md`
- Modify: `develop/04-基础设施与运维/04-02-部署、灾备与持续集成方案.md`
- Modify: `develop/07-部署与运维/07-02-运维操作手册.md`

**Step 1: Write the failing doc-validation test**
Create or extend test `python/tests/test_runbook_failover_commands.py` to assert failover command snippets exist and point to new scripts.

**Step 2: Run test to verify it fails**
Run: `pytest python/tests/test_runbook_failover_commands.py -q`
Expected: FAIL.

**Step 3: Write minimal implementation**
Update runbooks with explicit drill flow:
- `bootstrap_prodlike_multi_host.sh`
- `failover_orchestrator.py`
- `verify_failover_evidence.py`
and update `develop` docs to move multi-host DR from “后续口径” to executed capability after evidence is available.

**Step 4: Run test to verify it passes**
Run: `pytest python/tests/test_runbook_failover_commands.py -q`
Expected: PASS.

**Step 5: Commit**
```bash
git add docs/SYSTEMD_DEPLOYMENT_RUNBOOK.md docs/K8S_DEPLOYMENT_RUNBOOK.md docs/WAL_RECOVERY_RUNBOOK.md develop/04-基础设施与运维/04-02-部署、灾备与持续集成方案.md develop/07-部署与运维/07-02-运维操作手册.md python/tests/test_runbook_failover_commands.py
git commit -m "docs(ops): align runbooks for multi-host failover drill"
```

### Task 6: Requirement Mapping and Convergence Gate Update

**Files:**
- Modify: `docs/requirements/develop_requirements.yaml`
- Modify: `scripts/build/verify_develop_requirements.py`
- Test: `python/tests/test_verify_develop_requirements_script.py`

**Step 1: Write the failing test**
Add test to assert new multi-host DR requirement IDs map to code/test/evidence paths.

**Step 2: Run test to verify it fails**
Run: `pytest python/tests/test_verify_develop_requirements_script.py -q`
Expected: FAIL due to missing mappings.

**Step 3: Write minimal implementation**
Add/adjust requirements for multi-host DR paths and keep completion-language checks compatible with final wording.

**Step 4: Run test to verify it passes**
Run: `pytest python/tests/test_verify_develop_requirements_script.py -q`
Expected: PASS.

**Step 5: Commit**
```bash
git add docs/requirements/develop_requirements.yaml scripts/build/verify_develop_requirements.py python/tests/test_verify_develop_requirements_script.py
git commit -m "chore(requirements): map multi-host DR deliverables"
```

### Task 7: Evidence Generation and Final Acceptance

**Files:**
- Create: `docs/results/failover_result.env`
- Create: `docs/results/failover_drill_report_<date>.md`
- Modify: `docs/results/final_capability_acceptance_<date>.md`
- Modify: `develop/00-未完成能力补齐路线图.md`
- Modify: `develop/00-实现对齐矩阵与缺口总览.md`

**Step 1: Write failing validation tests**
Add tests to assert evidence files and mandatory fields are present (or extend existing acceptance test module).

**Step 2: Run tests to verify failure**
Run: `pytest python/tests/test_final_acceptance_artifacts.py -q`
Expected: FAIL due to missing evidence.

**Step 3: Generate minimal passing evidence**
Run scripted drill in dry-run first, then execute mode in controlled env, then write structured report and final acceptance summary.

**Step 4: Run tests to verify pass**
Run:
- `pytest python/tests/test_final_acceptance_artifacts.py -q`
- `python3 scripts/build/verify_develop_requirements.py --requirements-file docs/requirements/develop_requirements.yaml --completion-language-report docs/results/develop_completion_language_report.json`
Expected: PASS for both.

**Step 5: Commit**
```bash
git add docs/results/failover_result.env docs/results/failover_drill_report_*.md docs/results/final_capability_acceptance_*.md develop/00-未完成能力补齐路线图.md develop/00-实现对齐矩阵与缺口总览.md
git commit -m "feat(dr): close M10 multi-host failover acceptance"
```

### Task 8: Full Gate Verification and Release Readiness

**Files:**
- Modify: `.github/workflows/release-package.yml` (if failover evidence gate is required in release)
- Modify: `docs/IMPLEMENTATION_PROGRESS.md`

**Step 1: Run full verification suite**
Run:
- `./scripts/build/bootstrap.sh`
- `ctest --test-dir build`
- `pytest`
- `ruff check python scripts`
- `black --check python scripts`
- `mypy python`

Expected: all PASS.

**Step 2: Verify develop convergence**
Run:
- `python3 scripts/build/verify_develop_requirements.py --requirements-file docs/requirements/develop_requirements.yaml --completion-language-report docs/results/develop_completion_language_report.json`
Expected: no missing mappings and no forbidden unfinished language.

**Step 3: Final commit**
```bash
git add .github/workflows/release-package.yml docs/IMPLEMENTATION_PROGRESS.md
git commit -m "chore(release): enforce multi-host DR readiness gates"
```

## Acceptance Scenarios (M10)

1. Primary host unavailable: standby promotion succeeds and evidence records precise failed step and recovery duration.
2. Data sync lag exceeds threshold: verifier fails with explicit lag metric and non-zero exit.
3. Drill in dry-run: orchestration produces simulated evidence without mutating host state.
4. Drill in execute mode: orchestration produces success evidence and follow-up health report.
5. Requirement mapping gate: all `develop/*.md` remain mapped and no unfinished-language terms remain.

## Public Interface Changes

1. New ops script contract: `scripts/ops/failover_orchestrator.py` with flags `--env-config`, `--output-file`, `--execute`.
2. New evidence contract: `docs/results/failover_result.env` keys (`FAILOVER_SUCCESS`, `FAILOVER_FAILED_STEP`, `FAILOVER_DURATION_SECONDS`, `DATA_SYNC_LAG_EVENTS`, step statuses).
3. New verification API: `scripts/ops/verify_failover_evidence.py` thresholds (`--max-failover-seconds`, `--max-data-lag-events`).

## Assumptions and Defaults

1. Keep existing single-host scripts fully backward compatible; multi-host flow is additive.
2. Multi-host drills default to `--dry-run`; destructive switch actions require explicit `--execute`.
3. Initial M10 target is primary/standby failover (not active-active); multi-active remains future scope.
4. CI executes contract and evidence checks in simulation mode; execute-mode drills run in controlled prodlike environment.
