#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def _parse_env_file(path: Path) -> dict[str, str]:
    payload: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", maxsplit=1)
        payload[key.strip()] = value.strip()
    return payload


def _required_keys(payload: dict[str, str], keys: tuple[str, ...], label: str) -> None:
    missing = [key for key in keys if not payload.get(key, "").strip()]
    if missing:
        raise ValueError(f"{label} missing required keys: {','.join(missing)}")


def _build_markdown(cutover: dict[str, str], rollback: dict[str, str]) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()

    monitor_keys = [item.strip() for item in cutover["MONITOR_KEYS"].split(",") if item.strip()]
    monitor_lines = "\n".join(f"- [ ] {item}" for item in monitor_keys)

    return f"""# CTP One-Shot Cutover Checklist

Generated at: {generated_at}
Environment: {cutover['CUTOVER_ENV_NAME']}
Cutover window(local): {cutover['CUTOVER_WINDOW_LOCAL']}
CTP config: `{cutover['CTP_CONFIG_PATH']}`

## 1) Pre-Cutover (T-30min)

- [ ] Stop old core engine

```bash
{cutover['OLD_CORE_ENGINE_STOP_CMD']}
```

- [ ] Stop old strategy runner

```bash
{cutover['OLD_STRATEGY_RUNNER_STOP_CMD']}
```

- [ ] Precheck readiness evidence

```bash
{cutover['PRECHECK_CMD']}
```

- [ ] Bootstrap infra and initialize schemas/topics

```bash
{cutover['BOOTSTRAP_INFRA_CMD']}
{cutover['INIT_KAFKA_TOPIC_CMD']}
{cutover['INIT_CLICKHOUSE_SCHEMA_CMD']}
{cutover['INIT_DEBEZIUM_CONNECTOR_CMD']}
```

## 2) Cutover Window

- [ ] Start new core engine

```bash
{cutover['NEW_CORE_ENGINE_START_CMD']}
```

- [ ] Start new strategy runner

```bash
{cutover['NEW_STRATEGY_RUNNER_START_CMD']}
```

- [ ] Warmup query and settlement verification

```bash
{cutover['WARMUP_QUERY_CMD']}
```

## 3) Post-Cutover {cutover['POST_SWITCH_MONITOR_MINUTES']} Minutes Watch

{monitor_lines}

- [ ] Persist cutover evidence to:

`{cutover['CUTOVER_EVIDENCE_OUTPUT']}`

## 4) Rollback Drill Template

Trigger condition:
- `{rollback['ROLLBACK_TRIGGER_CONDITION']}`

- [ ] Stop new stack

```bash
{rollback['NEW_CORE_ENGINE_STOP_CMD']}
{rollback['NEW_STRATEGY_RUNNER_STOP_CMD']}
```

- [ ] Restore previous binaries and compat bridge

```bash
{rollback['RESTORE_PREVIOUS_BINARIES_CMD']}
{rollback['RESTORE_REDIS_BRIDGE_COMPAT_CMD']}
```

- [ ] Start previous stack

```bash
{rollback['PREVIOUS_CORE_ENGINE_START_CMD']}
{rollback['PREVIOUS_STRATEGY_RUNNER_START_CMD']}
```

- [ ] Validate rollback health

```bash
{rollback['POST_ROLLBACK_VALIDATE_CMD']}
```

- [ ] Rollback must finish within `{rollback['MAX_ROLLBACK_SECONDS']}` seconds.
- [ ] Persist rollback evidence to `{rollback['ROLLBACK_EVIDENCE_OUTPUT']}`.
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one-shot CTP cutover checklist from cutover/rollback env templates"
    )
    parser.add_argument("--cutover-template", required=True)
    parser.add_argument("--rollback-template", required=True)
    parser.add_argument("--output-md", required=True)
    parser.add_argument("--output-json", default="")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    cutover_path = Path(args.cutover_template)
    rollback_path = Path(args.rollback_template)
    if not cutover_path.exists():
        print(f"error: cutover template not found: {cutover_path}")
        return 2
    if not rollback_path.exists():
        print(f"error: rollback template not found: {rollback_path}")
        return 2

    cutover = _parse_env_file(cutover_path)
    rollback = _parse_env_file(rollback_path)

    cutover_required = (
        "CUTOVER_ENV_NAME",
        "CUTOVER_WINDOW_LOCAL",
        "CTP_CONFIG_PATH",
        "OLD_CORE_ENGINE_STOP_CMD",
        "OLD_STRATEGY_RUNNER_STOP_CMD",
        "BOOTSTRAP_INFRA_CMD",
        "INIT_KAFKA_TOPIC_CMD",
        "INIT_CLICKHOUSE_SCHEMA_CMD",
        "INIT_DEBEZIUM_CONNECTOR_CMD",
        "NEW_CORE_ENGINE_START_CMD",
        "NEW_STRATEGY_RUNNER_START_CMD",
        "PRECHECK_CMD",
        "WARMUP_QUERY_CMD",
        "POST_SWITCH_MONITOR_MINUTES",
        "MONITOR_KEYS",
        "CUTOVER_EVIDENCE_OUTPUT",
    )
    rollback_required = (
        "ROLLBACK_ENV_NAME",
        "ROLLBACK_TRIGGER_CONDITION",
        "NEW_CORE_ENGINE_STOP_CMD",
        "NEW_STRATEGY_RUNNER_STOP_CMD",
        "RESTORE_PREVIOUS_BINARIES_CMD",
        "RESTORE_REDIS_BRIDGE_COMPAT_CMD",
        "PREVIOUS_CORE_ENGINE_START_CMD",
        "PREVIOUS_STRATEGY_RUNNER_START_CMD",
        "POST_ROLLBACK_VALIDATE_CMD",
        "MAX_ROLLBACK_SECONDS",
        "ROLLBACK_EVIDENCE_OUTPUT",
    )

    try:
        _required_keys(cutover, cutover_required, "cutover template")
        _required_keys(rollback, rollback_required, "rollback template")
    except ValueError as exc:
        print(f"error: {exc}")
        return 2

    output_md = Path(args.output_md)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    markdown = _build_markdown(cutover, rollback)
    output_md.write_text(markdown, encoding="utf-8")
    print(str(output_md))

    if args.output_json:
        output_json = Path(args.output_json)
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(
            json.dumps(
                {
                    "cutover_template": str(cutover_path),
                    "rollback_template": str(rollback_path),
                    "output_md": str(output_md),
                    "cutover_env": cutover["CUTOVER_ENV_NAME"],
                    "rollback_env": rollback["ROLLBACK_ENV_NAME"],
                },
                ensure_ascii=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        print(str(output_json))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
