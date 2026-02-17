# Reconnect Fault Injection Result

## Metadata

- Date:
- Operator:
- Host:
- Build:
- Config profile:
- Interface:

## Scenario Matrix

| Scenario | Parameters | Start Time | End Time | Recovered | Recovery Seconds | Notes |
|---|---|---|---|---|---:|---|
| disconnect | duration=20s |  |  |  |  |  |
| jitter | delay=250ms,jitter=30ms,duration=30s |  |  |  |  |  |
| loss | loss=5%,duration=30s |  |  |  |  |  |
| combined | delay=200ms,jitter=40ms,loss=3%,duration=30s |  |  |  |  |  |

## Acceptance

- [ ] unhealthy state observed during fault
- [ ] auto-reconnect observed after clear
- [ ] P99 recovery `< 10s`
- [ ] no crash
- [ ] no stuck state after repeated runs

## Logs and Evidence

- Probe log file:
- Core engine log file:
- Command log:
- Incident notes:

## Auto Report

Generated markdown report comes from `reconnect_evidence_cli` output file.
