# Reconnect Fault Injection Result

## Metadata
- Date: 2026-02-10
- Operator: kevin0923
- Host: DESKTOP-H7JH7QT
- Build: simnow-20260210-224215
- Config profile: configs/sim/ctp_trading_hours_group2.yaml
- Interface: eth0

## Summary
- Samples: 4
- Recovery P99 (s): 0.839
- Target P99 (s): 10.000
- Meets target: no

## Scenario Matrix
| Scenario | Parameters | Start Time (UTC) | End Time (UTC) | Recovered | Recovery Seconds | Notes |
|---|---|---|---|---|---:|---|
| disconnect | delay_ms=250,duration_sec=20,iface=eth0,jitter_ms=30,loss_percent=2.5,ports=40001,40011,30001,30011,30002,30012,30003,30013,target_ip=182.254.243.31 | 2026-02-10T14:42:23.207087+00:00 | 2026-02-10T14:42:43.631638+00:00 | yes | 0.839 | delay_ms=250,duration_sec=20,iface=eth0,jitter_ms=30,loss_percent=2.5,ports=40001,40011,30001,30011,30002,30012,30003,30013,target_ip=182.254.243.31;unhealthy_not_observed |
| jitter | delay_ms=250,duration_sec=30,iface=eth0,jitter_ms=30,loss_percent=2.5,ports=40001,40011,30001,30011,30002,30012,30003,30013,target_ip=182.254.243.31 | 2026-02-10T14:42:43.700925+00:00 | 2026-02-10T14:43:13.703818+00:00 | yes | 0.772 | delay_ms=250,duration_sec=30,iface=eth0,jitter_ms=30,loss_percent=2.5,ports=40001,40011,30001,30011,30002,30012,30003,30013,target_ip=182.254.243.31;unhealthy_not_observed |
| loss | delay_ms=250,duration_sec=30,iface=eth0,jitter_ms=30,loss_percent=5.0,ports=40001,40011,30001,30011,30002,30012,30003,30013,target_ip=182.254.243.31 | 2026-02-10T14:43:13.770406+00:00 | 2026-02-10T14:43:43.773263+00:00 | no |  | delay_ms=250,duration_sec=30,iface=eth0,jitter_ms=30,loss_percent=5.0,ports=40001,40011,30001,30011,30002,30012,30003,30013,target_ip=182.254.243.31;unhealthy_not_observed;not_recovered |
| combined | delay_ms=200,duration_sec=30,iface=eth0,jitter_ms=40,loss_percent=3.0,ports=40001,40011,30001,30011,30002,30012,30003,30013,target_ip=182.254.243.31 | 2026-02-10T14:43:43.839205+00:00 | 2026-02-10T14:44:13.841863+00:00 | no |  | delay_ms=200,duration_sec=30,iface=eth0,jitter_ms=40,loss_percent=3.0,ports=40001,40011,30001,30011,30002,30012,30003,30013,target_ip=182.254.243.31;unhealthy_not_observed;not_recovered |

## Acceptance
- [ ] unhealthy state observed during fault
- [ ] auto-reconnect observed after clear
- [ ] P99 recovery `< 10s`
- [ ] no crash (manual confirmation required)
- [ ] no stuck state after repeated runs

## Logs and Evidence
- Probe log file: runtime/reconnect_probe.log
- Fault events file: runtime/fault_events.jsonl

