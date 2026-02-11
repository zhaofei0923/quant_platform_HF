# Kubernetes Deployment Runbook (Non-Hotpath Components)

## Scope

- This runbook targets non-hotpath components only.
- Current manifest bundle deploys `data_pipeline` to Kubernetes.
- `core_engine` remains recommended on bare metal/systemd for low-latency path.

## 1) Render manifests

```bash
.venv/bin/python scripts/ops/render_k8s_manifests.py \
  --repo-root . \
  --output-dir deploy/k8s \
  --namespace quant-hft \
  --image-repository ghcr.io/<org>/quant-hft \
  --image-tag v0.1.0
```

Generated files:
- `deploy/k8s/namespace.yaml`
- `deploy/k8s/configmap-data-pipeline.yaml`
- `deploy/k8s/secret-archive.example.yaml`
- `deploy/k8s/persistentvolumeclaim-runtime.yaml`
- `deploy/k8s/deployment-data-pipeline.yaml`
- `deploy/k8s/kustomization.yaml`

## 2) Prepare archive secret

```bash
cp deploy/k8s/secret-archive.example.yaml deploy/k8s/secret-archive.yaml
```

Fill:
- `QUANT_HFT_ARCHIVE_ACCESS_KEY`
- `QUANT_HFT_ARCHIVE_SECRET_KEY`

Then update `deploy/k8s/kustomization.yaml` to reference `secret-archive.yaml`.

## 3) Apply manifests

```bash
kubectl apply -k deploy/k8s
kubectl -n quant-hft get pods
```

## 4) Validate runtime

```bash
kubectl -n quant-hft logs deploy/quant-hft-data-pipeline --tail=200
kubectl -n quant-hft get pvc quant-hft-runtime
```

## Notes

- Default archive endpoint is `minio:9000`; adjust in ConfigMap for your cluster.
- Default storage request is `20Gi`; tune `--runtime-pvc-size` when rendering.
- Use release bundle script for reproducible packaging:
  - `scripts/build/package_nonhotpath_release.sh`

## Rollback Drill

1. Record current revision:

```bash
kubectl -n quant-hft rollout history deploy/quant-hft-data-pipeline
```

2. Deploy new image/manifests and confirm rollout:

```bash
kubectl -n quant-hft rollout status deploy/quant-hft-data-pipeline --timeout=180s
```

3. Simulate failure and rollback:

```bash
kubectl -n quant-hft rollout undo deploy/quant-hft-data-pipeline
kubectl -n quant-hft rollout status deploy/quant-hft-data-pipeline --timeout=180s
```

4. Validate:

```bash
kubectl -n quant-hft get pods -l app.kubernetes.io/name=quant-hft-data-pipeline
kubectl -n quant-hft logs deploy/quant-hft-data-pipeline --tail=200
```

Acceptance:
- rollback completes without stuck rollout
- pipeline pod returns to Ready and resumes periodic export logs
