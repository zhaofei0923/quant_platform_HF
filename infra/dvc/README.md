# DVC Remote for Backtest Dataset

This repository uses an S3-compatible DVC remote for PR-6.1 backtest datasets.

## Remote Profile

- remote name: `backtest-s3`
- bucket: `quant-backtest-data`
- endpoint: `https://s3.amazonaws.com`
- region: `us-east-1`
- auth mode: environment variables
- path style: disabled (AWS default)

## Environment Variables

Export credentials before `dvc push/pull`:

```bash
export AWS_ACCESS_KEY_ID="<your-access-key-id>"
export AWS_SECRET_ACCESS_KEY="<your-secret-access-key>"
export AWS_DEFAULT_REGION="us-east-1"
```

## Suggested Workflow

```bash
dvc remote list
dvc add runtime/backtest/parquet
git add runtime/backtest/parquet.dvc dvc.lock
dvc push
```
