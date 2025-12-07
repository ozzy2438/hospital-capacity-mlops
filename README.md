# hospital-capacity-mlops

MLOps pipeline for hospital capacity and surge forecasting.

This repository contains an Apache Airflow DAG that:
- Retrains the model on a schedule (e.g. monthly).
- Evaluates a new candidate model against the current production model.
- Only auto-promotes the candidate when performance criteria are met.

You can extend this with additional monitoring, drift detection, and alerting as needed.
