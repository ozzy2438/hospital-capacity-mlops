from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_training_data(**context):
    """
    Load and prepare training data for the hospital capacity model.
    Replace the body of this function with your actual data extraction logic.
    """
    # TODO: implement data extraction (e.g., query your warehouse or database)
    return "data_extraction_complete"


def train_candidate_model(**context):
    """
    Train a new candidate model using the latest data.
    Persist the model artifact to your chosen storage (e.g., S3, GCS, local).
    """
    # TODO: implement model training and artifact saving
    return "candidate_model_trained"


def evaluate_models(**context):
    """
    Compare the current production model with the newly trained candidate model
    on the same evaluation dataset. Compute metrics such as AUC, precision,
    recall, and calibration error.

    This function should set a promotion flag based on your criteria and return
    a small dictionary that the downstream task can read via XCom.
    """
    # TODO: load production and candidate models, evaluate on a hold-out set
    eval_result = {"promote": False, "metrics_prod": {}, "metrics_cand": {}}
    return eval_result


def maybe_promote_model(**context):
    """
    Conditionally promote the candidate model to production based on the
    evaluation results from the previous task.
    """
    ti = context["ti"]
    eval_result = ti.xcom_pull(task_ids="evaluate_models")

    if not eval_result:
        raise ValueError("No evaluation result found in XCom.")

    promote = bool(eval_result.get("promote", False))

    if promote:
        # TODO: implement logic to copy or register the candidate as the new production model
        print("Promoting candidate model to production.")
    else:
        # No promotion; log and exit.
        print("Candidate model did not meet promotion criteria. Keeping current production model.")


with DAG(
    dag_id="hospital_capacity_retraining",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["mlops", "hospital-capacity"],
) as dag:

    extract_data_task = PythonOperator(
        task_id="extract_training_data",
        python_callable=extract_training_data,
    )

    train_candidate_task = PythonOperator(
        task_id="train_candidate_model",
        python_callable=train_candidate_model,
    )

    evaluate_models_task = PythonOperator(
        task_id="evaluate_models",
        python_callable=evaluate_models,
    )

    maybe_promote_task = PythonOperator(
        task_id="maybe_promote_model",
        python_callable=maybe_promote_model,
    )

    extract_data_task >> train_candidate_task >> evaluate_models_task >> maybe_promote_task
