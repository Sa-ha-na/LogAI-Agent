import json
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.hooks.dataform import DataformHook
from google.cloud import bigquery

# ==========================================
# GLOBAL CONFIGURATION CONSTANTS
# ==========================================
PROJECT_ID = "idea-premier-league-ideathon"
REGION = "us-central1"
REPOSITORY_ID = "Datasentinals_Ipl"
WORKSPACE_ID = "patient_dim"

# ==========================================
# LIVE GENAI ERROR INTERPRETER
# ==========================================
def ai_global_error_handler(context):
    import vertexai
    from vertexai.generative_models import GenerativeModel
    
    ti = context.get('task_instance')
    failed_task = ti.task_id
    dag_id = ti.dag_id
    run_id = ti.run_id
    
    raw_error = str(context.get('exception'))
    detailed_error = raw_error

    # Deep Extraction: Safely fetch the exact SQL failure from Dataform
    if "run_" in failed_task:
        try:
            match = re.search(r'workflowInvocations/([a-zA-Z0-9-]+)', raw_error)
            
            if match:
                invocation_id = match.group(1)
                hook = DataformHook()
                
                actions = hook.query_workflow_invocation_actions(
                    project_id=PROJECT_ID,
                    region=REGION,
                    repository_id=REPOSITORY_ID,
                    workflow_invocation_id=invocation_id
                )
                
                # Check all actions. If any failed, dump everything to a string!
                for action in actions:
                    if "FAILED" in str(action):
                        detailed_error = f"DATAFORM SQL FAILURE DETAILS: {str(action)}"
                        break
        except Exception as e:
            detailed_error = raw_error + f" (Note: Deep extraction failed with {e})"

    # Upgraded Context-Aware Prompt
    prompt = f"""
    An Airflow task has failed. Analyze this error and explain it simply.
    
    FAILED TASK: {failed_task}
    RAW ERROR: {detailed_error}

    STRICT RULES:
    1. If the RAW ERROR clearly contains a specific issue (e.g., "not found", "syntax error", "column missing"), explain what broke.
    2. If the RAW ERROR ONLY shows Airflow metadata (like "Workflow Invocation failed" and "state: FAILED") but hides the actual SQL error, DO NOT GUESS. You MUST set clean_summary to: "The Dataform job failed, but the SQL error is hidden." and root_cause_analysis to: "Airflow only captured the metadata. Check the Dataform UI for the exact SQL syntax error."
    3. Keep clean_summary under 15 words.
    4. Keep root_cause_analysis under 2 sentences.

    Respond STRICTLY in this JSON format:
    {{
        "severity": "CRITICAL",
        "clean_summary": "Short summary.",
        "root_cause_analysis": "Brief reason.",
        "step_by_step_remediation": "1. Simple step to fix it."
    }}
    """
    
    try:
        vertexai.init(project=PROJECT_ID, location=REGION)
        model = GenerativeModel("gemini-2.5-flash")
        response = model.generate_content(prompt)
        raw_ai_text = response.text
        
        if raw_ai_text.startswith("```"):
            raw_ai_text = raw_ai_text.split("```json")[-1].split("```")[0].strip()
            
        ai_data = json.loads(raw_ai_text)
        
    except Exception as api_err:
        ai_data = {
            "severity": "CRITICAL",
            "clean_summary": "AI error analysis failed.",
            "root_cause_analysis": f"API Error: {str(api_err)[:50]}",
            "step_by_step_remediation": "1. Check Vertex AI API."
        }

    # Insert into BigQuery
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        log_query = f"""
            INSERT INTO `{PROJECT_ID}.dataverse_ipl.etl_performance_log` 
            (dag_id, run_id, job_tag, execution_date, records_processed, execution_duration_sec, ai_status, ai_analysis)
            VALUES (
                '{dag_id}', '{run_id}', '{failed_task}', CURRENT_TIMESTAMP(), 0, 0.0, 
                '{ai_data['severity']}', \"\"\"Summary: {ai_data['clean_summary']} | Cause: {ai_data['root_cause_analysis']}\"\"\"
            )
        """
        bq_client.query(log_query).result()
    except Exception as bq_err:
        print(f"Logging metrics to BigQuery encountered an error: {bq_err}")

# ==========================================
# PERFORMANCE MONITORING
# ==========================================
def run_ai_performance_agent(**kwargs):
    """
    Evaluates actual execution run profiles against a strict 6-minute limit,
    comparing it to historical maximums. Only logs to BQ if the limit is breached.
    """
    bq_client = bigquery.Client(project=PROJECT_ID)
    dag_run = kwargs['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    
    # 1. Calculate the real execution duration dynamically
    start_date = dag_run.start_date
    end_date = timezone.utcnow()
    current_duration_sec = (end_date - start_date).total_seconds()
    current_duration_min = current_duration_sec / 60.0
    
    current_records = 0  # Replace with XCom pull if you track exact record counts

    # 2. Evaluate execution threshold against strict 6 minutes (360 seconds)
    if current_duration_sec > 360.0:
        
        # 3. Fetch the historical maximum duration for this specific DAG
        try:
            max_query = f"""
                SELECT MAX(execution_duration_sec) as max_dur 
                FROM `{PROJECT_ID}.dataverse_ipl.etl_performance_log` 
                WHERE dag_id = '{dag_id}' AND job_tag = 'ai_performance_monitor'
            """
            result = list(bq_client.query(max_query).result())
            max_dur_sec = result[0].max_dur if result and result[0].max_dur else 0.0
            max_dur_min = max_dur_sec / 60.0
        except Exception as e:
            print(f"Failed to fetch historical max: {e}")
            max_dur_min = 0.0

        ai_status = "CRITICAL"
        ai_reasoning = (
            f"Performance Alert: Pipeline took {current_duration_min:.2f} minutes, "
            f"breaching the 6-minute limit. "
            f"The historical maximum for this DAG was {max_dur_min:.2f} minutes."
        )
        
        # 4. Log to BigQuery ONLY if SLA is breached
        log_query = f"""
            INSERT INTO `{PROJECT_ID}.dataverse_ipl.etl_performance_log` 
            (dag_id, run_id, job_tag, execution_date, records_processed, execution_duration_sec, ai_status, ai_analysis)
            VALUES (
                '{dag_id}', '{run_id}', 'ai_performance_monitor', CURRENT_TIMESTAMP(), 
                {current_records}, {current_duration_sec}, '{ai_status}', \"\"\"{ai_reasoning}\"\"\"
            )
        """
        bq_client.query(log_query).result()
        
        # 5. Fail the task so Airflow registers the breach
        raise RuntimeError(ai_reasoning)
    else:
        print(f"Pipeline execution speed ({current_duration_min:.2f} mins) sits safely within acceptable limits. No log created.")


# ==========================================
# DAG TOPOLOGY & CORE PARAMETERS
# ==========================================
default_args = {
    "owner": "data_engineering_team",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,  
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": ai_global_error_handler, # Attached to every task to ensure failures are caught!
}

with DAG(
    "v15", # Updated DAG ID to avoid conflicts with previous runs
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "dataform", "patient_dim", "ai_agent"],
) as dag:

    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID, region=REGION, repository_id=REPOSITORY_ID,
        compilation_result={
            "workspace": f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/workspaces/{WORKSPACE_ID}"
        },
    )

    run_patient_dim_jobs = DataformCreateWorkflowInvocationOperator(
        task_id="run_patient_dim_jobs",
        project_id=PROJECT_ID, region=REGION, repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}",
            "invocation_config": {"included_tags": ["job_patient_dim"]},
        },
    )

    run_nurse_dim_jobs = DataformCreateWorkflowInvocationOperator(
        task_id="run_nurse_dim_jobs",
        project_id=PROJECT_ID, region=REGION, repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}",
            "invocation_config": {"included_tags": ["job_nurse_dim"]},
        },
    )

    ai_performance_monitor = PythonOperator(
        task_id="ai_performance_monitor",
        python_callable=run_ai_performance_agent,
        trigger_rule="all_done" # Ensures it checks performance even if upstream tasks fail
    )

    compile_dataform >> [run_patient_dim_jobs, run_nurse_dim_jobs] >> ai_performance_monitor
