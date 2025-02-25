import requests
import json
import base64
import time
import os

import os
from dotenv import load_dotenv

load_dotenv()


# databricks_token = 
databricks_host = os.getenv("DATABRICKS_HOST")
databricks_token = os.getenv("DATABRICKS_TOKEN")
headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Content-Type": "application/json"
}
 
# Read config from DBFS
dbfs_json_path = "dbfs:/tmp/tasks_config.json"
dbfs_api_url = f"{databricks_host}/api/2.0/dbfs/read"
 
response = requests.get(dbfs_api_url, headers=headers, json={"path": dbfs_json_path})
 
if response.status_code == 200:
    json_data = base64.b64decode(response.json()["data"]).decode("utf-8")
    config = json.loads(json_data)
    print("JSON Config Loaded from DBFS!")
else:
    print(f"Failed to read JSON from DBFS: {response.text}")
    exit()
 
 
def create_workspace_folder(folder_path):
    """Creates a folder in Databricks workspace"""
    workspace_mkdirs_url = f"{databricks_host}/api/2.0/workspace/mkdirs"
    response = requests.post(workspace_mkdirs_url, headers=headers, json={"path": folder_path})
    if response.status_code == 200:
        print(f"Folder Created: {folder_path} (or already exists)")
    else:
        print(f"Failed to create folder: {response.text}")
        exit()
 
 
def create_notebook(job_id, task):
    """Creates a notebook for a given task inside the job folder"""
    notebook_path = f"/Workspace/Jobs/{job_id}/{task['task_id']}"
    
    notebook_content = f"""
# Databricks Notebook for {task['task_id']}
# Developer can add transformation logic here
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("{task['task_id']}").getOrCreate()
 
print("Task {task['task_id']} is ready for development.")
"""
    
    encoded_content = base64.b64encode(notebook_content.encode("utf-8")).decode("utf-8")
    
    payload = {
        "content": encoded_content,
        "path": notebook_path,
        "language": "PYTHON",
        "overwrite": True
    }
    
    response = requests.post(f"{databricks_host}/api/2.0/workspace/import", headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f"Notebook Created: {notebook_path}")
    else:
        print(f"Failed to create notebook: {response.text}")
 
 
def create_databricks_job(job_id, tasks):
    """Creates a Databricks job with the given tasks"""
    job_name = f"ETL_Pipeline_Job_{job_id}"
    databricks_job_url = f"{databricks_host}/api/2.1/jobs/create"
 
    job_tasks = []
    previous_task_key = None
 
    for task in tasks:
        task_id = str(task["task_id"])
 
        task_payload = {
            "task_key": task_id,
            "notebook_task": {"notebook_path": f"/Workspace/Jobs/{job_id}/{task_id}"},
            "existing_cluster_id": "0221-073701-blxdvzj3"
        }
        
        if previous_task_key:
            task_payload["depends_on"] = [{"task_key": previous_task_key}]
        
        job_tasks.append(task_payload)
        previous_task_key = task_id
 
    job_payload = {"name": job_name, "tasks": job_tasks}
    response = requests.post(databricks_job_url, headers=headers, data=json.dumps(job_payload))
 
    if response.status_code == 200:
        job_id = response.json()["job_id"]
        print(f"Job Created: {job_name} with {len(tasks)} tasks (ID: {job_id})")
        return job_id
    else:
        print(f"Failed to create job: {response.text}")
        return None
 
 
def trigger_job_and_wait(job_id):
    """Triggers a Databricks job and waits for completion before proceeding."""
    trigger_url = f"{databricks_host}/api/2.1/jobs/run-now"
    payload = {"job_id": job_id}
    response = requests.post(trigger_url, headers=headers, json=payload)
 
    if response.status_code == 200:
        run_id = response.json()["run_id"]
        print(f"Job {job_id} triggered successfully! Run ID: {run_id}")
        wait_for_job_completion(run_id)
    else:
        print(f"Failed to trigger job {job_id}: {response.text}")
 
 
def wait_for_job_completion(run_id):
    """Waits until a job run is completed before proceeding."""
    status_url = f"{databricks_host}/api/2.1/jobs/runs/get"
    while True:
        response = requests.get(status_url, headers=headers, json={"run_id": run_id})
        if response.status_code == 200:
            run_status = response.json()
            life_cycle_state = run_status["state"]["life_cycle_state"]
            result_state = run_status["state"].get("result_state", "")
            
            print(f"Run {run_id} Status: {life_cycle_state} | Result: {result_state}")
 
            if life_cycle_state == "TERMINATED":
                if result_state == "SUCCESS":
                    print(f"Run {run_id} completed successfully!")
                    return
                else:
                    print(f"Run {run_id} failed or was canceled!")
                    exit(1)
        
        time.sleep(10)
 
# Process the config JSON
jobs = {}
for task in config["tasks"]:
    job_id = task["job_id"]
    if job_id not in jobs:
        jobs[job_id] = []
    jobs[job_id].append(task)
 
created_jobs = {}
for job_id, tasks in jobs.items():
    create_workspace_folder(f"/Workspace/Jobs/{job_id}")
    for task in tasks:
        create_notebook(job_id, task)
    job_databricks_id = create_databricks_job(job_id, tasks)
    if job_databricks_id:
        created_jobs[job_id] = job_databricks_id
 
sorted_job_ids = sorted(created_jobs.keys(), key=lambda x: int(str(x).split("_")[-1]))
 
previous_job_id = None
for job_id in sorted_job_ids:
    job_databricks_id = created_jobs[job_id]
    if previous_job_id:
        print(f"Waiting for Job {previous_job_id} to complete before triggering Job {job_id}...")
    trigger_job_and_wait(job_databricks_id)
    previous_job_id = job_id
 
print("Pipeline execution complete!")