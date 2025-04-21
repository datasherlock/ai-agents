import uuid
from google.cloud import dataproc_v1 as dataproc
from google.api_core.exceptions import NotFound, GoogleAPICallError
from google.protobuf.duration_pb2 import Duration
from typing import Optional, List, Dict
from google.protobuf.json_format import MessageToDict

def initialize_clients(region: str):
    try:
        cluster_client = dataproc.ClusterControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"})
        job_client = dataproc.JobControllerClient()
        # You could add other clients like AutoscalingPolicyServiceClient if needed
        # policy_client = dataproc.AutoscalingPolicyServiceClient()
    except Exception as e:
        print(f"Error initializing Google Cloud Dataproc clients: {e}")
        # Handle initialization failure appropriately, maybe raise or exit
        cluster_client = None
        job_client = None
    return cluster_client, job_client



def _parse_cluster_response(cluster) -> Dict:
    """Helper to extract key info from a Cluster object."""
    if not cluster:
        return {}
    return {
        'name': cluster.cluster_name,
        'status': cluster.status.state.name, # Get enum name string
        'uuid': cluster.cluster_uuid,
        'project_id': cluster.project_id,
        # Region is usually inferred from the client request, not always in response
        'zone': cluster.config.gce_cluster_config.zone_uri.split('/')[-1] if cluster.config.gce_cluster_config.zone_uri else None,
        'image_version': cluster.config.software_config.image_version,
        'master_nodes': cluster.config.master_config.num_instances,
        'master_machine_type': cluster.config.master_config.machine_type_uri.split('/')[-1] if cluster.config.master_config.machine_type_uri else None,
        'worker_nodes': cluster.config.worker_config.num_instances,
        'worker_machine_type': cluster.config.worker_config.machine_type_uri.split('/')[-1] if cluster.config.worker_config.machine_type_uri else None,
        'creation_timestamp': cluster.status.creation_time.rfc3339() if hasattr(cluster.status, 'creation_time') else None,
        'endpoint_uri': cluster.config.endpoint_config.http_ports['Web UI'] if cluster.config.endpoint_config and 'Web UI' in cluster.config.endpoint_config.http_ports else None,
    }

def _parse_job_response(job) -> Dict:
    """Helper to extract key info from a Job object."""
    if not job:
        return {}

    job_info = {
        'job_id': job.reference.job_id,
        'type': job.type_case.name, # e.g., 'pyspark_job', 'hive_job'
        'status': job.status.state.name,
        'status_details': job.status.details,
        'cluster_name': job.placement.cluster_name,
        'submitted_by': job.submitted_by,
        'driver_output_uri': job.driver_output_resource_uri,
        'start_time': job.status.start_time.rfc3339() if hasattr(job.status, 'start_time') else None,
        'end_time': job.status.end_time.rfc3339() if hasattr(job.status, 'end_time') else None,
    }
    # Add type-specific details
    if job.type_case.name == 'pyspark_job':
        job_info['main_file'] = job.pyspark_job.main_python_file_uri
        job_info['args'] = list(job.pyspark_job.args)
    elif job.type_case.name == 'spark_job':
        job_info['main_class_or_jar'] = job.spark_job.main_class or job.spark_job.main_jar_file_uri
        job_info['args'] = list(job.spark_job.args)
    elif job.type_case.name == 'hive_job':
        job_info['query_file_uri'] = job.hive_job.query_file_uri
        job_info['query_list'] = list(job.hive_job.query_list.queries)
    elif job.type_case.name == 'pig_job':
        job_info['query_file_uri'] = job.pig_job.query_file_uri
        job_info['query_list'] = list(job.pig_job.query_list.queries)
    # Add other job types as needed (Spark SQL, Presto, etc.)

    return job_info



def submit_dataproc_job(
    project_id: str,
    region: str,
    cluster_name: str,
    job_type: str,
    job_details: dict,
    job_id_prefix: str = "agent-job",
    labels: dict = None
) -> dict:
    """
    Submits various types of Dataproc jobs based on the provided job_type.

    Args:
        project_id (str): Google Cloud project ID.
        region (str): Dataproc region (e.g., 'us-central1').
                      NOTE: Ensure the job_client was initialized for this region,
                      or re-initialize if necessary for multi-region agents.
        cluster_name (str): Name of the cluster to run the job on.
        job_type (str): Type of job. Supported values correspond to the Job type fields
                      in the API (case-insensitive matching attempted):
                      'pyspark', 'spark', 'hive', 'pig', 'spark_sql', 'hadoop',
                      'spark_r', 'presto', 'trino'.
        job_details (dict): Dictionary containing parameters specific TO THE JOB TYPE.
                            Keys MUST match the fields within the corresponding Job type
                            message in the Dataproc API (e.g., for 'pyspark', keys
                            like 'main_python_file_uri', 'args', 'python_file_uris').
                            See Dataproc documentation for fields per job type.
        job_id_prefix (str, optional): Prefix for the generated job ID.
                                      Defaults to "agent-job".
        labels (dict, optional): Dictionary of labels to attach to the job.
                                 Defaults to None.

    Returns:
        dict: A dictionary with submitted job details (ID, initial status)
              or an error dictionary if submission fails.
    """
    # Check if client is initialized (especially important if initialization can fail)
    if not job_client:
        return {"error": "Dataproc job client not initialized."}

    # Optional: Add check if job_client's region matches the requested region,
    # or re-initialize the client for the target region if your agent needs to be multi-region.
    # For simplicity now, we assume the client matches the requested region.

    job_uuid = str(uuid.uuid4())[:8] # Short UUID for readability in ID
    # Standardize job type string to map to API fields (snake_case)
    job_type_cleaned = job_type.lower().replace('-', '_')
    job_id = f"{job_id_prefix}-{job_type_cleaned}-{job_uuid}"

    # --- Construct the core Job payload ---
    job_payload = {
        "placement": {"cluster_name": cluster_name},
        "reference": {"job_id": job_id},
    }
    if labels:
        job_payload["labels"] = labels

    # --- Map job_type to the correct field name in the Job message ---
    # Example: 'pyspark' -> 'pyspark_job', 'spark_sql' -> 'spark_sql_job'
    job_type_field = f"{job_type_cleaned}_job"

    # List of valid job type fields within the dataproc_v1.types.Job message
    supported_job_fields = [
        "hadoop_job", "spark_job", "pyspark_job", "hive_job", "pig_job",
        "spark_r_job", "spark_sql_job", "presto_job", "trino_job" # Check API docs for exhaustive list
    ]

    if job_type_field not in supported_job_fields:
        supported_types_str = ", ".join([f.replace('_job', '') for f in supported_job_fields])
        return {
            "error": f"Unsupported job_type: '{job_type}'. " \
                     f"Cleaned type '{job_type_field}' is invalid. " \
                     f"Supported types derive from: {supported_types_str}."
            }

    # --- Add the type-specific details to the payload ---
    # **IMPORTANT**: Assumes `job_details` dictionary keys and values directly match
    # the structure expected by the API for the specified job_type_field.
    # Basic validation example (can be expanded significantly):
    if not isinstance(job_details, dict):
         return {"error": f"job_details must be a dictionary, got {type(job_details)}."}

    # Example validation for pyspark (add others as needed)
    if job_type_field == "pyspark_job" and "main_python_file_uri" not in job_details:
        return {"error": "Missing required key 'main_python_file_uri' in job_details for pyspark job."}
    elif job_type_field == "spark_job" and not ("main_class" in job_details or "main_jar_file_uri" in job_details):
        return {"error": "Missing required key 'main_class' or 'main_jar_file_uri' in job_details for spark job."}
    elif job_type_field in ["hive_job", "pig_job", "spark_sql_job"] and not ("query_file_uri" in job_details or ("query_list" in job_details and job_details["query_list"])):
         return {"error": f"Missing required key 'query_file_uri' or non-empty 'query_list' in job_details for {job_type} job."}
    # Add more validations based on Dataproc API requirements for each job type

    job_payload[job_type_field] = job_details
    # print(f"Submitting job payload: {job_payload}") # Debug print

    try:
        # Construct the request object
        request = dataproc.SubmitJobRequest(
            project_id=project_id,
            region=region,
            job=job_payload,
            # request_id=job_id # Optional: Use for idempotency if needed
        )

        # Submit the job - this is synchronous for submission confirmation
        submitted_job = job_client.submit_job(request=request)

        # Return concise success info (or use MessageToDict for full details)
        return {
            "message": f"{job_type.capitalize()} job '{submitted_job.reference.job_id}' submitted successfully.",
            "job_id": submitted_job.reference.job_id,
            "status": submitted_job.status.state.name, # Initial status (e.g., PENDING, QUEUED)
            "cluster_name": submitted_job.placement.cluster_name,
            "submitted_job_type": submitted_job.type_case.name # Actual type field set in the response
        }

    except InvalidArgument as e:
        print(f"InvalidArgument Error submitting {job_type} job '{job_id}': {e}")
        # This often means the structure of job_details was incorrect for the job_type
        return {"error": f"InvalidArgument submitting job: {e.message}. Review the structure of 'job_details' for '{job_type}'."}
    except GoogleAPICallError as e:
        print(f"API Error submitting {job_type} job '{job_id}': {e}")
        return {"error": f"API Error submitting job: {e.message}"}
    except Exception as e:
        print(f"Unexpected error submitting {job_type} job '{job_id}': {e}")
        return {"error": f"Unexpected error submitting job: {str(e)}"}

# --- Example Usage (Add to your __main__ block for testing) ---
if __name__ == '__main__':
    # Replace with your actual project/region/cluster details
    TEST_PROJECT_ID = "your-gcp-project-id"
    TEST_REGION = "us-central1" # Make sure client is initialized for this region
    TEST_CLUSTER_NAME = "your-test-cluster-name"

    print("\n--- Testing Generic Job Submission ---")

    if not job_client:
        print("Job client not initialized. Skipping submission tests.")
    else:
        # --- Example 1: PySpark Job ---
        print("\n[INFO] Testing PySpark Job Submission...")
        pyspark_details = {
            "main_python_file_uri": f"gs://dataproc-examples/pyspark/hello-world/hello-world.py",
            "args": ["arg1", "value1"],
            # Add other fields like 'python_file_uris', 'jar_file_uris', 'properties' as needed
        }
        pyspark_result = submit_dataproc_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            job_type="pyspark", # or "PySpark", "pySpark"
            job_details=pyspark_details,
            labels={"source": "agent-test"}
        )
        print(f"PySpark Submission Result: {pyspark_result}")

        # --- Example 2: Spark Job (JAR) ---
        print("\n[INFO] Testing Spark Job (JAR) Submission...")
        spark_jar_details = {
            "main_jar_file_uri": "file:///usr/lib/spark/examples/jars/spark-examples.jar",
            "main_class": "org.apache.spark.examples.SparkPi",
            "args": ["1000"], # Argument for SparkPi (iterations)
            # Add 'jar_file_uris', 'properties' etc. if needed
        }
        spark_jar_result = submit_dataproc_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            job_type="spark",
            job_details=spark_jar_details
        )
        print(f"Spark JAR Submission Result: {spark_jar_result}")

        # --- Example 3: Hive Job (Query List) ---
        print("\n[INFO] Testing Hive Job (Query List) Submission...")
        hive_query_details = {
            "query_list": {
                "queries": [
                    "SHOW DATABASES;",
                    "CREATE TABLE IF NOT EXISTS names_agent_test (id INT, name STRING);",
                    "SELECT * FROM names_agent_test LIMIT 10;"
                ]
            },
            # Add 'script_variables', 'jar_file_uris', 'properties' if needed
        }
        hive_result = submit_dataproc_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            job_type="hive",
            job_details=hive_query_details
        )
        print(f"Hive Query Submission Result: {hive_result}")

        # --- Example 4: Invalid Job Type ---
        print("\n[INFO] Testing Invalid Job Type...")
        invalid_type_result = submit_dataproc_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            job_type="invalid-job-type",
            job_details={"foo": "bar"}
        )
        print(f"Invalid Type Submission Result: {invalid_type_result}")

        # --- Example 5: Missing Required Field ---
        print("\n[INFO] Testing Missing Required Field (PySpark)...")
        missing_field_details = {
            "args": ["arg1"] # Missing main_python_file_uri
        }
        missing_field_result = submit_dataproc_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            job_type="pyspark",
            job_details=missing_field_details
        )
        print(f"Missing Field Submission Result: {missing_field_result}")


def get_current_time(query: str) -> str:
    """Simulates getting the current time for a city.

    Args:
        city: The name of the city to get the current time for.

    Returns:
        A string with the current time information.
    """
    if "sf" in query.lower() or "san francisco" in query.lower():
        tz_identifier = "America/Los_Angeles"
    else:
        return f"Sorry, I don't have timezone information for query: {query}."

    tz = ZoneInfo(tz_identifier)
    now = datetime.datetime.now(tz)
    return f"The current time for query {query} is {now.strftime('%Y-%m-%d %H:%M:%S %Z%z')}"

def get_lichess_rating(username: str):
    """Fetches and prints Lichess user ratings using the public API.

    This function accepts the Lichess username as an argument,
    constructs the Lichess API URL for that user (http://en.lichess.org/api/user/),
    fetches the user data, parses the JSON response, and prints the ratings
    for various game performance categories (like blitz, rapid, classical, etc.)
    to standard output.

    
    Args:
        username (str): The Lichess username to fetch ratings for. 

    Returns:
        dict: A dictionary containing the performance statistics ('perfs') for
              the specified user, as returned by the Lichess API. Keys are
              game type strings (e.g., 'blitz', 'rapid'), and values are
              dictionaries containing details like 'rating', 'prog' (progress),
              'games'. Returns the dictionary even if empty or if ratings are missing.
              Can raise exceptions if the API request fails or parsing errors occur.

    """
    import json
    from urllib.request import urlopen

    user_json = urlopen("http://en.lichess.org/api/user/" + username).read()
    user_perfs = json.loads(user_json)['perfs']

    print(f'username: {username}')
    for k in user_perfs:
        rating = user_perfs[k].get('rating')
        if rating:
            print(f'{k}: {rating}')
    return user_perfs

