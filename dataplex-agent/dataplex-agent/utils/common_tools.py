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

