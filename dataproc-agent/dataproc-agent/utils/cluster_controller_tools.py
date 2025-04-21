from .common_tools import *
import uuid
from google.cloud import dataproc_v1 as dataproc
from google.api_core.exceptions import NotFound, GoogleAPICallError
from google.protobuf.duration_pb2 import Duration
from typing import Optional, List, Dict
from google.protobuf.json_format import MessageToDict
from google.protobuf import field_mask_pb2

async def create_dataproc_cluster_async(project_id: str, region: str, cluster_name: str, cluster_config: dict):
    """
    Asynchronously creates a new Dataproc cluster in a specific project and region.

    Args:
        project_id (str): The Google Cloud project ID.
        region (str): The Dataproc region (e.g., 'us-central1').
        cluster_name (str): The name for the new cluster.
        cluster_config (dict): A dictionary representing the cluster configuration.
                               This should match the structure of the ClusterConfig message.
                               Example: {'gce_cluster_config': {'zone_uri': 'us-central1-a'},
                                         'master_config': {'num_instances': 1, 'machine_type_uri': 'n1-standard-1'},
                                         'worker_config': {'num_instances': 2, 'machine_type_uri': 'n1-standard-1'}}

    Returns:
        dict: A dictionary containing the result of the operation (LRO details or error).
    """
    try:
        # Create an async client
        cluster_client = dataproc.ClusterControllerAsyncClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

        # Prepare the cluster object
        cluster = {
            "project_id": project_id,
            "cluster_name": cluster_name,
            "config": cluster_config,
        }

        # Make the async request
        operation = await cluster_client.create_cluster(
            request={"project_id": project_id, "region": region, "cluster": cluster}
        )

        print(f"Initiated async cluster creation for {cluster_name}. Operation: {operation.metadata.operation_type}")
        # Returning the operation details allows tracking the async creation process.
        return {"status": "creating", "operation_name": operation.operation.name, "metadata": str(operation.metadata)}

    except GoogleAPICallError as e:
        print(f"API Error creating cluster {cluster_name} async in {project_id}/{region}: {e}")
        return {"error": f"API Error creating cluster async: {e.message}"}
    except Exception as e:
        print(f"Unexpected error creating cluster {cluster_name} async in {project_id}/{region}: {e}")
        return {"error": f"Unexpected error creating cluster async: {str(e)}"}
    
async def get_dataproc_cluster_async(project_id: str, region: str, cluster_name: str):
    """
    Asynchronously retrieves details of a specific Dataproc cluster.

    Args:
        project_id (str): The Google Cloud project ID.
        region (str): The Dataproc region (e.g., 'us-central1').
        cluster_name (str): The name of the cluster.

    Returns:
        dict: A dictionary containing cluster details or an error message.
    """
    try:
        # Create an async client
        cluster_client = dataproc.ClusterControllerAsyncClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

        # Make the async request
        request = dataproc.GetClusterRequest(
            project_id=project_id, region=region, cluster_name=cluster_name
        )
        cluster = await cluster_client.get_cluster(request=request)

        # Convert the proto message to a dictionary
        cluster_dict = MessageToDict(cluster._pb, preserving_proto_field_name=True)
        return cluster_dict

    except NotFound:
        print(f"Cluster {cluster_name} not found async in {project_id}/{region}.")
        return {"error": f"Cluster not found async: {cluster_name}"}
    except GoogleAPICallError as e:
        print(f"API Error getting cluster {cluster_name} async in {project_id}/{region}: {e}")
        return {"error": f"API Error getting cluster async: {e.message}"}
    except Exception as e:
        print(f"Unexpected error getting cluster {cluster_name} async in {project_id}/{region}: {e}")
        return {"error": f"Unexpected error getting cluster async: {str(e)}"}

async def list_dataproc_clusters_async(project_id: str, region: str, filter_str: str):
    """
    Asynchronously lists all Dataproc clusters in a specific project and region.

    Args:
        project_id (str): The Google Cloud project ID.
        region (str): The Dataproc region (e.g., 'us-central1').
        filter_str (str, optional): A filter string to apply (e.g., 'status.state = RUNNING').
                                    Defaults to None.

    Returns:
        list: A list of dictionaries, each containing info about a cluster,
              or a list containing a single error dictionary.
    """
    clusters_list = []
    try:
        # Create an async client
        cluster_client = dataproc.ClusterControllerAsyncClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

        # Prepare request
        request = dataproc.ListClustersRequest(project_id=project_id, region=region)
        if filter_str:
            request.filter = filter_str

        # The list_clusters method returns an async iterator.
        async for cluster in cluster_client.list_clusters(request=request):
            cluster_iterator = await cluster_client.list_clusters(request=request)
            async for cluster in cluster_iterator:
                cluster_dict = MessageToDict(cluster._pb, preserving_proto_field_name=True)
                clusters_list.append(cluster_dict)

        if not clusters_list:
             print(f"No clusters found async in {project_id}/{region} matching filter '{filter_str}'.")
             # Return empty list is valid, signifies no clusters found
        return clusters_list

    except GoogleAPICallError as e:
        print(f"API Error listing clusters async in {project_id}/{region}: {e}")
        return [{"error": f"API Error listing clusters async: {e.message}"}]
    except Exception as e:
        print(f"Unexpected error listing clusters async in {project_id}/{region}: {e}")
        return [{"error": f"Unexpected error listing clusters async: {str(e)}"}]

async def update_dataproc_cluster_async(project_id: str, region: str, cluster_name: str, update_mask: list[str], updated_cluster_config: dict):
    """
    Asynchronously updates the configuration of an existing Dataproc cluster.

    Args:
        project_id (str): The Google Cloud project ID.
        region (str): The Dataproc region (e.g., 'us-central1').
        cluster_name (str): The name of the cluster to update.
        update_mask (list[str]): A list of field paths to update (e.g., ['config.worker_config.num_instances']).
                                 Uses FieldMask syntax.
        updated_cluster_config (dict): A dictionary containing the fields to update and their new values.
                                       Only fields specified in update_mask should be present.
                                       Example for worker count: {'worker_config': {'num_instances': 4}}

    Returns:
        dict: A dictionary containing the LRO details or an error message.
    """
    try:
        # Create an async client
        cluster_client = dataproc.ClusterControllerAsyncClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

        # Create the FieldMask
        field_mask = field_mask_pb2.FieldMask(paths=update_mask)

        # Prepare the cluster update object (only include fields being updated)
        # The structure must match the Cluster object, containing only the updated fields.
        cluster_update_data = {"config": updated_cluster_config}


        # Make the async request
        operation = await cluster_client.update_cluster(
            request={
                "project_id": project_id,
                "region": region,
                "cluster_name": cluster_name,
                "cluster": cluster_update_data,
                "update_mask": field_mask,
            }
        )

        print(f"Initiated async cluster update for {cluster_name}. Operation: {operation.metadata.operation_type}")
        return {"status": "updating", "operation_name": operation.operation.name, "metadata": str(operation.metadata)}

    except NotFound:
        print(f"Cluster {cluster_name} not found for async update in {project_id}/{region}.")
        return {"error": f"Cluster not found for async update: {cluster_name}"}
    except GoogleAPICallError as e:
        print(f"API Error updating cluster {cluster_name} async in {project_id}/{region}: {e}")
        return {"error": f"API Error updating cluster async: {e.message}"}
    except Exception as e:
        print(f"Unexpected error updating cluster {cluster_name} async in {project_id}/{region}: {e}")
        return {"error": f"Unexpected error updating cluster async: {str(e)}"}

async def delete_dataproc_cluster_async(project_id: str, region: str, cluster_name: str):
    """
    Asynchronously deletes a specific Dataproc cluster.

    Args:
        project_id (str): The Google Cloud project ID.
        region (str): The Dataproc region (e.g., 'us-central1').
        cluster_name (str): The name of the cluster to delete.

    Returns:
        dict: A dictionary containing the LRO details or an error message.
    """
    try:
        # Create an async client
        cluster_client = dataproc.ClusterControllerAsyncClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

        # Make the async request
        request = dataproc.DeleteClusterRequest(
            project_id=project_id, region=region, cluster_name=cluster_name
        )
        operation = await cluster_client.delete_cluster(request=request)

        print(f"Initiated async cluster deletion for {cluster_name}. Operation: {operation.metadata.operation_type}")
        return {"status": "deleting", "operation_name": operation.operation.name, "metadata": str(operation.metadata)}

    except NotFound:
        print(f"Cluster {cluster_name} not found for async deletion in {project_id}/{region}.")
        return {"error": f"Cluster not found for async deletion: {cluster_name}"}
    except GoogleAPICallError as e:
        print(f"API Error deleting cluster {cluster_name} async in {project_id}/{region}: {e}")
        return {"error": f"API Error deleting cluster async: {e.message}"}
    except Exception as e:
        print(f"Unexpected error deleting cluster {cluster_name} async in {project_id}/{region}: {e}")
        return {"error": f"Unexpected error deleting cluster async: {str(e)}"}



def list_dataproc_clusters(project_id: str, region: str):
    """
    Lists all Dataproc clusters in a specific project and region.

    Args:
        project_id (str): The Google Cloud project ID.
        region (str): The Dataproc region (e.g., 'us-central1').

    Returns:
        list: A list of dictionaries, each containing basic info about a cluster,
              or an empty list if none are found or an error occurs.
    """
    cluster_client, _ = initialize_clients(region)
    if not cluster_client:
        return [{"error": "Dataproc cluster client not initialized."}]
    clusters = []
    try:
        request = dataproc.ListClustersRequest(project_id=project_id, region=region)
        # The list_clusters method handles pagination automatically.
        for cluster in cluster_client.list_clusters(request=request):
            cluster_data_dict = MessageToDict(
                cluster._pb,
                preserving_proto_field_name=True
            )
            print(cluster_data_dict)
            # Append the created dictionary to the clusters list
            clusters.append(cluster_data_dict)


        return clusters
    except GoogleAPICallError as e:
        print(f"API Error listing clusters in {project_id}/{region}: {e}")
        return [{"error": f"API Error listing clusters: {e.message}"}]
    except Exception as e:
        print(f"Unexpected error listing clusters in {project_id}/{region}: {e}")
        return [{"error": f"Unexpected error listing clusters: {str(e)}"}]