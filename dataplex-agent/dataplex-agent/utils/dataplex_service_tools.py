# Import necessary libraries
from google.cloud import dataplex_v1
from google.api_core.exceptions import GoogleAPICallError, NotFound, InvalidArgument
from google.protobuf.json_format import MessageToDict
from google.protobuf import field_mask_pb2
import time # For potential LRO polling delays in examples

# Note: Authentication is handled implicitly by the Google Cloud client libraries.
# Ensure your environment is authenticated (e.g., using `gcloud auth application-default login`).

# --- Helper Function for LROs (Optional) ---
def _handle_lro(operation, operation_description: str):
    """Helper to return LRO details or potential immediate errors."""
    
    if operation.exception():
        print(f"Error during {operation_description}: {operation.exception()}")
        return {"error": f"Error during {operation_description}: {operation.exception()}"}
    print(f"Initiated {operation_description}. Operation: {operation.operation.name}")
    
    return {"status": "pending", "operation_name": operation.operation.name, "metadata": str(operation.metadata)}

# --- Dataplex Service Client Functions ---

# --- Lake Operations ---

def create_dataplex_lake(project_id: str, location: str, lake_id: str, lake_details: dict):
    """
    Creates a Dataplex lake.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID to assign to the new lake.
        lake_details (dict): A dictionary representing the lake configuration.
                             Example: {'display_name': 'My Lake', 'description': 'Test lake', 'metastore': {'service': 'projects/p/locations/l/services/s'}}

    Returns:
        dict: A dictionary containing the LRO details for the creation operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        parent = client.common_location_path(project_id, location)

        # Construct the Lake object from the dictionary
        lake_obj = dataplex_v1.Lake(lake_details)

        request = dataplex_v1.CreateLakeRequest(
            parent=parent,
            lake_id=lake_id,
            lake=lake_obj,
            # validate_only=False # Optional: Set to True to validate without creating
        )

        operation = client.create_lake(request=request)
        
        return _handle_lro(operation, f"lake creation for '{lake_id}'")

    except InvalidArgument as e:
        print(f"Invalid argument creating lake {lake_id}: {e}")
        return {"error": f"Invalid argument: {e.message}"}
    except GoogleAPICallError as e:
        print(f"API Error creating lake {lake_id} in {project_id}/{location}: {e}")
        return {"error": f"API Error creating lake: {e.message}"}
    except Exception as e:
        print(f"Unexpected error creating lake {lake_id} in {project_id}/{location}: {e}")
        return {"error": f"Unexpected error creating lake: {str(e)}"}

def get_dataplex_lake(project_id: str, location: str, lake_id: str):
    """
    Retrieves details of a specific Dataplex lake.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the lake.

    Returns:
        dict: A dictionary containing lake details or an error message.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.lake_path(project_id, location, lake_id)
        request = dataplex_v1.GetLakeRequest(name=name)
        lake = client.get_lake(request=request)
        
        lake_dict = MessageToDict(lake._pb, preserving_proto_field_name=True)
        return lake_dict
    except NotFound:
        print(f"Lake '{lake_id}' not found in {project_id}/{location}.")
        return {"error": f"Lake not found: {lake_id}"}
    except GoogleAPICallError as e:
        print(f"API Error getting lake {lake_id} in {project_id}/{location}: {e}")
        return {"error": f"API Error getting lake: {e.message}"}
    except Exception as e:
        print(f"Unexpected error getting lake {lake_id} in {project_id}/{location}: {e}")
        return {"error": f"Unexpected error getting lake: {str(e)}"}

def list_dataplex_lakes(project_id: str, location: str, filter_str: str):
    """
    Lists Dataplex lakes in a specific project and location.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        filter_str (str, optional): A filter string (e.g., 'state = ACTIVE'). Defaults to None.

    Returns:
        list: A list of dictionaries, each containing info about a lake,
              or a list containing a single error dictionary.
    """
    lakes_list = []
    try:
        client = dataplex_v1.DataplexServiceClient()
        parent = client.common_location_path(project_id, location)
        request = dataplex_v1.ListLakesRequest(parent=parent, filter=filter_str)

        for lake in client.list_lakes(request=request):
            
            lake_dict = MessageToDict(lake._pb, preserving_proto_field_name=True)
            lakes_list.append(lake_dict)

        if not lakes_list:
             print(f"No lakes found in {project_id}/{location} matching filter '{filter_str}'.")
        return lakes_list

    except GoogleAPICallError as e:
        print(f"API Error listing lakes in {project_id}/{location}: {e}")
        return [{"error": f"API Error listing lakes: {e.message}"}]
    except Exception as e:
        print(f"Unexpected error listing lakes in {project_id}/{location}: {e}")
        return {"error": f"Unexpected error listing lakes: {str(e)}"}

def update_dataplex_lake(project_id: str, location: str, lake_id: str, update_mask: list[str], updated_lake_details: dict):
    """
    Updates a Dataplex lake.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the lake to update.
        update_mask (list[str]): Field paths to update (e.g., ['display_name', 'description']).
        updated_lake_details (dict): Dictionary with the new values for the fields in update_mask.

    Returns:
        dict: A dictionary containing the LRO details for the update operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        lake_name = client.lake_path(project_id, location, lake_id)

        # Construct the Lake object with only the updated fields and the name
        lake_obj = dataplex_v1.Lake(name=lake_name, **updated_lake_details)
        field_mask = field_mask_pb2.FieldMask(paths=update_mask)

        request = dataplex_v1.UpdateLakeRequest(
            lake=lake_obj,
            update_mask=field_mask,
            # validate_only=False # Optional
        )

        operation = client.update_lake(request=request)
        
        return _handle_lro(operation, f"lake update for '{lake_id}'")

    except NotFound:
        print(f"Lake '{lake_id}' not found for update in {project_id}/{location}.")
        return {"error": f"Lake not found for update: {lake_id}"}
    except InvalidArgument as e:
        print(f"Invalid argument updating lake {lake_id}: {e}")
        return {"error": f"Invalid argument: {e.message}"}
    except GoogleAPICallError as e:
        print(f"API Error updating lake {lake_id} in {project_id}/{location}: {e}")
        return {"error": f"API Error updating lake: {e.message}"}
    except Exception as e:
        print(f"Unexpected error updating lake {lake_id} in {project_id}/{location}: {e}")
        return {"error": f"Unexpected error updating lake: {str(e)}"}

def delete_dataplex_lake(project_id: str, location: str, lake_id: str):
    """
    Deletes a Dataplex lake.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the lake to delete.

    Returns:
        dict: A dictionary containing the LRO details for the deletion operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.lake_path(project_id, location, lake_id)
        request = dataplex_v1.DeleteLakeRequest(name=name)

        operation = client.delete_lake(request=request)
        
        return _handle_lro(operation, f"lake deletion for '{lake_id}'")

    except NotFound:
        print(f"Lake '{lake_id}' not found for deletion in {project_id}/{location}.")
        return {"error": f"Lake not found for deletion: {lake_id}"}
    except GoogleAPICallError as e:
        print(f"API Error deleting lake {lake_id} in {project_id}/{location}: {e}")
        return {"error": f"API Error deleting lake: {e.message}"}
    except Exception as e:
        print(f"Unexpected error deleting lake {lake_id} in {project_id}/{location}: {e}")
        return {"error": f"Unexpected error deleting lake: {str(e)}"}


# --- Zone Operations ---

def create_dataplex_zone(project_id: str, location: str, lake_id: str, zone_id: str, zone_details: dict):
    """
    Creates a Dataplex zone within a lake.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        zone_id (str): The ID to assign to the new zone.
        zone_details (dict): Dictionary representing zone config. Must include 'type' ('RAW' or 'CURATED')
                             and 'resource_spec' ({'location_type': 'SINGLE_REGION' or 'MULTI_REGION'}).
                             Example: {'display_name': 'Raw Zone', 'type_': 'RAW', 'resource_spec': {'location_type': 'SINGLE_REGION'}, 'discovery_spec': {'enabled': True}}
                             Note: Use 'type_' because 'type' is a reserved keyword.

    Returns:
        dict: A dictionary containing the LRO details for the creation operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        parent = client.lake_path(project_id, location, lake_id)

        # Handle 'type' keyword conflict
        if 'type' in zone_details:
            zone_details['type_'] = zone_details.pop('type')

        zone_obj = dataplex_v1.Zone(**zone_details)

        request = dataplex_v1.CreateZoneRequest(
            parent=parent,
            zone_id=zone_id,
            zone=zone_obj,
        )
        operation = client.create_zone(request=request)
        
        return _handle_lro(operation, f"zone creation for '{zone_id}' in lake '{lake_id}'")

    except InvalidArgument as e:
        print(f"Invalid argument creating zone {zone_id}: {e}")
        return {"error": f"Invalid argument: {e.message}"}
    except GoogleAPICallError as e:
        print(f"API Error creating zone {zone_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"API Error creating zone: {e.message}"}
    except Exception as e:
        print(f"Unexpected error creating zone {zone_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error creating zone: {str(e)}"}

def get_dataplex_zone(project_id: str, location: str, lake_id: str, zone_id: str):
    """
    Retrieves details of a specific Dataplex zone.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        zone_id (str): The ID of the zone.

    Returns:
        dict: A dictionary containing zone details or an error message.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.zone_path(project_id, location, lake_id, zone_id)
        request = dataplex_v1.GetZoneRequest(name=name)
        zone = client.get_zone(request=request)
        
        zone_dict = MessageToDict(zone._pb, preserving_proto_field_name=True)
        return zone_dict
    except NotFound:
        print(f"Zone '{zone_id}' not found in {project_id}/{location}/{lake_id}.")
        return {"error": f"Zone not found: {zone_id}"}
    except GoogleAPICallError as e:
        print(f"API Error getting zone {zone_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"API Error getting zone: {e.message}"}
    except Exception as e:
        print(f"Unexpected error getting zone {zone_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error getting zone: {str(e)}"}

def list_dataplex_zones(project_id: str, location: str, lake_id: str, filter_str: str):
    """
    Lists Dataplex zones within a specific lake.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        filter_str (str, optional): A filter string (e.g., 'type = RAW'). Defaults to None.

    Returns:
        list: A list of dictionaries, each containing info about a zone,
              or a list containing a single error dictionary.
    """
    zones_list = []
    try:
        client = dataplex_v1.DataplexServiceClient()
        parent = client.lake_path(project_id, location, lake_id)
        request = dataplex_v1.ListZonesRequest(parent=parent, filter=filter_str)

        for zone in client.list_zones(request=request):
            
            zone_dict = MessageToDict(zone._pb, preserving_proto_field_name=True)
            zones_list.append(zone_dict)

        if not zones_list:
             print(f"No zones found in {project_id}/{location}/{lake_id} matching filter '{filter_str}'.")
        return zones_list

    except NotFound: # If the parent lake doesn't exist
        print(f"Lake '{lake_id}' not found in {project_id}/{location} when listing zones.")
        return [{"error": f"Parent lake not found: {lake_id}"}]
    except GoogleAPICallError as e:
        print(f"API Error listing zones in {project_id}/{location}/{lake_id}: {e}")
        return [{"error": f"API Error listing zones: {e.message}"}]
    except Exception as e:
        print(f"Unexpected error listing zones in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error listing zones: {str(e)}"}

def update_dataplex_zone(project_id: str, location: str, lake_id: str, zone_id: str, update_mask: list[str], updated_zone_details: dict):
    """
    Updates a Dataplex zone.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        zone_id (str): The ID of the zone to update.
        update_mask (list[str]): Field paths to update (e.g., ['display_name', 'discovery_spec.enabled']).
        updated_zone_details (dict): Dictionary with the new values for the fields in update_mask.

    Returns:
        dict: A dictionary containing the LRO details for the update operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        zone_name = client.zone_path(project_id, location, lake_id, zone_id)

        # Handle 'type' keyword conflict if present in details (though type is immutable)
        if 'type' in updated_zone_details:
             updated_zone_details['type_'] = updated_zone_details.pop('type')

        zone_obj = dataplex_v1.Zone(name=zone_name, **updated_zone_details)
        field_mask = field_mask_pb2.FieldMask(paths=update_mask)

        request = dataplex_v1.UpdateZoneRequest(
            zone=zone_obj,
            update_mask=field_mask,
        )
        operation = client.update_zone(request=request)
        
        return _handle_lro(operation, f"zone update for '{zone_id}'")

    except NotFound:
        print(f"Zone '{zone_id}' not found for update in {project_id}/{location}/{lake_id}.")
        return {"error": f"Zone not found for update: {zone_id}"}
    except InvalidArgument as e:
        print(f"Invalid argument updating zone {zone_id}: {e}")
        return {"error": f"Invalid argument: {e.message}"}
    except GoogleAPICallError as e:
        print(f"API Error updating zone {zone_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"API Error updating zone: {e.message}"}
    except Exception as e:
        print(f"Unexpected error updating zone {zone_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error updating zone: {str(e)}"}

def delete_dataplex_zone(project_id: str, location: str, lake_id: str, zone_id: str):
    """
    Deletes a Dataplex zone.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        zone_id (str): The ID of the zone to delete.

    Returns:
        dict: A dictionary containing the LRO details for the deletion operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.zone_path(project_id, location, lake_id, zone_id)
        request = dataplex_v1.DeleteZoneRequest(name=name)
        operation = client.delete_zone(request=request)
        
        return _handle_lro(operation, f"zone deletion for '{zone_id}'")

    except NotFound:
        print(f"Zone '{zone_id}' not found for deletion in {project_id}/{location}/{lake_id}.")
        return {"error": f"Zone not found for deletion: {zone_id}"}
    except GoogleAPICallError as e:
        print(f"API Error deleting zone {zone_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"API Error deleting zone: {e.message}"}
    except Exception as e:
        print(f"Unexpected error deleting zone {zone_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error deleting zone: {str(e)}"}

# --- Asset Operations ---

def create_dataplex_asset(project_id: str, location: str, lake_id: str, zone_id: str, asset_id: str, asset_details: dict):
    """
    Creates a Dataplex asset within a zone.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        zone_id (str): The ID of the parent zone.
        asset_id (str): The ID to assign to the new asset.
        asset_details (dict): Dictionary representing asset config. Must include 'resource_spec'
                              ({'name': 'resource_name', 'type': 'STORAGE_BUCKET' or 'BIGQUERY_DATASET'}).
                              Example: {'display_name': 'My Bucket Asset', 'resource_spec': {'name': 'projects/p/buckets/b', 'type_': 'STORAGE_BUCKET'}, 'discovery_spec': {'enabled': True}}
                              Note: Use 'type_' because 'type' is a reserved keyword.

    Returns:
        dict: A dictionary containing the LRO details for the creation operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        parent = client.zone_path(project_id, location, lake_id, zone_id)

        # Handle 'type' keyword conflict in resource_spec
        if 'resource_spec' in asset_details and 'type' in asset_details['resource_spec']:
            asset_details['resource_spec']['type_'] = asset_details['resource_spec'].pop('type')

        asset_obj = dataplex_v1.Asset(**asset_details)

        request = dataplex_v1.CreateAssetRequest(
            parent=parent,
            asset_id=asset_id,
            asset=asset_obj,
        )
        operation = client.create_asset(request=request)
        
        return _handle_lro(operation, f"asset creation for '{asset_id}' in zone '{zone_id}'")

    except InvalidArgument as e:
        print(f"Invalid argument creating asset {asset_id}: {e}")
        return {"error": f"Invalid argument: {e.message}"}
    except GoogleAPICallError as e:
        print(f"API Error creating asset {asset_id} in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return {"error": f"API Error creating asset: {e.message}"}
    except Exception as e:
        print(f"Unexpected error creating asset {asset_id} in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return {"error": f"Unexpected error creating asset: {str(e)}"}

def get_dataplex_asset(project_id: str, location: str, lake_id: str, zone_id: str, asset_id: str):
    """
    Retrieves details of a specific Dataplex asset.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        zone_id (str): The ID of the parent zone.
        asset_id (str): The ID of the asset.

    Returns:
        dict: A dictionary containing asset details or an error message.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.asset_path(project_id, location, lake_id, zone_id, asset_id)
        request = dataplex_v1.GetAssetRequest(name=name)
        asset = client.get_asset(request=request)
        
        asset_dict = MessageToDict(asset._pb, preserving_proto_field_name=True)
        return asset_dict
    except NotFound:
        print(f"Asset '{asset_id}' not found in {project_id}/{location}/{lake_id}/{zone_id}.")
        return {"error": f"Asset not found: {asset_id}"}
    except GoogleAPICallError as e:
        print(f"API Error getting asset {asset_id} in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return {"error": f"API Error getting asset: {e.message}"}
    except Exception as e:
        print(f"Unexpected error getting asset {asset_id} in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return {"error": f"Unexpected error getting asset: {str(e)}"}

def list_dataplex_assets(project_id: str, location: str, lake_id: str, zone_id: str, filter_str: str):
    """
    Lists Dataplex assets within a specific zone.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        zone_id (str): The ID of the parent zone.
        filter_str (str, optional): A filter string (e.g., 'resource_spec.type = STORAGE_BUCKET'). Defaults to None.

    Returns:
        list: A list of dictionaries, each containing info about an asset,
              or a list containing a single error dictionary.
    """
    assets_list = []
    try:
        client = dataplex_v1.DataplexServiceClient()
        parent = client.zone_path(project_id, location, lake_id, zone_id)
        request = dataplex_v1.ListAssetsRequest(parent=parent, filter=filter_str)

        for asset in client.list_assets(request=request):
            
            asset_dict = MessageToDict(asset._pb, preserving_proto_field_name=True)
            assets_list.append(asset_dict)

        if not assets_list:
             print(f"No assets found in {project_id}/{location}/{lake_id}/{zone_id} matching filter '{filter_str}'.")
        return assets_list

    except NotFound: # If the parent zone doesn't exist
        print(f"Zone '{zone_id}' not found in {project_id}/{location}/{lake_id} when listing assets.")
        return [{"error": f"Parent zone not found: {zone_id}"}]
    except GoogleAPICallError as e:
        print(f"API Error listing assets in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return [{"error": f"API Error listing assets: {e.message}"}]
    except Exception as e:
        print(f"Unexpected error listing assets in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return [{"error": f"Unexpected error listing assets: {str(e)}"}]

def update_dataplex_asset(project_id: str, location: str, lake_id: str, zone_id: str, asset_id: str, update_mask: list[str], updated_asset_details: dict):
    """
    Updates a Dataplex asset.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        zone_id (str): The ID of the parent zone.
        asset_id (str): The ID of the asset to update.
        update_mask (list[str]): Field paths to update (e.g., ['display_name', 'discovery_spec.enabled']).
        updated_asset_details (dict): Dictionary with the new values for the fields in update_mask.

    Returns:
        dict: A dictionary containing the LRO details for the update operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        asset_name = client.asset_path(project_id, location, lake_id, zone_id, asset_id)

        # Handle 'type' keyword conflict if present (though resource_spec is immutable)
        if 'resource_spec' in updated_asset_details and 'type' in updated_asset_details['resource_spec']:
             updated_asset_details['resource_spec']['type_'] = updated_asset_details['resource_spec'].pop('type')

        asset_obj = dataplex_v1.Asset(name=asset_name, **updated_asset_details)
        field_mask = field_mask_pb2.FieldMask(paths=update_mask)

        request = dataplex_v1.UpdateAssetRequest(
            asset=asset_obj,
            update_mask=field_mask,
        )
        operation = client.update_asset(request=request)
        
        return _handle_lro(operation, f"asset update for '{asset_id}'")

    except NotFound:
        print(f"Asset '{asset_id}' not found for update in {project_id}/{location}/{lake_id}/{zone_id}.")
        return {"error": f"Asset not found for update: {asset_id}"}
    except InvalidArgument as e:
        print(f"Invalid argument updating asset {asset_id}: {e}")
        return {"error": f"Invalid argument: {e.message}"}
    except GoogleAPICallError as e:
        print(f"API Error updating asset {asset_id} in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return {"error": f"API Error updating asset: {e.message}"}
    except Exception as e:
        print(f"Unexpected error updating asset {asset_id} in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return {"error": f"Unexpected error updating asset: {str(e)}"}

def delete_dataplex_asset(project_id: str, location: str, lake_id: str, zone_id: str, asset_id: str):
    """
    Deletes a Dataplex asset.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        zone_id (str): The ID of the parent zone.
        asset_id (str): The ID of the asset to delete.

    Returns:
        dict: A dictionary containing the LRO details for the deletion operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.asset_path(project_id, location, lake_id, zone_id, asset_id)
        request = dataplex_v1.DeleteAssetRequest(name=name)
        operation = client.delete_asset(request=request)
        
        return _handle_lro(operation, f"asset deletion for '{asset_id}'")

    except NotFound:
        print(f"Asset '{asset_id}' not found for deletion in {project_id}/{location}/{lake_id}/{zone_id}.")
        return {"error": f"Asset not found for deletion: {asset_id}"}
    except GoogleAPICallError as e:
        print(f"API Error deleting asset {asset_id} in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return {"error": f"API Error deleting asset: {e.message}"}
    except Exception as e:
        print(f"Unexpected error deleting asset {asset_id} in {project_id}/{location}/{lake_id}/{zone_id}: {e}")
        return {"error": f"Unexpected error deleting asset: {str(e)}"}

# --- Task Operations ---

def create_dataplex_task(project_id: str, location: str, lake_id: str, task_id: str, task_details: dict):
    """
    Creates a Dataplex task within a lake.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        task_id (str): The ID to assign to the new task.
        task_details (dict): Dictionary representing task config. Must include 'trigger_spec'
                             ({'type': 'ON_DEMAND' or 'RECURRING'}), 'execution_spec' ({'service_account': '...', 'args': {...}}),
                             and one of 'spark', 'notebook', 'sql_script'.
                             Example: {'trigger_spec': {'type_': 'ON_DEMAND'}, 'execution_spec': {'service_account': 'sa@...', 'args': {'ARG1': 'val1'}}, 'spark': {'python_script_file': 'gs://...'}}
                             Note: Use 'type_' because 'type' is a reserved keyword.

    Returns:
        dict: A dictionary containing the LRO details for the creation operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        parent = client.lake_path(project_id, location, lake_id)

        # Handle 'type' keyword conflict in trigger_spec
        if 'trigger_spec' in task_details and 'type' in task_details['trigger_spec']:
            task_details['trigger_spec']['type_'] = task_details['trigger_spec'].pop('type')

        task_obj = dataplex_v1.Task(**task_details)

        request = dataplex_v1.CreateTaskRequest(
            parent=parent,
            task_id=task_id,
            task=task_obj,
        )
        operation = client.create_task(request=request)
        
        return _handle_lro(operation, f"task creation for '{task_id}' in lake '{lake_id}'")

    except InvalidArgument as e:
        print(f"Invalid argument creating task {task_id}: {e}")
        return {"error": f"Invalid argument: {e.message}"}
    except GoogleAPICallError as e:
        print(f"API Error creating task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"API Error creating task: {e.message}"}
    except Exception as e:
        print(f"Unexpected error creating task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error creating task: {str(e)}"}

def get_dataplex_task(project_id: str, location: str, lake_id: str, task_id: str):
    """
    Retrieves details of a specific Dataplex task.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        task_id (str): The ID of the task.

    Returns:
        dict: A dictionary containing task details or an error message.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.task_path(project_id, location, lake_id, task_id)
        request = dataplex_v1.GetTaskRequest(name=name)
        task = client.get_task(request=request)
        
        task_dict = MessageToDict(task._pb, preserving_proto_field_name=True)
        return task_dict
    except NotFound:
        print(f"Task '{task_id}' not found in {project_id}/{location}/{lake_id}.")
        return {"error": f"Task not found: {task_id}"}
    except GoogleAPICallError as e:
        print(f"API Error getting task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"API Error getting task: {e.message}"}
    except Exception as e:
        print(f"Unexpected error getting task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error getting task: {str(e)}"}

def list_dataplex_tasks(project_id: str, location: str, lake_id: str, filter_str: str):
    """
    Lists Dataplex tasks within a specific lake.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        filter_str (str, optional): A filter string (e.g., 'state = ACTIVE'). Defaults to None.

    Returns:
        list: A list of dictionaries, each containing info about a task,
              or a list containing a single error dictionary.
    """
    tasks_list = []
    try:
        client = dataplex_v1.DataplexServiceClient()
        parent = client.lake_path(project_id, location, lake_id)
        request = dataplex_v1.ListTasksRequest(parent=parent, filter=filter_str)

        for task in client.list_tasks(request=request):
            
            task_dict = MessageToDict(task._pb, preserving_proto_field_name=True)
            tasks_list.append(task_dict)

        if not tasks_list:
             print(f"No tasks found in {project_id}/{location}/{lake_id} matching filter '{filter_str}'.")
        return tasks_list

    except NotFound: # If the parent lake doesn't exist
        print(f"Lake '{lake_id}' not found in {project_id}/{location} when listing tasks.")
        return [{"error": f"Parent lake not found: {lake_id}"}]
    except GoogleAPICallError as e:
        print(f"API Error listing tasks in {project_id}/{location}/{lake_id}: {e}")
        return [{"error": f"API Error listing tasks: {e.message}"}]
    except Exception as e:
        print(f"Unexpected error listing tasks in {project_id}/{location}/{lake_id}: {e}")
        return [{"error": f"Unexpected error listing tasks: {str(e)}"}]

def update_dataplex_task(project_id: str, location: str, lake_id: str, task_id: str, update_mask: list[str], updated_task_details: dict):
    """
    Updates a Dataplex task.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        task_id (str): The ID of the task to update.
        update_mask (list[str]): Field paths to update (e.g., ['display_name', 'trigger_spec.disabled']).
        updated_task_details (dict): Dictionary with the new values for the fields in update_mask.

    Returns:
        dict: A dictionary containing the LRO details for the update operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        task_name = client.task_path(project_id, location, lake_id, task_id)

        # Handle 'type' keyword conflict if present
        if 'trigger_spec' in updated_task_details and 'type' in updated_task_details['trigger_spec']:
             updated_task_details['trigger_spec']['type_'] = updated_task_details['trigger_spec'].pop('type')

        task_obj = dataplex_v1.Task(name=task_name, **updated_task_details)
        field_mask = field_mask_pb2.FieldMask(paths=update_mask)

        request = dataplex_v1.UpdateTaskRequest(
            task=task_obj,
            update_mask=field_mask,
        )
        operation = client.update_task(request=request)
        
        return _handle_lro(operation, f"task update for '{task_id}'")

    except NotFound:
        print(f"Task '{task_id}' not found for update in {project_id}/{location}/{lake_id}.")
        return {"error": f"Task not found for update: {task_id}"}
    except InvalidArgument as e:
        print(f"Invalid argument updating task {task_id}: {e}")
        return {"error": f"Invalid argument: {e.message}"}
    except GoogleAPICallError as e:
        print(f"API Error updating task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"API Error updating task: {e.message}"}
    except Exception as e:
        print(f"Unexpected error updating task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error updating task: {str(e)}"}

def delete_dataplex_task(project_id: str, location: str, lake_id: str, task_id: str):
    """
    Deletes a Dataplex task.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        task_id (str): The ID of the task to delete.

    Returns:
        dict: A dictionary containing the LRO details for the deletion operation or an error.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.task_path(project_id, location, lake_id, task_id)
        request = dataplex_v1.DeleteTaskRequest(name=name)
        operation = client.delete_task(request=request)
        
        return _handle_lro(operation, f"task deletion for '{task_id}'")

    except NotFound:
        print(f"Task '{task_id}' not found for deletion in {project_id}/{location}/{lake_id}.")
        return {"error": f"Task not found for deletion: {task_id}"}
    except GoogleAPICallError as e:
        print(f"API Error deleting task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"API Error deleting task: {e.message}"}
    except Exception as e:
        print(f"Unexpected error deleting task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error deleting task: {str(e)}"}

def run_dataplex_task(project_id: str, location: str, lake_id: str, task_id: str):
    """
    Runs an on-demand execution of a Dataplex task.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        task_id (str): The ID of the task to run.

    Returns:
        dict: A dictionary containing the job details of the initiated run or an error message.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.task_path(project_id, location, lake_id, task_id)
        request = dataplex_v1.RunTaskRequest(name=name)
        response = client.run_task(request=request)
        print(f"Successfully initiated run for task '{task_id}'. Job ID: {response.job.name.split('/')[-1]}")
        job_dict = MessageToDict(response.job._pb, preserving_proto_field_name=True)
        
        return {"status": "started", "job": job_dict}
    except NotFound:
        print(f"Task '{task_id}' not found for run in {project_id}/{location}/{lake_id}.")
        return {"error": f"Task not found for run: {task_id}"}
    except GoogleAPICallError as e:
        print(f"API Error running task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"API Error running task: {e.message}"}
    except Exception as e:
        print(f"Unexpected error running task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return {"error": f"Unexpected error running task: {str(e)}"}

def get_dataplex_job(project_id: str, location: str, lake_id: str, task_id: str, job_id: str):
    """
    Retrieves details of a specific Dataplex job (task run).

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        task_id (str): The ID of the parent task.
        job_id (str): The ID of the job.

    Returns:
        dict: A dictionary containing job details or an error message.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.job_path(project_id, location, lake_id, task_id, job_id)
        request = dataplex_v1.GetJobRequest(name=name)
        job = client.get_job(request=request)
        
        job_dict = MessageToDict(job._pb, preserving_proto_field_name=True)
        return job_dict
    except NotFound:
        print(f"Job '{job_id}' not found for task '{task_id}' in {project_id}/{location}/{lake_id}.")
        return {"error": f"Job not found: {job_id}"}
    except GoogleAPICallError as e:
        print(f"API Error getting job {job_id} in {project_id}/{location}/{lake_id}/{task_id}: {e}")
        return {"error": f"API Error getting job: {e.message}"}
    except Exception as e:
        print(f"Unexpected error getting job {job_id} in {project_id}/{location}/{lake_id}/{task_id}: {e}")
        return {"error": f"Unexpected error getting job: {str(e)}"}

def list_dataplex_jobs(project_id: str, location: str, lake_id: str, task_id: str):
    """
    Lists Dataplex jobs (task runs) for a specific task.

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        task_id (str): The ID of the parent task.

    Returns:
        list: A list of dictionaries, each containing info about a job,
              or a list containing a single error dictionary.
    """
    jobs_list = []
    try:
        client = dataplex_v1.DataplexServiceClient()
        parent = client.task_path(project_id, location, lake_id, task_id)
        request = dataplex_v1.ListJobsRequest(parent=parent)

        for job in client.list_jobs(request=request):
            
            job_dict = MessageToDict(job._pb, preserving_proto_field_name=True)
            jobs_list.append(job_dict)

        if not jobs_list:
             print(f"No jobs found for task '{task_id}' in {project_id}/{location}/{lake_id}.")
        return jobs_list

    except NotFound: # If the parent task doesn't exist
        print(f"Task '{task_id}' not found in {project_id}/{location}/{lake_id} when listing jobs.")
        return [{"error": f"Parent task not found: {task_id}"}]
    except GoogleAPICallError as e:
        print(f"API Error listing jobs for task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return [{"error": f"API Error listing jobs: {e.message}"}]
    except Exception as e:
        print(f"Unexpected error listing jobs for task {task_id} in {project_id}/{location}/{lake_id}: {e}")
        return [{"error": f"Unexpected error listing jobs: {str(e)}"}]

def cancel_dataplex_job(project_id: str, location: str, lake_id: str, task_id: str, job_id: str):
    """
    Cancels a running Dataplex job (task run).

    Args:
        project_id (str): The Google Cloud project ID.
        location (str): The Google Cloud location (e.g., 'us-central1').
        lake_id (str): The ID of the parent lake.
        task_id (str): The ID of the parent task.
        job_id (str): The ID of the job to cancel.

    Returns:
        dict: An empty dictionary on success, or a dictionary with an error message.
    """
    try:
        client = dataplex_v1.DataplexServiceClient()
        name = client.job_path(project_id, location, lake_id, task_id, job_id)
        request = dataplex_v1.CancelJobRequest(name=name)
        client.cancel_job(request=request) # Returns None on success
        
        print(f"Successfully requested cancellation for job '{job_id}'.")
        return {"status": "cancellation_requested"}
    except NotFound:
        print(f"Job '{job_id}' not found for cancellation for task '{task_id}' in {project_id}/{location}/{lake_id}.")
        return {"error": f"Job not found for cancellation: {job_id}"}
    except GoogleAPICallError as e:
         # Check if the error is because the job is already done (common case)
        if e.code == 400 and ("invalid state" in str(e).lower() or "terminal state" in str(e).lower()):
             print(f"Job {job_id} is likely already in a terminal state and cannot be cancelled.")
             return {"error": f"Job {job_id} is already in a terminal state."}
        print(f"API Error cancelling job {job_id} in {project_id}/{location}/{lake_id}/{task_id}: {e}")
        return {"error": f"API Error cancelling job: {e.message}"}
    except Exception as e:
        print(f"Unexpected error cancelling job {job_id} in {project_id}/{location}/{lake_id}/{task_id}: {e}")
        return {"error": f"Unexpected error cancelling job: {str(e)}"}