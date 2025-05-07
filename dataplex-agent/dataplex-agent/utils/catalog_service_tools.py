from google.cloud import dataplex_v1
# We still might need types for processing the *response* enums
from google.cloud.dataplex_v1 import types
from google.api_core.exceptions import GoogleAPIError

def search_dataplex_catalog(project_id: str, location_id: str, query: str) -> list[dict]:
    """
    Searches for Dataplex entries within a specific project and location
    by passing arguments directly to the client method.

    Args:
        project_id: The Google Cloud project ID.
        location_id: The Dataplex location (e.g., 'us-central1').
        query: The search query string (e.g., table name, bucket name,
               fileset name, or keywords). Performs a keyword-like search.

    Returns:
        A list of dictionaries, where each dictionary represents a found
        Dataplex search result entry. Returns an empty list if no entries
        are found or an error occurs.

    Raises:
        GoogleAPIError: If an error occurs during the API call.
        ValueError: If input arguments are invalid.
    """
    if not all([project_id, location_id, query]):
        raise ValueError("project_id, location_id, and query must be provided.")

    scope = f"projects/{project_id}/locations/{location_id}"

    # Create a Dataplex Metadata Service client
    client = dataplex_v1.MetadataServiceClient()

    found_entries = []

    print(f"Searching Dataplex in '{scope}' for query: '{query}'...")

    try:
        # Perform the search by passing arguments directly to the method
        pager = client.search_resources(
            name=scope,
            query=query
            # page_size=100 # Optional
        )

        # Iterate through the results
        for result in pager:
            # Process response enums using the types module
            system_str = types.SearchResourcesResult.SearchResultSystem(result.system).name
            type_str = types.EntryType(result.type_).name

            entry_details = {
                "dataplex_entry_name": result.name,
                "display_name": result.display_name,
                "description": result.description,
                "linked_resource": result.linked_resource,
                "system": system_str,
                "type": type_str,
                "relative_resource_name": result.relative_resource_name
            }
            found_entries.append(entry_details)

        print(f"Found {len(found_entries)} entries.")
        return found_entries

    except GoogleAPIError as e:
        print(f"An API error occurred: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

