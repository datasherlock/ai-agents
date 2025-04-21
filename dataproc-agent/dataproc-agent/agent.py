# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import os
from zoneinfo import ZoneInfo
from google.cloud import dataproc_v1 as dataproc
import google.auth
from google.adk.agents import Agent
from .utils.cluster_controller_tools import *
from .utils.llm_config import *


_, project_id = google.auth.default()
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", project_id)
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "us-central1")
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "True")


root_agent = Agent(
    name="root_agent",
    model=root_model,
    instruction=root_prompt,
    tools=[list_dataproc_clusters, create_dataproc_cluster_async,
           get_dataproc_cluster_async, list_dataproc_clusters_async, update_dataproc_cluster_async, delete_dataproc_cluster_async],
)



