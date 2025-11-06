# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
"""
This is an entrypoint to the orchestration features of ai4s.jobq.

If users import from this file, the autocomplete will be uncluttered and auto-documenting.
"""

from ai4s.jobq.orchestration.manager import batch_enqueue, get_results, launch_workers
from ai4s.jobq.work import WorkSpecification

__all__ = ["WorkSpecification", "launch_workers", "batch_enqueue", "get_results"]
