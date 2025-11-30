#!/usr/bin/env python3
"""
Manual runner for DLT pipelines.
Runs the DLT pipeline once and exits.
"""
from __future__ import annotations
import sys

# Add dlt directory to path for imports
sys.path.insert(0, "/app/dlt")

# Import the pipeline runner function from run.py
from run import run_pipelines


if __name__ == "__main__":
    # Get resource identifier from command-line argument if provided
    # Format: "pipeline_name" or "pipeline_name.resource_name"
    # Examples: "github_files" or "github_files.beefy_ui_chains"
    resource = sys.argv[1] if len(sys.argv) > 1 else None
    run_pipelines(resource)

