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
    run_pipelines()

