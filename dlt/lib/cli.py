from __future__ import annotations
from dataclasses import dataclass
import sys
from typing import Optional

@dataclass
class CliArgs:
    show_list: bool = False
    only_resource: Optional[str] = None

def parse_args() -> CliArgs:
    """Parse command line arguments."""
    # first arg is either a resource name or no args at all
    if len(sys.argv) == 1:
        return CliArgs()
    else:
        if sys.argv[1] == "--list":
            return CliArgs(show_list=True)
        return CliArgs(only_resource=sys.argv[1])


def apply_args_to_source(source: Any, args: CliArgs) -> Any:
    if args.show_list:
        print(source.resources.keys())
        sys.exit(0)

    """Apply command line arguments to a source."""
    if args.only_resource:
        # make sure the resource exists
        if args.only_resource not in source.resources.keys():
            raise ValueError(f"Resource {args.only_resource} not found in source {source.name}")
        source = source.with_resources(args.only_resource)

    return source