from __future__ import annotations
import dlt
from sources.github_files import github_files_source
from lib.config import configure_env
from lib.cli import parse_args, apply_args_to_source

async def main():
    args = parse_args()
    pipeline = dlt.pipeline(
        pipeline_name='github_files',
        dev_mode=False,
        staging="filesystem",
        progress="log",
        dataset_name='github_files',
        destination='clickhouse',
    )

    source = await github_files_source()
    source = apply_args_to_source(source, args)
    load_info = pipeline.run(source)

    # pretty print the information on data that was loaded
    print(load_info)

if __name__=='__main__':
    import asyncio

    configure_env()
    asyncio.run(main())