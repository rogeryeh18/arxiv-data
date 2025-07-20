import argparse
import json
from pathlib import Path

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


def jsonl_to_parquet(input_path: Path, output_path: Path):
    logger.info(f'Loading JSONL from {input_path}')

    records = []
    with input_path.open('r', encoding='utf-8') as fin:
        for i, line in enumerate(fin):
            try:
                record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError:
                logger.warning(f'Skipping line {i} due to invalid JSON')

    logger.info(f'Loaded {len(records)} records. Converting to DataFrame...')
    df = pd.DataFrame(records)

    logger.info(f'Saving to Parquet: {output_path}')
    df.to_parquet(output_path, index=False)
    logger.info('Conversion complete.')


def main():
    parser = argparse.ArgumentParser(description='Convert cleaned JSONL to Parquet')
    parser.add_argument('--input', required=True, help='Path to cleaned JSONL input file')
    parser.add_argument('--output', required=True, help='Path to output Parquet file')
    args = parser.parse_args()

    jsonl_to_parquet(Path(args.input), Path(args.output))


if __name__ == '__main__':
    main()
