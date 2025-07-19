import argparse
import json
from pathlib import Path

from src.utils.logger import get_logger

logger = get_logger(__name__)


def clean_record(record: dict) -> dict:
    """
    Clean and simplify a single arXiv record.
    """
    return {
        'id': record.get('id'),
        'title': record.get('title', '').strip(),
        'abstract': record.get('abstract', '').strip(),
        'categories': record.get('categories', '').split(),
        'authors': [
            ' '.join([p for p in author if p]) for author in record.get('authors_parsed', [])
        ],
        'versions': record.get('versions', []),
        'update_date': record.get('update_date'),
        'submitter': record.get('submitter'),
        'journal_ref': record.get('journal-ref'),
        'doi': record.get('doi'),
    }


def clean_jsonl(input_path: Path, output_path: Path, start_line: int = 0):
    logger.info(f'Cleaning JSONL: {input_path} -> {output_path} starting from line {start_line}')

    total_processed = 0
    with (
        input_path.open('r', encoding='utf-8') as fin,
        output_path.open('w', encoding='utf-8') as fout,
    ):
        for i, line in enumerate(fin):
            if i < start_line:
                continue
            try:
                raw = json.loads(line)
                cleaned = clean_record(raw)
                json.dump(cleaned, fout, ensure_ascii=False)
                fout.write('\n')
                total_processed += 1
                if total_processed % 1000 == 0:
                    logger.info(f'Processed {total_processed} lines')
            except json.JSONDecodeError:
                logger.warning(f'Skipping invalid JSON at line {i}')
            except Exception as e:
                logger.warning(f'Skipping line {i} due to error: {e}')

    logger.info(f'Cleaning complete. Total records processed: {total_processed}')


def main():
    parser = argparse.ArgumentParser(description='Clean arXiv JSONL file')
    parser.add_argument('--input', required=True, help='Path to input JSONL file')
    parser.add_argument('--output', required=True, help='Path to output cleaned JSONL file')
    parser.add_argument(
        '--start-line', type=int, default=0, help='Line number to start processing from'
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    clean_jsonl(input_path, output_path, args.start_line)


if __name__ == '__main__':
    main()
