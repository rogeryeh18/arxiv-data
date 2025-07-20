import argparse
import json
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import List

from elasticsearch import Elasticsearch, helpers

from src.utils.logger import get_logger

logger = get_logger(__name__)

INDEX_NAME = 'arxiv-papers'

DEFAULT_MAPPING = {
    'mappings': {
        'properties': {
            'id': {'type': 'keyword'},
            'title': {'type': 'text'},
            'abstract': {'type': 'text'},
            'categories': {'type': 'keyword'},
            'authors': {'type': 'keyword'},
            'update_date': {'type': 'date'},
            'submitter': {'type': 'keyword'},
            'journal_ref': {'type': 'text'},
            'doi': {'type': 'keyword'},
            'versions': {
                'type': 'nested',
                'properties': {'version': {'type': 'keyword'}, 'created': {'type': 'date'}},
            },
            '__ingest_timestamp': {
                'type': 'date',
            },
        }
    }
}


def normalize_dates(record: dict) -> dict:
    if 'versions' in record:
        for v in record['versions']:
            try:
                if isinstance(v.get('created'), str):
                    v['created'] = parsedate_to_datetime(v['created']).isoformat()
            except Exception as e:
                logger.warning(
                    f'Failed to parse created date in versions: {v.get("created")} — {e}'
                )
    return record


def create_index_if_not_exists(es: Elasticsearch, index: str):
    if not es.indices.exists(index=index):
        logger.info(f'Creating index: {index}')
        es.indices.create(index=index, body=DEFAULT_MAPPING)
    else:
        logger.info(f'Index {index} already exists')


def load_jsonl_lines(jsonl_path: Path, start_line: int = 0):
    with jsonl_path.open('r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i < start_line:
                continue
            try:
                record = json.loads(line)
                record['__ingest_timestamp'] = datetime.now().isoformat()
                record = normalize_dates(record)
                yield record
            except json.JSONDecodeError:
                logger.warning(f'Skipping invalid JSON at line {i}')


def bulk_ingest(es: Elasticsearch, index: str, records: List[dict]):
    actions = [{'_index': index, '_id': rec.get('id'), '_source': rec} for rec in records]
    helpers.bulk(es, actions)


def main():
    parser = argparse.ArgumentParser(description='Ingest cleaned arXiv JSONL into Elasticsearch')
    parser.add_argument('--input', required=True, help='Path to cleaned JSONL file')
    parser.add_argument(
        '--batch-size', type=int, default=1000, help='Number of records per bulk upload'
    )
    parser.add_argument('--start-line', type=int, default=0, help='Line number to start from')
    parser.add_argument('--index', type=str, default=INDEX_NAME, help='Elasticsearch index name')
    args = parser.parse_args()

    es = Elasticsearch('http://host.docker.internal:9200')
    create_index_if_not_exists(es, args.index)

    batch = []
    for i, record in enumerate(load_jsonl_lines(Path(args.input), start_line=args.start_line)):
        batch.append(record)
        if len(batch) >= args.batch_size:
            logger.info(f'Ingesting records {i - len(batch) + 1} to {i}')
            bulk_ingest(es, args.index, batch)
            batch.clear()

    if batch:
        logger.info(f'Ingesting final {len(batch)} records')
        bulk_ingest(es, args.index, batch)

    logger.info('Ingestion complete.')


if __name__ == '__main__':
    main()
