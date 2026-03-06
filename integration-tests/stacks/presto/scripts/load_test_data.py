#!/usr/bin/env python3
"""
gRPC data loader for the presto-stack.

Sends 12 hardcoded CockroachDB IR metadata records via the MetadataIngestionService
Ingest RPC. The coordinator auto-creates the table on first insert via
TableProvisioner.ensureTable(), so no pre-existing schema is required.

Environment variables:
    GRPC_HOST      — coordinator hostname (default: coordinator)
    GRPC_PORT      — gRPC ingestion port (default: 9091)
    TABLE_NAME     — target table name (default: clp_cockroachdb)
"""

import logging
import os
import time

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
import ingestion_pb2
import ingestion_pb2_grpc

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

GRPC_HOST  = os.environ.get('GRPC_HOST', 'coordinator')
GRPC_PORT  = os.environ.get('GRPC_PORT', '9091')
TABLE_NAME = os.environ.get('TABLE_NAME', 'clp_cockroachdb')

URL_PREFIX = 'https://r2.yscope.io/prod/logging/cockroachdb/'
BASE_TS    = 1738368000_000_000_000   # 2025-02-01 00:00:00 UTC (nanoseconds)
HOUR       = 3_600_000_000_000       # 1 hour in nanoseconds

_RAW_RECORDS = [
    # node01, us-east-1a
    {'filename': 'crdb-node01-20250201T00.clp.zst', 'host': 'node01.crdb.us-east-1a', 'zone': 'us-east-1a', 'hour': 0, 'record_count': 84_210},
    {'filename': 'crdb-node01-20250201T01.clp.zst', 'host': 'node01.crdb.us-east-1a', 'zone': 'us-east-1a', 'hour': 1, 'record_count': 79_543},
    {'filename': 'crdb-node01-20250201T02.clp.zst', 'host': 'node01.crdb.us-east-1a', 'zone': 'us-east-1a', 'hour': 2, 'record_count': 71_882},
    # node02, us-east-1b
    {'filename': 'crdb-node02-20250201T00.clp.zst', 'host': 'node02.crdb.us-east-1b', 'zone': 'us-east-1b', 'hour': 0, 'record_count': 91_037},
    {'filename': 'crdb-node02-20250201T01.clp.zst', 'host': 'node02.crdb.us-east-1b', 'zone': 'us-east-1b', 'hour': 1, 'record_count': 88_601},
    {'filename': 'crdb-node02-20250201T02.clp.zst', 'host': 'node02.crdb.us-east-1b', 'zone': 'us-east-1b', 'hour': 2, 'record_count': 83_917},
    # node03, us-west-2a
    {'filename': 'crdb-node03-20250201T00.clp.zst', 'host': 'node03.crdb.us-west-2a', 'zone': 'us-west-2a', 'hour': 0, 'record_count': 67_244},
    {'filename': 'crdb-node03-20250201T01.clp.zst', 'host': 'node03.crdb.us-west-2a', 'zone': 'us-west-2a', 'hour': 1, 'record_count': 72_109},
    {'filename': 'crdb-node03-20250201T02.clp.zst', 'host': 'node03.crdb.us-west-2a', 'zone': 'us-west-2a', 'hour': 2, 'record_count': 69_855},
    # node04, us-west-2b
    {'filename': 'crdb-node04-20250201T00.clp.zst', 'host': 'node04.crdb.us-west-2b', 'zone': 'us-west-2b', 'hour': 0, 'record_count': 58_432},
    {'filename': 'crdb-node04-20250201T01.clp.zst', 'host': 'node04.crdb.us-west-2b', 'zone': 'us-west-2b', 'hour': 1, 'record_count': 61_773},
    {'filename': 'crdb-node04-20250201T02.clp.zst', 'host': 'node04.crdb.us-west-2b', 'zone': 'us-west-2b', 'hour': 2, 'record_count': 55_901},
]


def build_request(raw):
    n         = raw['record_count']
    min_ts    = BASE_TS + raw['hour'] * HOUR
    max_ts    = min_ts + HOUR - 1
    ir_path   = URL_PREFIX + raw['filename']

    record = ingestion_pb2.MetadataRecord(
        file=ingestion_pb2.FileFields(
            state='IR_CLOSED',
            min_timestamp=min_ts,
            max_timestamp=max_ts,
            record_count=n,
            ir=ingestion_pb2.IrFileInfo(
                clp_ir_storage_backend='http',
                clp_ir_path=ir_path,
                # clp_ir_bucket intentionally omitted — not used by the http backend
            ),
        ),
        dim=[
            ingestion_pb2.DimEntry(
                key='service',
                value=ingestion_pb2.DimensionValue(
                    str=ingestion_pb2.StringDimension(value='cockroachdb', max_length=128)
                ),
            ),
            ingestion_pb2.DimEntry(
                key='host',
                value=ingestion_pb2.DimensionValue(
                    str=ingestion_pb2.StringDimension(value=raw['host'], max_length=128)
                ),
            ),
            ingestion_pb2.DimEntry(
                key='zone',
                value=ingestion_pb2.DimensionValue(
                    str=ingestion_pb2.StringDimension(value=raw['zone'], max_length=128)
                ),
            ),
        ],
        agg=[
            ingestion_pb2.AggEntry(
                field='level', qualifier='info',
                agg_type=ingestion_pb2.GTE, int_val=n,
                alias_column='record_count',
            ),
            ingestion_pb2.AggEntry(
                field='level', qualifier='warn',
                agg_type=ingestion_pb2.GTE, int_val=int(n * 0.05),
            ),
            ingestion_pb2.AggEntry(
                field='level', qualifier='error',
                agg_type=ingestion_pb2.GTE, int_val=int(n * 0.02),
            ),
            ingestion_pb2.AggEntry(
                field='level', qualifier='fatal',
                agg_type=ingestion_pb2.GTE, int_val=int(n * 0.002),
            ),
        ],
    )
    return ingestion_pb2.IngestRequest(table_name=TABLE_NAME, record=record)


def connect(deadline=60):
    target  = f'{GRPC_HOST}:{GRPC_PORT}'
    channel = grpc.insecure_channel(target)
    health_stub = health_pb2_grpc.HealthStub(channel)
    stub = ingestion_pb2_grpc.MetadataIngestionServiceStub(channel)

    start = time.time()
    while True:
        try:
            resp = health_stub.Check(
                health_pb2.HealthCheckRequest(service=''),
                timeout=2,
            )
            if resp.status == health_pb2.HealthCheckResponse.SERVING:
                logger.info('Coordinator healthy at %s', target)
                return stub
            logger.warning('Coordinator not serving (status=%s)', resp.status)
        except grpc.RpcError as e:
            elapsed = time.time() - start
            if elapsed >= deadline:
                raise RuntimeError(
                    f'Could not connect to coordinator at {target} after {deadline}s'
                ) from e
            logger.warning('Waiting for coordinator (%ds elapsed): %s', int(elapsed), e.details())
        time.sleep(2)


def main():
    stub = connect()

    sent = 0
    for raw in _RAW_RECORDS:
        req  = build_request(raw)
        resp = stub.Ingest(req, timeout=10)
        if resp.accepted:
            sent += 1
        else:
            logger.warning('Record rejected: %s', resp.error)

    logger.info('Sent %d/%d records to table "%s"', sent, len(_RAW_RECORDS), TABLE_NAME)


if __name__ == '__main__':
    main()
