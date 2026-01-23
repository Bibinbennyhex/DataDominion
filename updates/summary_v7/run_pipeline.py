#!/usr/bin/env python3
"""
Summary Pipeline v7.0 - CLI Entry Point
========================================

Ultimate performance pipeline with all optimizations.

Usage:
    spark-submit run_pipeline.py --config config/pipeline_config.json
    spark-submit run_pipeline.py --config config/pipeline_config.json --resume abc123
"""

import argparse
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SummaryPipeline")


def parse_args():
    parser = argparse.ArgumentParser(
        description='Summary Pipeline v7.0 - Ultimate Performance'
    )
    parser.add_argument('--config', type=str, required=True, help='Config file path')
    parser.add_argument('--mode', type=str, default='incremental',
                       choices=['incremental', 'initial', 'forward', 'backfill'])
    parser.add_argument('--resume', type=str, help='Resume from run ID')
    parser.add_argument('--stats', action='store_true', help='Show stats only')
    parser.add_argument('--optimize', action='store_true', help='Optimize tables only')
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    return parser.parse_args()


def main():
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE v7.0 - ULTIMATE PERFORMANCE")
    logger.info("=" * 80)
    logger.info(f"Config: {args.config}")
    logger.info(f"Mode: {args.mode}")
    logger.info("=" * 80)
    
    from core import PipelineConfig, create_spark_session
    from orchestration import ParallelOrchestrator
    from utils import TableOptimizer
    
    config = PipelineConfig(args.config)
    spark = create_spark_session(config)
    
    try:
        if args.stats:
            optimizer = TableOptimizer(spark, config)
            for table in [config.summary_table, config.latest_summary_table]:
                count = spark.read.table(table).count()
                logger.info(f"{table}: {count:,} records")
            return
        
        if args.optimize:
            optimizer = TableOptimizer(spark, config)
            optimizer.optimize_tables(force=True)
            return
        
        # Run pipeline
        orchestrator = ParallelOrchestrator(spark, config)
        metrics = orchestrator.run(resume_run_id=args.resume)
        
        logger.info("=" * 80)
        logger.info("COMPLETE")
        logger.info(f"Duration: {metrics.duration_minutes:.2f} minutes")
        logger.info(f"Throughput: {metrics.overall_throughput:,.0f} records/sec")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.warning("Interrupted")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
