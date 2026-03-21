import argparse
from pathlib import Path

from ingestion.manager.collection_manager import CollectionManager
from ingestion.utils.logger import setup_logger


def _resolve_default_config() -> str:
    # Works when running from repo root or from an installed location
    local_config = Path(__file__).resolve().parent / "config" / "sources.yaml"
    if local_config.exists():
        return str(local_config)

    cwd_config = Path.cwd() / "config" / "sources.yaml"
    return str(cwd_config)


def main():
    parser = argparse.ArgumentParser(description="Research Paper Collector")
    parser.add_argument(
        "query",
        type=str,
        help='Search query for papers (e.g., "machine learning" or "neural networks")',
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["arxiv", "openalex", "pubmed", "semanticscholar"],
        help="Specific source to collect from (default: all enabled sources)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to sources configuration file",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show collection statistics and exit",
    )

    args = parser.parse_args()
    logger = setup_logger("main", "logs/collector.log")

    config_path = args.config or _resolve_default_config()
    if not Path(config_path).exists():
        raise FileNotFoundError(
            f"Config file not found at '{config_path}'. "
            f"Pass an explicit config with --config."
        )

    try:
        manager = CollectionManager(config_path)

        if args.stats:
            stats = manager.get_stats()
            logger.info("Collection Statistics:")
            for source, data in stats.items():
                logger.info(f"  {source}: {data['papers']} papers, {data['size_mb']:.2f} MB")
        else:
            logger.info(f"Starting collection for query: '{args.query}'")
            results = manager.collect_papers(args.query, source=args.source)

            logger.info("Collection Results:")
            for source, result in results.items():
                logger.info(f"  {source}: {result}")

            stats = manager.get_stats()
            logger.info("Updated Statistics:")
            for source, data in stats.items():
                logger.info(f"  {source}: {data['papers']} papers, {data['size_mb']:.2f} MB")

        manager.close()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()