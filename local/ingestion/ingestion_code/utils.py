import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('../logs/ingestion.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)
