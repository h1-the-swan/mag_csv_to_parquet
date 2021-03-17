# -*- coding: utf-8 -*-

DESCRIPTION = """Get S2 and MAG IDs from redshift database."""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)

import logging
root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData, Table

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

DB_REDSHIFT = {
    'dbname': os.environ.get('S2_REDSHIFT_DEV_DBNAME', None),
    'host': os.environ.get('S2_REDSHIFT_DEV_HOST', None),
    'user': os.environ.get('S2_REDSHIFT_DEV_LOGIN', None),
    'password': os.environ.get('S2_REDSHIFT_DEV_PASSWORD', None),
    'port': os.environ.get('S2_REDSHIFT_DEV_PORT', None),

}


def main(args):
    engine = create_engine('postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'.format(**DB_REDSHIFT))
    sq = """
    SELECT paper_sha, corpus_paper_id, fields_of_study, mag_id FROM content.raw_papers rp WHERE mag_id IS NOT NULL;
    """
    outfp = Path(args.output)
    if outfp.exists():
        raise FileExistsError(f"{outfp} already exists")
    logger.debug(f"reading from database. SQL: {sq}")
    df = pd.read_sql(sq, engine)
    logger.debug(f"dataframe shape: {df.shape}")

    # logger.debug("dropping rows with missing mag_id")
    # df.dropna(subset=['mag_id'], inplace=True)
    # logger.debug(f"dataframe shape: {df.shape}")

    logger.debug("exploding mag_id column (for rows with multiple MAG IDs")
    df['mag_id'] = df.mag_id.apply(lambda x: x.split(','))
    df = df.explode('mag_id')
    logger.debug(f"dataframe shape: {df.shape}")

    logger.debug(f"saving to {outfp}")
    df.to_parquet(outfp, index=False)

if __name__ == "__main__":
    total_start = timer()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s", datefmt="%H:%M:%S"))
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    logger.info("pid: {}".format(os.getpid()))
    import argparse
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("output", help="output file (parquet)")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = timer()
    logger.info('all finished. total time: {}'.format(format_timespan(total_end-total_start)))
