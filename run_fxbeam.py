"""
FXBeam is a data processing pipeline to process tick and OHLCV data.

Main features of FXBeam:
* Take in tick data in CSV and convert it to OHLCV data
* Take in tick data in JSON and convert to OHLCV data
* [FUTURE] Take in OHLCV in CSV and convert to OHLCV data with a different window
* [FUTURE] Take in OHLCV in JSON and convert to OHLCV data with a different window
* [FUTURE] Support streaming
* [FUTURE] Support API format to not to save to file but be used in another pipeline

Author: Mark Cutajar
"""
import logging
import argparse

from fxbeam.fxbeam import FxBeam


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', '-i',
        dest='input',
        required=True,
        help='Google cloud storage or local files to process')

    parser.add_argument(
        '--output', '-o',
        dest='output',
        required=True,
        help='Output file name to write results to.')

    parser.add_argument(
        '--compression', '-c',
        dest='compression',
        required=False,
        default=None,
        help='Compression used for input files')

    parser.add_argument(
        '--file-type', '-ft',
        dest='file_type',
        required=False,
        default='json',
        help='The type of input files. One of json or csv. '
             'If json is provided, this needs to be new line delimited')

    args = parser.parse_args()
    fx_beam = FxBeam(
        args.input,
        args.output,
        window_size=300,
        pipeline_params=['--runner=DirectRunner'],
        # WHEN running in an Apache Spark Cluster
        # pipeline_params=['--runner=PortableRunner', '--job_endpoint=localhost:8099'],
        compression=args.compression,
        input_file_type=args.file_type
    )
    fx_beam.build()
    fx_beam.run()
