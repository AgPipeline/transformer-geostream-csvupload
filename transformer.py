"""Loads CSV files into BETYdb
"""

import argparse
import datetime
import logging
import os
from typing import Optional
import requests
import terrautils.betydb as betydb

import configuration
import transformer_class

class __internal__():
    """Class for functions intended for internal use only for this file
    """
    def __init__(self):
        """Performs initialization of class instance
        """

    @staticmethod
    def submit_traits(filename: str, file_type: str = 'csv', betydb_key: str = '', betydb_traits_url: str = '') -> Optional[list]:
        """ Submit traits file to BETY; can be CSV, JSON or XML.
        Arguments:
            filename: the name of the file to upload to BETYdb
            file_type: Identification of the type of file to upload
            betydb_key: the key to use when accessing BETYdb
            betydb_traits_url: the URL of the BETYdb to load the file to
        """
        # set defaults if necessary
        if not betydb_key:
            betydb_key = betydb.get_bety_key()
        if not betydb_traits_url:
            betydb_url = betydb.get_bety_api('traits')
        else:
            betydb_url = betydb_traits_url

        request_payload = {'key': betydb_key}

        if file_type == 'csv':
            content_type = 'text/csv'
        elif file_type == 'json':
            content_type = 'application/json'
        elif file_type == 'xml':
            content_type = 'application/xml'
        else:
            logging.error("Unsupported file type.")
            return None

        resp = requests.post("%s.%s" % (betydb_url, file_type), params=request_payload,
                             data=open(filename, 'rb').read(),
                             headers={'Content-type': content_type})

        if resp.status_code in [200, 201]:
            logging.info("Data successfully submitted to BETYdb.")
            return resp.json()['data']['ids_of_new_traits']

        logging.error("Error submitting data to BETYdb: %s -- %s", resp.status_code, resp.reason)
        resp.raise_for_status()

        return None

def add_parameters(parser: argparse.ArgumentParser) -> None:
    """Adds parameters
    Arguments:
        parser: instance of argparse.ArgumentParser
    """
    parser.add_argument('--BETYDB_URL', dest='betydb_url',
                        help='the url of the BETYdb instance to access (can also define BETYDB_URL environment variable)')
    parser.add_argument('--BETYDB_KEY', dest='betydb_key',
                        help='the key to use when accessing BETYdb (can also define BETYDB_KEY environment variable)')

    # Here we specify a default metadata file that we provide to get around the requirement while also allowing
    # pylint: disable=protected-access
    for action in parser._actions:
        if action.dest == 'metadata' and not action.default:
            action.default = ['/home/extractor/default_metadata.json']
            break


def check_continue(transformer: transformer_class.Transformer, check_md: dict) -> tuple:
    """Checks if conditions are right for continuing processing
    Arguments:
        transformer: instance of transformer class
        check_md: request specific metadata
    Return:
        Returns a tuple containing the return code for continuing or not, and
        an error message if there's an error
    """
    # pylint: disable=unused-argument
    for one_file in check_md['list_files']():
        if os.path.splitext(one_file)[1] == '.csv':
            return tuple([0])

    return -1, "Unable to find a CSV file in the list of files"


def perform_process(transformer: transformer_class.Transformer, check_md: dict) -> dict:
    """Performs the processing of the data
    Arguments:
        transformer: instance of transformer class
        check_md: request specific metadata
    Return:
        Returns a dictionary with the results of processing
    """
    # Determine the BETYdb information to pass
    runtime_betydb_url = '' if not transformer.args.betydb_url else os.path.join(transformer.args.betydb_url, 'api/v1/traits')
    runtime_betydb_key = '' if not transformer.args.betydb_key else transformer.args.betydb_key

    # Process each CSV file into BETYdb
    start_timestamp = datetime.datetime.now()
    files_count = 0
    files_csv = 0
    lines_read = 0
    files_loaded = []
    for one_file in check_md['list_files']():
        files_count += 1
        if os.path.splitext(one_file)[1] == '.csv':
            files_csv += 1

            # Make sure we can access the file
            if not os.path.exists(one_file):
                msg = "Unable to access csv file '%s'" % one_file
                logging.debug(msg)
                return {'code': -1000,
                        'error': msg}

            # Read in the lines from the file
            input_lines = None
            with open(one_file, 'r') as in_file:
                input_lines = in_file.readlines()

            # Only load into BETYdb if there are lines to write
            if input_lines:
                files_loaded.append(one_file)
                lines_read += len(input_lines) - 1  # Assumes there's a header
                __internal__.submit_traits(one_file, betydb_traits_url=runtime_betydb_url, betydb_key=runtime_betydb_key)

    if files_csv <= 0:
        logging.info("No CSV files were found in the list of files to process")

    return {
        'code': 0,
        configuration.TRANSFORMER_NAME: {
            'version': configuration.TRANSFORMER_VERSION,
            'utc_timestamp': datetime.datetime.utcnow().isoformat(),
            'processing_time': str(datetime.datetime.now() - start_timestamp),
            'num_files_received': str(files_count),
            'num_csv_files': str(files_csv),
            'lines_loaded': str(lines_read),
            'files_processed': str(files_loaded)
        }
    }
