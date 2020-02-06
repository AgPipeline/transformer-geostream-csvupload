"""Loads CSV files into GeoStreams
"""

import argparse
import csv
import datetime
import json
import logging
import os
from typing import Optional, Union
from urllib.parse import urlparse
import requests

from terrautils.betydb import get_sites_by_latlon
from terrautils.spatial import wkt_to_geojson

import configuration
import transformer_class

# Clowder and GeoStreams related definitions
CLOWDER_DEFAULT_URL = os.environ.get('CLOWDER_URL', 'https://terraref.ncsa.illinois.edu/clowder/')
CLOWDER_DEFAULT_KEY = os.environ.get('CLOWDER_KEY', '')
GEOSTREAMS_API_URL_PARTICLE = 'api/geostreams'

GEOSTREAMS_CSV_SENSOR_TYPE = 4

class __internal__():
    """Class for functions intended for internal use only for this file
    """
    def __init__(self):
        """Performs initialization of class instance
        """

    @staticmethod
    def get_geostreams_api_url(base_url: str, url_particles: Union[str, tuple, None]) -> str:
        """Returns the URL constructed from parameters
        Arguments:
            base_url: the base URL (assumes scheme:[//authority] with optional path)
            url_particles: additional strings to append to the end of the base url
        Return:
            Returns the formatted URL (guaranteed to not have a trailing slash)
        """
        def url_join(base_url: str, url_parts: tuple) -> str:
            """Internal function to create URLs in a consistent fashion
            Arguments:
                base_url: the starting part of the URL
                url_parts: the parts of the URL to join
            Return:
                The formatted URL
            """
            built_url = ''
            base_parse = urlparse(base_url)
            if base_parse.scheme:
                built_url = base_parse.scheme + '://'
            if base_parse.netloc:
                built_url += base_parse.netloc + '/'
            if base_parse.path and not base_parse.path == '/':
                built_url += base_parse.path.lstrip('/') + '/'

            joined_parts = '/'.join(url_parts).replace('//', '/').strip('/')

            return built_url + joined_parts

        if not url_particles:
            return url_join(base_url, tuple(GEOSTREAMS_API_URL_PARTICLE))

        if isinstance(url_particles, str):
            return url_join(base_url, (GEOSTREAMS_API_URL_PARTICLE, url_particles))

        formatted_particles = tuple(str(part) for part in url_particles)
        return url_join(base_url, tuple(GEOSTREAMS_API_URL_PARTICLE) + formatted_particles)

    @staticmethod
    def _common_geostreams_name_get(clowder_url: str, clowder_key: str, url_endpoint: str, name_query_key: str, name: str) -> \
            Optional[dict]:
        """Common function for retrieving data from GeoStreams by name
        Arguments:
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            url_endpoint: the endpoint to query (URL particle appended to the base URL, eg: 'streams')
            name_query_key: the name of the query portion of URL identifying the name to search on (eg: 'stream_name')
            name: the name to search on
        Return:
            Returns the found information, or None if not found
        """
        url = __internal__.get_geostreams_api_url(clowder_url, url_endpoint)
        params = {name_query_key: name}
        if clowder_key:
            params['key'] = clowder_key

        logging.debug("Calling geostreams url '%s' with params '%s'", url, str(params))
        resp = requests.get(url, params)
        resp.raise_for_status()

        for one_item in resp.json():
            if 'name' in one_item and one_item['name'] == name:
                logging.debug("Found %s '%s' = [%s]", name_query_key, name, one_item['id'])
                return one_item

        return None

    @staticmethod
    def common_geostreams_create(clowder_url: str, clowder_key: str, url_endpoint: str, request_body: str) -> Optional[str]:
        """Common function for creating an object in GeoStreams
        Arguments:
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            url_endpoint: the endpoint to query (URL particle appended to the base URL, eg: 'streams')
            request_body: the body of the request
        Return:
            Returns the ID of the created object or None if no ID was returned
        """
        url = __internal__.get_geostreams_api_url(clowder_url, url_endpoint)
        if clowder_key:
            url = url + '?key=' + clowder_key

        result = requests.post(url,
                               headers={'Content-type': 'application/json'},
                               data=request_body)
        result.raise_for_status()

        result_id = None
        result_json = result.json()
        if isinstance(result_json, dict) and 'id' in result_json:
            result_id = result_json['id']
            logging.debug("Created GeoStreams %s: id = '%s'", url_endpoint, result_id)
        else:
            logging.debug("Call to GeoStreams create %s returned no ID", url_endpoint)

        return result_id

    @staticmethod
    def get_sensor_by_name(sensor_name: str, clowder_url: str, clowder_key: str) -> Optional[dict]:
        """Returns the GeoStreams sensor information retrieved from Clowder
        Arguments:
            sensor_name: the name of the data sensor to retrieve
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
        Return:
            Returns the information on the sensor or None if the stream can't be found
        """
        return __internal__._common_geostreams_name_get(clowder_url, clowder_key, 'sensors', 'sensor_name', sensor_name)

    @staticmethod
    def get_stream_by_name(stream_name: str, clowder_url: str, clowder_key: str) -> Optional[dict]:
        """Returns the GeoStreams stream information retrieved from Clowder
        Arguments:
            stream_name: the name of the data stream to retrieve
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
        Return:
            Returns the information on the stream or None if the stream can't be found
        """
        return __internal__._common_geostreams_name_get(clowder_url, clowder_key, 'streams', 'stream_name', stream_name)

    @staticmethod
    def create_sensor(sensor_name: str, clowder_url: str, clowder_key: str, geom: dict, sensor_type: dict, region: str) -> str:
        """Create a new sensor in Geostreams.
        Arguments:
            sensor_name: name of new sensor to create
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            geom: GeoJSON object of sensor geometry
            sensor_type: JSON object with {"id", "title", and "sensorType"}
            region: region of sensor
        """
        body = {
            "name": sensor_name,
            "type": "Point",
            "geometry": geom,
            "properties": {
                "popupContent": sensor_name,
                "type": sensor_type,
                "name": sensor_name,
                "region": region
            }
        }

        return __internal__.common_geostreams_create(clowder_url, clowder_key, 'sensors', json.dumps(body))

    @staticmethod
    def create_stream(stream_name: str, clowder_url: str, clowder_key: str, sensor_id: str, geom: dict, properties=None) -> str:
        """Create the indicated GeoStream
        Arguments:
            stream_name: the name of the  data stream to retrieve
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            sensor_id: the ID of the sensor associated with the stream
            geom: the geometry of the stream to create
            properties: additional properties for the stream
        Return:
            The ID of the created stream
        """
        body = {
            "name": stream_name,
            "type": "Feature",
            "geometry": geom,
            "properties": {} if not properties else properties,
            "sensor_id": str(sensor_id)
        }

        return __internal__.common_geostreams_create(clowder_url, clowder_key, 'streams', json.dumps(body))

    @staticmethod
    def create_data_points(clowder_url: str, clowder_key: str, stream_id: str, data_point_list: list) -> None:
        """Uploads the data points to GeoStreams
        Arguments:
            clowder_url: the URL of the Clowder instance to access
            clowder_key: the key to use when accessing Clowder (can be None or '')
            stream_id: the ID of the stream to upload to
            data_point_list: the list of data points to upload
        """
        body = {
            "datapoints": data_point_list,
            "stream_id": str(stream_id)
        }

        __internal__.common_geostreams_create(clowder_url, clowder_key, 'datapoints/bulk', json.dumps(body))

    @staticmethod
    def get_matched_sites(clowder_url: str, clowder_key: str, plot_name: str, lat_lon: tuple, filter_date: str) -> dict:
        """Returns sensor metadata matching the plot
        Arguments:
            clowder_url: the URL of the Clowder instance to load the file to
            clowder_key: the key to use when accessing Clowder
            plot_name: name of plot to map data point into if possible, otherwise query BETY
            lat_lon: [latitude, longitude] tuple of data point location
            filter_date: date used to restrict number of sites returned from BETYdb
        Return:
            Returns the sites matching the plot and date. An empty dict may be returned
        """
        # SENSOR is the plot
        matched_sites = {}
        if plot_name:
            # If provided a plot name, see if the sensor exists before going into more expensive logic
            sensor_data = __internal__.get_sensor_by_name(plot_name, clowder_url, clowder_key)
            if sensor_data:
                matched_sites[sensor_data['id']] = {
                    "name": plot_name,
                    "geom": sensor_data['geometry']
                }

        if not matched_sites:
            # If we don't have existing sensor to use quickly, we must query geographically
            site_list = get_sites_by_latlon(lat_lon, filter_date)
            for one_site in site_list:
                plot_name = one_site['sitename']
                plot_geom = json.loads(wkt_to_geojson(one_site['geometry']))

                # Get existing sensor with this plot name from geostreams, or create if it doesn't exist
                sensor_data = __internal__.get_sensor_by_name(plot_name, clowder_url, clowder_key)
                if not sensor_data:
                    sensor_id = __internal__.create_sensor(plot_name, clowder_url, clowder_key, plot_geom,
                                                           {
                                                               "id": "MAC Field Scanner",
                                                               "title": "MAC Field Scanner",
                                                               "sensorType": GEOSTREAMS_CSV_SENSOR_TYPE
                                                           },
                                                           "Maricopa")
                    matched_sites[sensor_id] = {"name": plot_name, "geom": plot_geom}
                else:
                    sensor_id = sensor_data['id']
                    matched_sites[sensor_id] = {"name": plot_name, "geom": plot_geom}

        return matched_sites

    @staticmethod
    def create_datapoint(clowder_url: str, clowder_key: str, stream_id: str, geom: dict, start_time: str, end_time: str,
                         properties: dict = None) -> str:
        """Create a new data point in Geostreams
        Arguments:
            clowder_url: the URL of the Clowder instance to load the file to
            clowder_key: the key to use when accessing Clowder
            stream_id: id of stream to attach data point to
            geom: GeoJSON object of sensor geometry
            start_time: start time, in format 2017-01-25T09:33:02-06:00
            end_time: end time, in format 2017-01-25T09:33:02-06:00
            properties: JSON object with any desired properties
        Return:
            The ID of the created data point
        """
        body = {
            "start_time": start_time,
            "end_time": end_time,
            "type": "Point",
            "geometry": geom,
            "properties": properties,
            "stream_id": str(stream_id)
        }

        return __internal__.common_geostreams_create(clowder_url, clowder_key, 'datapoints', json.dumps(body))

    @staticmethod
    def create_datapoint_with_dependencies(clowder_traits_url: str, clowder_key: str, stream_prefix: str, lat_lon: tuple,
                                           start_time: str, end_time: str, metadata: dict = None, filter_date: str = '',
                                           geom: dict = None, plot_name: str = None) -> None:
        """ Submit traits CSV file to Clowder
        Arguments:
            clowder_traits_url: the URL of the Clowder instance to load the file to
            clowder_key: the key to use when accessing Clowder
            stream_prefix: prefix of stream to attach data point to
            lat_lon: [latitude, longitude] tuple of data point location
            start_time: start time, in format 2017-01-25T09:33:02-06:00
            end_time: end time, in format 2017-01-25T09:33:02-06:00
            metadata: JSON object with any desired properties
            filter_date: date used to restrict number of sites returned from BETYdb
            geom: geometry for data point (use plot if not provided)
            plot_name: name of plot to map data point into if possible, otherwise query BETY
        Exceptions:
            Raises RuntimeError exception if a Clowder URL is not specified
        """
        matched_sites = __internal__.get_matched_sites(clowder_traits_url, clowder_key, plot_name, lat_lon, filter_date)

        for sensor_id in matched_sites:
            plot_geom = matched_sites[sensor_id]["geom"]
            stream_name = "%s (%s)" % (stream_prefix, sensor_id)
            stream_data = __internal__.get_stream_by_name(stream_name, clowder_traits_url, clowder_key)
            if not stream_data:
                stream_id = __internal__.create_stream(stream_name, clowder_traits_url, clowder_key, sensor_id, plot_geom)
            else:
                stream_id = stream_data['id']

            logging.info("Posting datapoint to stream %s", stream_id)
            if not geom:
                geom = plot_geom
            __internal__.create_datapoint(clowder_traits_url, clowder_key, stream_id, geom, start_time, end_time, metadata)


def add_parameters(parser: argparse.ArgumentParser) -> None:
    """Adds parameters
    Arguments:
        parser: instance of argparse.ArgumentParser
    """
    parser.add_argument('--clowder_url', default=CLOWDER_DEFAULT_URL,
                        help="the url of the Clowder instance to access for GeoStreams (default '%s')" % CLOWDER_DEFAULT_URL)
    parser.add_argument('--clowder_key', default=CLOWDER_DEFAULT_KEY,
                        help="the key to use when accessing Clowder %s" %
                        ("(default: using environment value)" if CLOWDER_DEFAULT_KEY else ''))

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
        if os.path.splitext(one_file)[1].lower() == '.csv':
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
    # Process each CSV file into BETYdb
    start_timestamp = datetime.datetime.now()
    files_count = 0
    files_csv = 0
    lines_read = 0
    error_count = 0
    files_loaded = []
    for one_file in check_md['list_files']():
        files_count += 1
        if os.path.splitext(one_file)[1].lower() == '.csv':
            files_csv += 1

            # Make sure we can access the file
            if not os.path.exists(one_file):
                msg = "Unable to access csv file '%s'" % one_file
                logging.debug(msg)
                return {'code': -1000,
                        'error': msg}

            try:
                # Read in the lines from the file
                with open(one_file, 'r') as in_file:
                    reader = csv.DictReader(in_file)
                    files_loaded.append(one_file)
                    for row in reader:
                        centroid_lonlat = [row['lon'], row['lat']]
                        time_fmt = row['dp_time']
                        timestamp = row['timestamp']
                        dp_metadata = {
                            "source": row['source'],
                            "value": row['value']
                        }
                        trait = row['trait']

                        __internal__.create_datapoint_with_dependencies(transformer.args.clowder_url, transformer.args.clowder_key,
                                                                        trait, (centroid_lonlat[1], centroid_lonlat[0]), time_fmt,
                                                                        time_fmt, dp_metadata, timestamp)
                        lines_read += 1

            except Exception:
                logging.exception("Error reading CSV file '%s'. Continuing processing", os.path.basename(one_file))
                error_count += 1

    if files_csv <= 0:
        logging.info("No CSV files were found in the list of files to process")
    if error_count > 0:
        logging.error("Errors were found during processing")
        return {'code': -1001, 'error': "Too many errors occurred during processing. Please correct and try again"}

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
