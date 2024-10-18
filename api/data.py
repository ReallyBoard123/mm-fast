"""
@created: 13.07.2021
@copyright: Motion Miners GmbH, Emil-Figge Str. 80, 44227 Dortmund, 2021

@brief: MMLabsData class
"""

import pathlib
import os
import json
import threading
from PIL import Image
from .api import MMLabsAPI
from .exceptions import MMLabsException


class MMLabsData:
    """
    MMLabs Data class
    """

    def __init__(self, api=None, data_dir="data/", offline_mode=False, verbose=False):
        """
        constructor

        :param api: The MMLabs API instance object
        :param data_dir: The caching data dir
        :param offline_mode: enable offline use
        """
        if api is None:
            self.api = MMLabsAPI()
        else:
            self.api = api
        self.verbose = verbose
        self.data_dir = data_dir
        self.set_data_dir(data_dir)
        self.offline_mode = offline_mode
        # hold token list of { "<process_uuid>": { token: "<token_base64>" } }
        self.tokens = {}
        # dict of processes { "<process_uuid>": {<process_metadata_structure>} }
        self.processes = {}
        # dict of metadata for processes { "<process_uuid>": {<measurements_structure>} }
        self.measurements = {}
        # uuid of selected process
        self.selected_process = None
        self.selected_process_subscribers = set()
        # uuid of selected measurement
        self.selected_measurement = None
        self.selected_measurement_subscribers = set()
        # now read local cache
        self.read_cache()

    def register_selected_process(self, who):
        self.selected_process_subscribers.add(who)

    def unregister_selected_process(self, who):
        self.selected_process_subscribers.discard(who)

    def dispatch_selected_process(self, process_uuid):
        for subscriber in self.selected_process_subscribers:
            subscriber.update_selected_process(process_uuid)

    def register_selected_measurement(self, who):
        self.selected_measurement_subscribers.add(who)

    def unregister_selected_measurement(self, who):
        self.selected_measurement_subscribers.discard(who)

    def dispatch_selected_measurement(self, measurement_uuid):
        for subscriber in self.selected_measurement_subscribers:
            subscriber.update_selected_measurement(measurement_uuid)

    def set_selected_process(self, process_uuid):
        self.selected_process = process_uuid
        self.dispatch_selected_process(process_uuid)

    def set_selected_measurement(self, measurement_uuid):
        self.selected_measurement = measurement_uuid
        self.dispatch_selected_measurement(measurement_uuid)

    def set_data_dir(self, data_dir):
        """
        Set and create the data caching dir where downloads will be stored
        :param data_dir: The caching data dir
        """
        self.data_dir = data_dir
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)

    def read_cache(self):
        """
        Read in local cache in set data_dir.
        """
        self.processes = {}
        # scan for process uuid directories
        for dp in os.scandir(self.data_dir):
            if dp.is_dir() and dp.name != ".ipynb_checkpoints":
                metadata_filename = dp.path + "/process_metadata.json"
                process_uuid = None

                if pathlib.Path(metadata_filename).is_file():
                    if self.verbose:
                        print("found process directory %s" % dp.name)

                    with open(metadata_filename, 'r') as file:
                        process_metadata = json.load(file)
                        process_uuid = process_metadata["uuid"]
                        self.processes[process_uuid] = process_metadata

                if process_uuid is not None:
                    # scan for token
                    token_filename = dp.path + "/token.json"
                    if pathlib.Path(token_filename).is_file():
                        if self.verbose:
                            print("reading token...")
                        with open(token_filename, 'r') as file:
                            token = json.load(file)
                            self.tokens[process_uuid] = token
                    else:
                        print("No token for process %s found!" % process_uuid)

                    # scan for local measurements
                    measurements_filename = dp.path + "/measurements.json"
                    if pathlib.Path(measurements_filename).is_file():
                        if self.verbose:
                            print("reading local measurements...")

                        with open(measurements_filename, 'r') as file:
                            process_measurements = json.load(file)
                            self.measurements[process_uuid] = process_measurements

                        # try to check if local measurement files exist
                        process_dir = self.data_dir + '/' + process_uuid
                        for m in self.measurements[process_uuid]:
                            measurement_dir = process_dir + '/' + m["uuid"]
                            pathlib.Path(measurement_dir).mkdir(
                                parents=True, exist_ok=True)

                            # mark measurement as not offline available (default)
                            m["offline"] = False

                            if not m["is_complete"]:
                                if self.verbose:
                                    print(
                                        "skipping measurement %s, as it is not yet completed" % (m["uuid"]))
                                # ToDo: check if start time is far away and mark as dead or zombie
                                continue

                            for f in self.processes[process_uuid]["available_measurement_files"]:
                                filename = measurement_dir + '/' + f
                                # test if file already exists
                                if pathlib.Path(filename).is_file():
                                    if self.verbose:
                                        print("skipping measurement %s file %s (cached)..." % (
                                            m["uuid"], f))
                                    m["offline"] = True
                    else:
                        print("No local measurements for process %s found!" %
                              process_uuid)

    def get_api_all_data(self, process_uuid, use_cache=True):
        """
        Get all data from API and prepare datastructures.
        Set initial API token first.

        :param process_uuid: the process uuid to retrieve all data from
        :param use_cache: if True do not download file again if existing
        """
        if not self.offline_mode:
            self.get_api_process_metadata(process_uuid)
            self.get_api_layout_image(process_uuid)
            self.get_api_measurements(process_uuid)

            complete_measurements = [measurement for measurement in self.measurements[process_uuid] if measurement["is_complete"]]
            print(f"Found {len(complete_measurements)} measurements to download.")

            # Download measurements in parallel since we have a lot of small files to process
            n = 5 # number of parallel connections
            chunks = [complete_measurements[i * n:(i + 1) * n] for i in range((len(complete_measurements) + n - 1) // n )]
            for chunk in chunks:
                threads = []
                for measurement in chunk:
                    thread = threading.Thread(target=self.get_api_all_measurement_files, args=(process_uuid, measurement["uuid"], use_cache,))
                    thread.start()
                    threads.append(thread)
                for thread in threads:
                    thread.join()

            print(f"Download completed.")
        else:
            raise MMLabsException("Disable offline mode first")

    def get_api_all_measurement_files(self, process_uuid, measurement_uuid, use_cache=True):
        """
        Download all measurement files for given uuids from API.

        :param measurement_uuid: the measurement uuid to retrieve all data from
        :param process_uuid: the process uuid to retrieve all data from
        :param use_cache: if True do not download file again if existing
        """
        self.__set_api_token(process_uuid)
        process_dir = self.data_dir + '/' + process_uuid
        measurement_dir = process_dir + '/' + measurement_uuid
        pathlib.Path(measurement_dir).mkdir(parents=True, exist_ok=True)

        for f in self.processes[process_uuid]["available_measurement_files"]:
            filename = measurement_dir + '/' + f
            # test if file already exists
            if pathlib.Path(filename).is_file() and use_cache:
                if self.verbose:
                    print("skipping measurement %s file %s (cached)..." %
                          (measurement_uuid, f))
                continue
            if self.verbose:
                print("downloading measurement %s file %s..." %
                      (measurement_uuid, f))
            try:
                fdata = self.api.get_measurement(measurement_uuid, f)
                file = open(filename, 'wb')
                file.write(fdata)
                file.close()
            except Exception as err:
                print("Error retrieving file %s (should not happen): %s" % (f, err))

    def add_api_token(self, token):
        """
        Add a (new) token for process data access. 
        Will try to retrieve process metadata from API and persist in local cache. 

        :param token: the exported MPI access token (Base64)
        """
        if token is None:
            raise MMLabsException("token not set")

        self.api.set_token(token)
        # any errors will raise exception here
        process_metadata = self.api.get_process_metadata()
        process_uuid = process_metadata["uuid"]

        if process_uuid is None:
            raise MMLabsException("Invalid process metadata")

        data_dir = self.data_dir + '/' + process_uuid
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)

        token_json = {}
        token_json["token"] = token

        # persist token
        self.tokens[process_uuid] = token_json
        with open(data_dir + '/token.json', 'w') as file:
            file.write(json.dumps(token_json))

        # persist metadata
        self.processes[process_uuid] = process_metadata
        with open(data_dir + '/process_metadata.json', 'w') as file:
            file.write(json.dumps(process_metadata))

        # get measurements
        self.get_api_measurements(process_uuid)

        # get layout image
        self.get_api_layout_image(process_uuid)

    def __set_api_token(self, process_uuid):
        """
        Set token which was added before for a specific process uuid for API access. 

        :param process_uuid: the process uuid
        """
        token = self.tokens.get(process_uuid)
        if token is None:
            raise MMLabsException(
                "unable to find token for process %s" % process_uuid)
        self.api.set_token(token["token"])

    def get_api_measurements(self, process_uuid):
        if process_uuid is None:
            raise MMLabsException(
                "API get_measurements: process uuid is not set!")

        self.__set_api_token(process_uuid)
        self.measurements[process_uuid] = self.api.get_measurements()
        data_dir = self.data_dir + '/' + process_uuid
        with open(data_dir + '/measurements.json', 'w') as file:
            file.write(json.dumps(self.measurements[process_uuid]))
        return self.measurements[process_uuid]

    def get_api_process_metadata(self, process_uuid):
        if process_uuid is None:
            raise MMLabsException(
                "API get_process_metadata: process uuid is not set!")
        self.__set_api_token(process_uuid)

        self.processes[process_uuid] = self.api.get_process_metadata()
        data_dir = self.data_dir + '/' + process_uuid
        with open(data_dir + '/process_metadata.json', 'w') as file:
            file.write(json.dumps(self.processes[process_uuid]))
        return self.processes[process_uuid]

    def get_layout_image(self, process_uuid: str) -> bytes:
        """
        Get the associated layout image of the provided process. Note that this method
        returns raw bytes containing the image data. Consult the examples to learn how
        to use it.

        Note:
          This method is an alias for `get_api_layout_image`
        
        :param process_uuid: The process UUID oh which the layout shall be returned.

        :return: The byte representation of the layout image.
        """
        return self.get_api_layout_image(process_uuid)
    
    def get_api_layout_image(self, process_uuid):
        if process_uuid is None:
            raise MMLabsException(
                "API get_api_layout_image: process uuid is not set!")
        self.__set_api_token(process_uuid)

        image_bytes = self.api.get_layout_image()

        # create path and save to disk
        data_dir = self.data_dir + '/' + process_uuid
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        image_path = data_dir + '/layout.png'
        self.layout_image_path = image_path
        f = open(self.layout_image_path, 'wb')
        f.write(image_bytes)
        f.close()
        self.__create_grayscale_image(image_path)
        return image_bytes

    def get_layout_image_path(self, process_uuid, absolute=False):
        """
        Get path of layout image in local filedir
        :param absolute: return absolute path
        """
        path = self.data_dir + '/' + process_uuid + '/layout.png'
        if absolute:
            return os.path.abspath(path)

        return path

    def get_measurements_df(self, process_uuid):
        """
        Get measurements as sorted pandas dataframe
        :param process_uuid: the process uuid
        """
        if process_uuid is None:
            raise MMLabsException("process uuid is not set!")
        import pandas as pd
        return pd.DataFrame.from_dict(
            self.measurements[process_uuid]).set_index('timestamp').sort_index(ascending=False)

    def __create_grayscale_image(self, path):
        # create grayscale image
        image_dir = os.path.dirname(path)
        img = Image.open(path).convert('LA')
        img.save(image_dir + '/layout_gray.png')

    def get_measurement_dir_path(self, process_uuid, measurement_uuid):
        fp = os.path.join(self.data_dir, process_uuid, measurement_uuid)
        return fp
