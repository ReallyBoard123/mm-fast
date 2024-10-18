"""
@created: 13.07.2021
@copyright: Motion Miners GmbH, Emil-Figge Str. 80, 44227 Dortmund, 2021

@brief: MMLabsAPI class
"""

import requests
from .exceptions import MMLabsException


class MMLabsAPI:
    """
    MMLabs API calls
    """

    def __init__(self, base_url='https://mpi.motionminers.com/api/4labs/v2/', token=None):
        """
        constructor
        
        :param base_url: The MMLabs API endpoint
        :param token: The authorization token as encoded Base64
        """
        self.base_url = base_url
        self.token = token

    def set_token(self, token):
        """
        set authorization token to interact with API
        
        :param token: The authorization token as encoded Base64
        """
        self.token = token

    def get_layout_image(self):
        """
        get layout image from API
          '<base_url>/layout_image', methods=['GET']

        :returns: image as byte array
        :rtype: bytearray
        """
        r = self.__call_api(method='layout_image')
        return r.content

    def get_measurements(self):
        """
        get list of available measurements from API
          '<base_url>/measurements', methods=['GET']

        :returns: measurements structure
        :rtype: list
        """
        r = self.__call_api(method='measurements')
        return r.json()

    def get_measurement(self, uuid, filename):
        """
        get single measurement file from API
          '<base_url>/measurements/<uuid:measurement_uuid>/files/<string:file_name>', methods=['GET']
          
        :param uuid: The measurement uuid
        :param filename: The filename to download
        :returns: file as byte array
        :rtype: bytearray
        """
        method = f'measurements/{uuid}/files/{filename}'
        if uuid is None:
            raise MMLabsException('Missing measurement UUID parameter')
        if filename is None:
            raise MMLabsException('Missing filename parameter')
        return self.__call_api(method=method).content

    def get_process_metadata(self):
        """
        get process metadata from API
          '<base_url>/process_metadata', methods=['GET']

        :returns: process metadata structure
        :rtype: dict
        """
        r = self.__call_api(method='process_metadata')
        return r.json()

    def __call_api(self, method):
        """
        call API with authorization added
        
        :param method: The method to call on API endpoint
        :returns: the method call result
        """
        if method is None:
            raise MMLabsException('Missing method')
        if self.token is None:
            raise MMLabsException('Token not set')
        headers = {'Authorization': f'Bearer {self.token}'}
        url = f'{self.base_url}{method}'
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r
