"""
@created: 13.07.2021
@copyright: Motion Miners GmbH, Emil-Figge Str. 80, 44227 Dortmund, 2021

@brief: Custom exceptions
"""

class MMLabsException(Exception):
    """
    MMLabs simple Exception class
    """
    pass


class EmptyTimeSeriesException(Exception):
    """
    TimeSeries does not contain any elements, but requires presence of elements for processing
    """
    pass
