
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
