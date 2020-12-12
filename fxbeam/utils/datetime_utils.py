from datetime import datetime

import apache_beam as beam


class ToTimestamp(beam.DoFn):
    """ParDo to convert a datetime string to a timestamp object"""
    def process(self, element, string_format, datetime_key='date', timestamp_key=None, **kwargs):
        """
        :param element: Element being processed
        :param string_format: Format of the datetime string to be converted to a datetime format
        :param datetime_key: Field containing date field
        :param timestamp_key: New field for timestamp. If given the original key is deleted.
            If this is not given, the original key name is used.
        :return: Elements with a datetime filed
        """
        timestamp = datetime.strptime(element[datetime_key], string_format).timestamp()

        if timestamp_key:
            del element[datetime_key]
            output_key = timestamp_key
        else:
            output_key = datetime_key

        yield {
            **element,
            output_key: timestamp,
        }


class AddTimestamp(beam.DoFn):
    """ParDo to convert a datetime string to a timestamp and assign to element"""
    def process(self, element, timestamp_key, delete_key=False, **kwargs):
        """
        :param element: Element being processed
        :param timestamp_key: Field containing the timestamp
        :param delete_key: Whether to delete the original timestamp or not
        :return: Elements with a datetime filed
        """
        timestamp = element.pop(timestamp_key) if delete_key else element.get(timestamp_key)
        yield beam.window.TimestampedValue(element, timestamp)


class AddWindowStartTimestampToElement(beam.DoFn):
    """ParDo to add a window timestamp to the element"""
    def process(self, element,  timestamp_key='timestamp', window=beam.DoFn.WindowParam):
        """
        :param element: Element being processed
        :param timestamp_key: The key to assign the timestamp in
        :param window: The window being used when processing
        :return: Elements with the inserted timestamp key with the
            start value of the window
        """
        window_start = window.start.to_utc_datetime().timestamp()
        yield {
            **element,
            timestamp_key: window_start
        }


class TimestampToTimeGroup(beam.DoFn):
    """ParDo function to extract a unique key for each time window.

    By dividing the timestamp by the window size and flooring we get
    a unique key since the timestamp is an ever increasing integer number
    from 1st January 1970.
    """
    def process(self, element, window_size, time_group_key):
        """
        :param element: Element being processed
        :param window_size: The size of the window in seconds to be used
        :param time_group_key: The name of the key assigned with the time
            group value.
        :return: Elements with a new (or substituted) key with the time group.
        """
        yield {
            **element,
            time_group_key: element['timestamp'] // window_size
        }


class TimeGroupToTimestamp(beam.DoFn):
    """ParDo function to convert the timegroup key into a timestamp

    Since in the above function we divide the timestamp by window size
    to create the timegroup key, in this function we do the inverse.

    NOTE: The window size NEEDS TO BE THE SAME, or else data would be
    distorted.
    """
    def process(self, element, window_size, time_group_key, timestamp_key='timestamp'):
        """
        :param element: Element being processed
        :param window_size: The size of the window in seconds to be used.
            This needs to be the same one used in the function TimestampToTimeGroup.
        :param time_group_key: The name of the key with the time group
        :param timestamp_key: The key to assign the timestamp in
        :return: Elements with the timestamp key with timestamp values
        """
        yield {
            **element,
            timestamp_key: window_size * element[time_group_key]
        }


class ToReadable(beam.DoFn):
    """ParDo to create readable dates from timestamps"""
    def process(self, element, timestamp_key, datetime_key, datetime_format='%Y-%m-%d %H:%M:%S%z'):
        """
        :param element: Element being processed
        :param timestamp_key: The key with the timestamp value
        :param datetime_key: The key to assign the new readable format
        :param datetime_format: The format to be used when running strftime.
        :return: Elements with new readable datetime value
        """
        yield {
            **element,
            datetime_key: datetime.utcfromtimestamp(element[timestamp_key]).strftime(datetime_format)
        }
