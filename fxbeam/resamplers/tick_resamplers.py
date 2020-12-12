import apache_beam as beam
from apache_beam.transforms import window

from fxbeam.combiners.tick_to_ohlcv import TickToOHLCVCombiner
from fxbeam.utils.datetime_utils import TimestampToTimeGroup, TimeGroupToTimestamp, AddWindowStartTimestampToElement


class TickByTimeGroupResampler:
    """Resampler to combine ticks into OHLCV values.
    This creates a key based on the timestamp.

    The process function is the entry point to be used.
    """
    def __init__(self, window_size):
        self.window_size = window_size

    def process(self, data):
        """Entry point to the resampler. This function
        adds the group keys to the data, resamples using the
        TickToOHLCVCombiner and reinserts the timestamp value
        before returning the OHLCV data.
        :param data: PCollection being processed
        :return: PCollection of OHLCV data including timestamp key
        """
        data = self.timestamp_to_timegroup(data)
        data = self.resample(data)
        data = self.timegroup_to_timestamp(data)
        return data

    def timestamp_to_timegroup(self, data):
        """Function to create a timegroup key from timestamp
        :param data: PCollection being processed
        :return: PCollection with timegroup keys
        """
        return data | 'Resampler - Create group from timestamp' >> beam.ParDo(
            TimestampToTimeGroup(),
            window_size=self.window_size,
            time_group_key='time_group_key'
        )

    def timegroup_to_timestamp(self, data):
        """Function to create a timestamp from timegroup keys
        :param data: PCollection being processed
        :return: PCollection with timestamps
        """
        return data | 'Resampler - Add timestamp to elements' >> beam.ParDo(
            TimeGroupToTimestamp(),
            window_size=self.window_size,
            time_group_key='time_group_key'
        )

    @classmethod
    def resample(cls, data):
        """Resample function which runs the TickToOHLCVCombiner.
        The map before and de-map after are done since the Combine
        per key expects a map of key: value elements.
        :param data: PCollection being processed
        :return: PCollection with OHLCV data but without timestamp
        """
        return (
            data |
            'Resampler - Map data' >> beam.Map(lambda x: (x['time_group_key'], x)) |
            'Resampler - Perform OHLCV' >> beam.CombinePerKey(TickToOHLCVCombiner()) |
            'Resampler - De-Map data' >> beam.Map(lambda x: {'time_group_key': x[0], **x[1]})
        )


class TickByWindowResampler:
    """Resampler to combine ticks into OHLCV values.
    This creates windows to be used in the aggregation process.

    The process function is the entry point to be used.
    """
    def __init__(self, window_size):
        self.window_size = window_size

    def process(self, data):
        """Entry point to the resampler. This function
        applies the window to the data, resamples using the
        TickToOHLCVCombiner and reinserts the timestamp value
        of the start of the window before returning the OHLCV data.
        :param data: PCollection being processed
        :return: PCollection of OHLCV data including timestamp key
        """
        data = self.apply_window(data)
        data = self.resample(data)
        data = self.extract_window_time(data)
        return data

    def apply_window(self, data):
        """Function used to apply the window to the data.
        Currently this is FIXED since OHLCV data is calculated using a fixed window.
        :param data: PCollection being processed
        :return: PCollection with applied window depending on window size.
        """
        return data | 'Resampler - Divide data to windows' >> beam.WindowInto(
            window.FixedWindows(self.window_size)
        )

    @classmethod
    def extract_window_time(cls, data):
        """Function to assign the start window time ot the element
        :param data: PCollection being processed
        :return: PCollection with timestamps
        """
        return data | 'Resampler - Add timestamp to elements' >> beam.ParDo(
            AddWindowStartTimestampToElement()
        )

    @classmethod
    def resample(cls, data):
        """Resample function which runs the TickToOHLCVCombiner.

        The CombineGlobally function is used since we are not using
        keys. However, this still takes into account the non-global
        windowing method assigned in the previous step.

        :param data: PCollection being processed
        :return: PCollection with OHLCV data but without timestamp
        """
        return data | 'Resample - Perform OHLCV' >> beam.CombineGlobally(TickToOHLCVCombiner())
