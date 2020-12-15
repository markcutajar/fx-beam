import apache_beam as beam
from apache_beam.transforms import window

from fxbeam.combiners.tick_to_ohlcv import TickToOHLCVCombiner
from fxbeam.utils.datetime_utils import TimestampToTimeGroup, TimeGroupToTimestamp, AddWindowStartTimestampToElement


class TickByTimeGroupResampler:
    """Resampler to combine ticks into OHLCV values.
    This creates a key based on the timestamp.

    The process function is the entry point to be used.
    """
    def __init__(self, window_size, instrument_column=None):
        self.window_size = window_size
        self.instrument_column = instrument_column

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

    def map_elements(self, data):
        """Map function to create key:value pairs to run CombinePerKey function.
        :param data: PCollection being processed with time_group_key column and
            instrument_column if set to use.
        :return: PCollection with mapped data
        """
        action_name = 'Resampler - Map data'
        if self.instrument_column:
            return data | action_name >> beam.Map(
                lambda x: ((x['time_group_key'], x[self.instrument_column]), x)
            )
        return data | action_name >> beam.Map(lambda x: (x['time_group_key'], x))

    def demap_elements(self, data):
        """De-Map function to unmap key:value pairs after CombinePerKey function.
        :param data: PCollection being processed with (key: value) or
            (key1, instrumentKey: value) format.
        :return: PCollection with de-mapped data with keys added back into elements
        """
        action_name = 'Resampler - De-Map data'
        if self.instrument_column:
            return data | action_name >> beam.Map(
                lambda x: {'time_group_key': x[0][0], self.instrument_column: x[0][1], **x[1]}
            )
        return data | 'Resampler - De-Map data' >> beam.Map(lambda x: {'time_group_key': x[0], **x[1]})

    def resample(self, data):
        """Resample function which runs the TickToOHLCVCombiner.
        The map before and de-map after are done since the Combine
        per key expects a map of key: value elements.
        :param data: PCollection being processed
        :return: PCollection with OHLCV data but without timestamp
        """

        data = self.map_elements(data)
        data = data | 'Resampler - Perform OHLCV' >> beam.CombinePerKey(TickToOHLCVCombiner())
        return self.demap_elements(data)


class TickByWindowResampler:
    """Resampler to combine ticks into OHLCV values.
    This creates windows to be used in the aggregation process.

    The process function is the entry point to be used.
    """
    def __init__(self, window_size, instrument_column=None):
        self.window_size = window_size
        self.instrument_column = instrument_column

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

    def map_elements(self, data):
        """Map function to create key:value pairs to run CombinePerKey function.
        :param data: PCollection being processed with instrument_column if set to use.
        :return: PCollection with mapped data
        """
        return data | 'Resampler - Map data' >> beam.Map(lambda x: (x[self.instrument_column], x))

    def demap_elements(self, data):
        """De-Map function to unmap key:value pairs after CombinePerKey function.
        :param data: PCollection being processed with (instrumentKey: value)
        :return: PCollection with de-mapped data with keys added back into elements
        """
        return data | 'Resampler - De-Map data' >> beam.Map(lambda x: {self.instrument_column: x[0], **x[1]})

    def resample(self, data):
        """Resample function which runs the TickToOHLCVCombiner.

        The CombineGlobally function is used since we are not using
        keys. However, this still takes into account the non-global
        windowing method assigned in the previous step.

        :param data: PCollection being processed
        :return: PCollection with OHLCV data but without timestamp
        """
        action_name = 'Resampler - Perform OHLCV'
        if self.instrument_column:
            data = self.map_elements(data)
            data = data | action_name >> beam.CombinePerKey(TickToOHLCVCombiner())
            return self.demap_elements(data)

        return data | action_name >> beam.CombineGlobally(TickToOHLCVCombiner())
