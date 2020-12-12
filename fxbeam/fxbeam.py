import apache_beam as beam

from apache_beam.io import WriteToText, ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from fxbeam.resamplers.tick_resamplers import TickByTimeGroupResampler
from fxbeam.schemas import TickDataWithTimestampSchema
from fxbeam.utils.datatype_utils import ToFloat
from fxbeam.utils.datetime_utils import ToTimestamp, AddTimestamp, ToReadable
from fxbeam.utils.element_utils import SelectKeys
from fxbeam.utils.input_parsing import ParseDataRows


class FxBeam:

    """
    FXBeam is a class that handles tick and OHLCV data.

    The current functionality of the class is to lead a file / stream of
    tick data, resamples it according to window size and produce OHLCV data.

    This is done in Apache Beam instead of easier libraries to be able to handle,
    multiple files, that together wouldn't fit into memory.

    Currently, the class only handles a stream of a single instrument. However,
    changes are in progress of this to accept multiple instruments, as well as JSON
    format so as to be used in a stream processing pipeline.

    """

    OUTPUT_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    TICK_DATA_HEADER = ['timestamp', 'ask', 'bid', 'volume']
    TICK_DATA_COLUMNS = ['ask', 'bid', 'volume']
    TICK_DATA_TIMESTAMP_FORMAT = '%Y%m%d %H%M%S%f'

    def __init__(
            self, input_file, output_file, window_size,
            pipeline_params, compression=None, save_main_session=False
    ):

        # We use the save_main_session option because one or more DoFn's in this
        # workflow rely on global context (e.g., a module imported at module level).
        pipeline_options = PipelineOptions(pipeline_params)
        pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
        self.pipeline = beam.Pipeline(options=pipeline_options)

        # Set window size
        self.window_size = window_size

        # Set IO
        self.input_file = input_file
        self.output_file = output_file
        self.compression = compression

        # Set Pipeline stages
        self.resampler = TickByTimeGroupResampler(self.window_size)

    def build(self):
        """Function to build the pipeline stages"""
        data = self.read()
        data = self.resampler.process(data)
        data = self.extract_output(data)
        self.write(data)

    def run(self):
        """Function to run pipeline"""
        self.pipeline.run()

    def extract_output(self, data):
        """Function to run before saving the file to disk.
        Currently it only converts the timestamp to a readable
        format and selects keys defined in OUTPUT_COLUMNS.
        """
        data = data | 'Add readable time' >> beam.ParDo(
            ToReadable(),
            timestamp_key='timestamp',
            datetime_key='timestamp'
        )
        data = data | 'Select keys' >> beam.ParDo(
            SelectKeys(),
            keys=self.OUTPUT_COLUMNS
        )
        return data

    def write(self, data):
        """Pipeline stage to write to file"""
        data | 'Save output to file' >> WriteToText(self.output_file)

    def read(self):
        """
        Pipeline stage to read the files specified. This pipeline assumes the
        files DO NOT CONTAIN A header and are a simpler stream.

        The files are assumed to be a CSV string. However, only a few minor changes
        below have to be made so a JSON could be used.

        1. Input data is read and split depending on TICK_DATA_HEADER class values.
        2. The timestamp field is converted to an actual timestamp defending on format.
        3. The elements are assigned a window timestamp value. This is useful when using
            windows.
        4. The values are all converted into floats. (This is a bottleneck. If we start
            processing JSONs this could be avoided).

        :return: PCollection with Tick data
        """

        _compression = self.compression if self.compression else 'auto'

        rows = self.pipeline | 'Read data file' >> ReadFromText(
            self.input_file,
            compression_type=_compression
        )

        rows = rows | 'Convert to tick objects' >> beam.ParDo(
            ParseDataRows(),
            headers=self.TICK_DATA_HEADER
        )

        rows = rows | 'Convert to timestamp field' >> beam.ParDo(
            ToTimestamp(),
            string_format=self.TICK_DATA_TIMESTAMP_FORMAT,
            datetime_key=self.TICK_DATA_HEADER[0],
            timestamp_key='timestamp'
        )

        rows = rows | 'Convert to datetime object' >> beam.ParDo(
            AddTimestamp(),
            timestamp_key='timestamp'
        )

        rows = rows | 'Convert to values to floats' >> beam.ParDo(
            ToFloat(),
            fields=self.TICK_DATA_COLUMNS
        ).with_output_types(TickDataWithTimestampSchema)

        return rows
