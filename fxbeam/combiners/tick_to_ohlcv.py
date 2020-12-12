import apache_beam as beam


class TickToOHLCVCombiner(beam.CombineFn):
    """Combiner that takes in tick data and converts it into OHLCV value.
    This assumes grouping is already done elsewhere and whatever comes in
    is part of the window.
    """
    def create_accumulator(self):
        """The initialization function for the accumulator.
        :return: The initial state
        """
        return {
            'open': None,
            'high': None,
            'low': None,
            'close': None,
            'volume': 0
        }

    def add_input(self, ohlcv, input_):
        """Function used when adding a new input.
        This function needs to check the new input against the
        open, high, low, close elements assigned already. The
        volume is incremented as a tick data is assumed as a
        transaction on the exchange.
        :param ohlcv: The accumulator state.
        :param input_: The new input received by the accumulator
        :return: The new accumulator state.
        """
        ohlcv = self.check_open(input_, ohlcv, 'timestamp')
        ohlcv = self.check_close(input_, ohlcv, 'timestamp')
        ohlcv = self.check_high(input_, ohlcv)
        ohlcv = self.check_low(input_, ohlcv)
        ohlcv = self.increment_volume(ohlcv)
        return ohlcv

    def merge_accumulators(self, accumulators):
        """Function to be used when merging accumulators coming from
        different nodes. This assumes all the other accumulators done
        their job when selecting the OHLCV values.
        :param accumulators: Accumulator states same as the current
            accumulator state.
        :return: A combined state of all the accumulators
        """
        return {
            'open': self.combine_open(accumulators, 'timestamp'),
            'close': self.combine_close(accumulators, 'timestamp'),
            'low':  self.combine_low(accumulators),
            'high': self.combine_high(accumulators),
            'volume': self.combine_volume(accumulators)
        }

    def extract_output(self, ohlcv):
        """Function that runs once all the accumulators ran and combined.
        This extracts the values from the state elements.
        :param ohlcv: The accumulator state
        :return: Returns OHLCV as values rather than as elements.
        """
        return {
            'open': self.get_value(ohlcv['open']),
            'high': self.get_value(ohlcv['high']),
            'low': self.get_value(ohlcv['low']),
            'close': self.get_value(ohlcv['close']),
            'volume': ohlcv['volume']
        }

    @classmethod
    def check_open(cls, item, ohlcv, time_key):
        """Function to check if the new item timestamp is less than current open.
        If it does it means it can be assigned as the new 'open element'.
        :param item: Item being processed
        :param ohlcv: Current accumulator state
        :param time_key: Key with the timestamp value
        :return: Accumulator state, updated or otherwise
        """
        if ohlcv['open'] is None or item[time_key] < ohlcv['open'][time_key]:
            ohlcv['open'] = item
        return ohlcv

    @classmethod
    def check_close(cls, item, ohlcv, time_key):
        """Function to check if the new item timestamp is higher than current close.
        If it does it means it can be assigned as the new 'close element'.
        :param item: Item being processed
        :param ohlcv: Current accumulator state
        :param time_key: Key with the timestamp value
        :return: Accumulator state, updated or otherwise
        """
        if ohlcv['close'] is None or item[time_key] > ohlcv['close'][time_key]:
            ohlcv['close'] = item
        return ohlcv

    @classmethod
    def check_high(cls, item, ohlcv):
        """Function to check if the new item value is higher than current high.
        If it does it means it can be assigned as the new 'high element'.
        :param item: Item being processed
        :param ohlcv: Current accumulator state
        :return: Accumulator state, updated or otherwise
        """
        if ohlcv['high'] is None or cls.get_value(item) > cls.get_value(ohlcv['high']):
            ohlcv['high'] = item
        return ohlcv

    @classmethod
    def check_low(cls, item, ohlcv):
        """Function to check if the new item value is lower than current low.
        If it does it means it can be assigned as the new 'low element'.
        :param item: Item being processed
        :param ohlcv: Current accumulator state
        :return: Accumulator state, updated or otherwise
        """
        if ohlcv['low'] is None or cls.get_value(item) < cls.get_value(ohlcv['low']):
            ohlcv['low'] = item
        return ohlcv

    @staticmethod
    def increment_volume(ohlcv):
        """Function to increment the volume by a single tick
        :param ohlcv: Current accumulator state
        :return: Updated accumulator state
        """
        ohlcv['volume'] += 1
        return ohlcv

    @staticmethod
    def get_value(item):
        """Function to calculate the value of the element.
        This is done so we can easily change how the value is calculated,
        by changing it in this one place.
        :param item: Tick item
        :return: Value of tick item
        """
        return item['ask']

    @classmethod
    def combine_open(cls, items, time_key):
        """Function to combine opens. It checks all the accumulators'
        opens and keeps the one with least timestamp.
        :param items: Accumulator states
        :param time_key: Key with the timestamp value
        :return: The open with the least timestamp key
        """
        _open = items[0]['open']
        for item in items:
            if item['open'][time_key] < _open[time_key]:
                _open = item['open']
        return _open

    @classmethod
    def combine_close(cls, items, time_key):
        """Function to combine closes. It checks all the accumulators'
        closes and keeps the one with highest timestamp.
        :param items: Accumulator states
        :param time_key: Key with the timestamp value
        :return: The close with the least timestamp key
        """
        _close = items[0]['close']
        for item in items:
            if item['close'][time_key] > _close[time_key]:
                _close = item['close']
        return _close

    @classmethod
    def combine_high(cls, items):
        """Function to combine highs. It checks all the accumulators'
        highs and keeps highest in value.
        :param items: Accumulator states
        :return: The high with the highest value
        """
        _high = items[0]['high']
        for item in items:
            if cls.get_value(item['high']) > cls.get_value(_high):
                _high = item['high']
        return _high

    @classmethod
    def combine_low(cls, items):
        """Function to combine lows. It checks all the accumulators'
        lows and keeps lowest in value.
        :param items: Accumulator states
        :return: The low with the lowest value
        """
        _low = items[0]['low']
        for item in items:
            if cls.get_value(item['low']) < cls.get_value(_low):
                _low = item['low']
        return _low

    @staticmethod
    def combine_volume(items):
        """Function to combine the volume elements of
        different accumulators. This is done by simply summing
        since volume is a simple count of the number of ticks
        in the window.
        :param items: Accumulator states
        :return: A combined value for volume
        """
        return sum([item['volume'] for item in items])
