import apache_beam as beam


class SelectKeys(beam.DoFn):
    """ParDo to select only certain elements to return in the output"""
    def process(self, element, keys, **kwargs):
        """
        :param element: Element to be processed
        :param keys: List of keys to be left in the PCollection
        :return Elements only with selected keys
        """
        yield {
            key: element.get(key) for key in keys
        }
