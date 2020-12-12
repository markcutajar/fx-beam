import apache_beam as beam


class ToFloat(beam.DoFn):
    """ParDo to convert a float string to a float object"""
    def process(self, element, fields, **kwargs):
        """
        :param element: Element being processed
        :param fields: Fields to convert into a float
        :return: Elements with a datetime filed
        """

        if isinstance(fields, str):
            fields = [fields]

        for f in fields:
            element[f] = float(element[f])

        yield element
