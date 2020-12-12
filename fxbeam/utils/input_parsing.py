import apache_beam as beam


class ParseDataRows(beam.DoFn):
    """ParDo to process data rows according to headers list"""

    def process(self, element, headers, **kwargs):
        """
        :param element: Element to be processed. A row of comma separated text
        :param headers: List of headers for the file
        :return Elements in the form of a map according to the headers
        """
        tokens = element.split(',')

        if len(tokens) != len(headers):
            raise ValueError('Header not matching data length')

        yield {
            header: token
            for header, token in zip(headers, tokens)
        }
