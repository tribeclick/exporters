import json
import six
from fastavro.writer import Writer, write_data, null_write_block, write_long
import io

from exporters.export_formatter.base_export_formatter import BaseExportFormatter


class AvroExportFormatter(BaseExportFormatter):
    """
    This export formatter provides a way of exporting items in AVRO format.
    follows the avro schema 1.8 https://avro.apache.org/docs/1.8.1/spec.html
    We write the items on a 1 per block way.
    This allows for streaming and keeps the files consistency even if the process
    ends unexpectedly

        - schema_path(string)
            path to the json schema for the data to use in avro export.
    """

    supported_options = {
        'schema_path': {'type': six.string_types},
    }
    file_extension = 'avro'

    def __init__(self, *args, **kwargs):
        super(AvroExportFormatter, self).__init__(*args, **kwargs)
        with open(self.read_option('schema_path')) as fname:
            self.schema = json.load(fname)
        self.file_extension = 'avro'
        header_buffer = io.BytesIO()
        self.writer = Writer(fo=header_buffer, schema=self.schema, sync_interval=0)
        self.header_value = header_buffer.getvalue()
        self._clear_buffer()
        self.item_separator = ''

    def format_header(self):
        return self.header_value

    def _clear_buffer(self):
        self.writer.fo.seek(0)
        self.writer.fo.truncate()

    def format(self, item):
        try:
            self.writer.write(item)
            item_value = self.writer.fo.getvalue()
            return item_value
        except Exception as e:
            self.logger.warn(item)
            self.logger.error(e)
        finally:
            self._clear_buffer()

