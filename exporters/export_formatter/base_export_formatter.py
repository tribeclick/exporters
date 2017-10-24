from exporters.pipeline.base_pipeline_item import BasePipelineItem
from exporters.logger.base_logger import FormatterLogger


class BaseExportFormatter(BasePipelineItem):

    file_extension = None
    item_separator = '\n'

    def __init__(self, options, metadata=None):
        super(BaseExportFormatter, self).__init__(options, metadata)
        self.logger = FormatterLogger({
            'log_level': options.get('log_level'),
            'logger_name': options.get('logger_name')
        })

    def format(self, item):
        raise NotImplementedError

    def format_header(self):
        return ''

    def format_footer(self):
        return ''
