import logging


LEVEL_DICT = {
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


class BaseLogger(object):
    def __init__(self, options):
        level = LEVEL_DICT.get(options.get('log_level'), 'INFO')
        self.logger = logging.getLogger(options.get('logger_name', 'export-pipeline'))
        self.logger.setLevel(level)


class CategoryLogger(BaseLogger):
    name = "category_logger"

    def debug(self, msg):
        self._log(msg=msg, level=logging.DEBUG)

    def info(self, msg):
        self._log(msg=msg, level=logging.INFO)

    def warning(self, msg):
        self._log(msg=msg, level=logging.WARNING)

    def error(self, msg):
        self._log(msg=msg, level=logging.ERROR)

    def critical(self, msg):
        self._log(msg=msg, level=logging.CRITICAL)

    def _log(self, msg, level):
        log_msg = ' -- {name} -- {msg}'.format(name=self.name, msg=msg)
        self.logger.log(msg=log_msg, level=level)


class ReaderLogger(CategoryLogger):
    name = 'READER'


class TransformLogger(CategoryLogger):
    name = 'TRANSFORM'


class FilterLogger(CategoryLogger):
    name = 'FILTER'


class WriterLogger(CategoryLogger):
    name = 'WRITER'


class ExportManagerLogger(CategoryLogger):
    name = 'EXPORTMANAGER'


class PersistenceLogger(CategoryLogger):
    name = 'PERSISTENCE'


class StatsManagerLogger(CategoryLogger):
    name = 'STATSMANAGER'


class FormatterLogger(CategoryLogger):
    name = 'FORMATTER'
