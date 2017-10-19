"""
Kafka reader
"""
import logging
import six

from exporters.readers.base_reader import BaseReader
from exporters.records.base_record import BaseRecord
from exporters.default_retries import retry_short
from exporters.utils import str_list, int_list


class KafkaScannerReader(BaseReader):
    """
    This reader retrieves items from kafka brokers.

        - batch_size (int)
            Number of items to be returned in each batch

        - brokers (list)
            List of brokers uris.

        - topic (str)
            Topic to read from.

        - partitions (list)
            Partitions to read from.

        - ssl_configs (dict)

        - decompress (boolean)
            Whether to unzip messages or not

        - msgformat (string)
            Format of the messages (supports msgpack or json)

        - min_lower_offsets: (dict)
            Dict with minimum offsetss to stop at. Good for recurring exports.
    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': six.integer_types, 'default': 10000},
        'brokers': {'type': str_list},
        'topic': {'type': six.string_types},
        'partitions': {'type': int_list, 'default': None},
        'ssl_configs': {'type': dict, 'default': None},
        'decompress': {'type': bool, 'default': True},
        'msgformat': {'type': six.string_types, 'default': "msgpack"},
        'min_lower_offsets': {'type': dict, 'default': None},
    }

    def __init__(self, *args, **kwargs):
        from kafka_scanner import KafkaScanner
        super(KafkaScannerReader, self).__init__(*args, **kwargs)
        brokers = self.read_option('brokers')
        topic = self.read_option('topic')
        partitions = self.read_option('partitions')
        scanner = KafkaScanner(brokers, topic, partitions=partitions,
                               batchsize=self.read_option('batch_size'),
                               ssl_configs=self.read_option('ssl_configs'),
                               decompress=self.read_option('decompress'),
                               msgformat=self.read_option('msgformat'),
                               min_lower_offsets=self.read_option('min_lower_offsets'),
                               )

        self.batches = scanner.scan_topic_batches()

        if partitions:
            topic_str = '{} (partitions: {})'.format(topic, partitions)
        else:
            topic_str = topic
        self.logger.info('KafkaScannerReader has been initiated.'
                         'Topic: {}.'.format(topic_str))

        logger = logging.getLogger('kafka_scanner')
        logger.setLevel(logging.WARN)

    @retry_short
    def get_from_kafka(self):
        return self.batches.next()

    def get_next_batch(self):
        """
        This method is called from the manager. It must return a list or a generator
        of BaseRecord objects.
        When it has nothing else to read, it must set class variable "finished" to True.
        """
        try:
            batch = self.get_from_kafka()
            for message in batch:
                item = BaseRecord(message)
                self.increase_read()
                yield item
        except StopIteration as e:
            self.finished = True
        self.logger.debug('Done reading batch')

    def set_last_position(self, last_position):
        """
        Called from the manager, it is in charge of updating the last position of data commited
        by the writer, in order to have resume support
        """
        if last_position is None:
            self.last_position = {}
        else:
            self.last_position = last_position

class KafkaScannerDirectReader(KafkaScannerReader):
    """
    This reader retrieves items from kafka brokers.

        - batch_size (int)
            Number of items to be returned in each batch

        - brokers (list)
            List of brokers uris.

        - topic (str)
            Topic to read from.

        - partitions (list)
            Partitions to read from.

        - ssl_configs (dict)

        - decompress (boolean)
            Whether to unzip messages or not

        - msgformat (string)
            Format of the messages (supports msgpack or json)

        - keep_offsets: (dict)
            If True, use committed offsets as starting ones. Else start from earlies offsets.
    """

    # List of options to set up the reader
    supported_options = {
        'batch_size': {'type': six.integer_types, 'default': 1000},
        'brokers': {'type': str_list},
        'topic': {'type': six.string_types},
        'partitions': {'type': int_list, 'default': None},
        'consumer_group': {'type': six.string_types},
        'ssl_configs': {'type': dict, 'default': None},
        'decompress': {'type': bool, 'default': True},
        'msgformat': {'type': six.string_types, 'default': "msgpack"},
        'keep_offsets': {'type': bool, 'default': True},
    }

    def __init__(self, *args, **kwargs):
        from kafka_scanner import KafkaScannerDirect
        super(KafkaScannerDirectReader, self).__init__(*args, **kwargs)
        brokers = self.read_option('brokers')
        topic = self.read_option('topic')
        partitions = self.read_option('partitions')
        scanner = KafkaScannerDirect(brokers, topic,
                               group=self.read_option('consumer_group'),
                               partitions=partitions,
                               batchsize=self.read_option('batch_size'),
                               ssl_configs=self.read_option('ssl_configs'),
                               decompress=self.read_option('decompress'),
                               msgformat=self.read_option('msgformat'),
                               keep_offsets=self.read_option('keep_offsets'),
                               )

        self.batches = scanner.scan_topic_batches()

        if partitions:
            topic_str = '{} (partitions: {})'.format(topic, partitions)
        else:
            topic_str = topic
        self.logger.info('KafkaScannerDirectReader has been initiated.'
                         'Topic: {}.'.format(topic_str))

        logger = logging.getLogger('kafka_scanner')
        logger.setLevel(logging.WARN)

