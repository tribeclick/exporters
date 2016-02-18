from collections import Counter
from exporters.default_retries import retry_long
from exporters.writers.base_writer import BaseWriter


class AzureBlobWriter(BaseWriter):
    """
    Writes items to S3 bucket. It is a File Based writer, so it has filebase
    option available

        - account_name (str)
            Public acces name of the azure account.

        - account_key (str)
            Public acces key to the azure account.

        - container (str)
            Blob container name.
    """
    supported_options = {
        'account_name': {'type': basestring, 'env_fallback': 'EXPORTERS_AZUREWRITER_NAME'},
        'account_key': {'type': basestring, 'env_fallback': 'EXPORTERS_AZUREWRITER_KEY'},
        'container': {'type': basestring}
    }

    def __init__(self, options):
        from azure.storage.blob import BlobService
        super(AzureBlobWriter, self).__init__(options)
        account_name = self.read_option('account_name')
        account_key = self.read_option('account_key')
        self.container = self.read_option('container')
        self.azure_service = BlobService(account_name, account_key)
        self.azure_service.create_container(self.container)
        self.logger.info('AzureBlobWriter has been initiated.'
                         'Writing to container {}'.format(self.container))
        self.writer_metadata['files_counter'] = Counter()

    def write(self, dump_path, group_key=None):
        self._write_blob(dump_path)
        self.writer_metadata['files_counter'][''] += 1

    @retry_long
    def _write_blob(self, dump_path):
        self.azure_service.put_block_blob_from_path(
            self.read_option('container'),
            dump_path.split('/')[-1],
            dump_path,
            max_connections=5,
        )
