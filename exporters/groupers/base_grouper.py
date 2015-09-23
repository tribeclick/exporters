from exporters.pipeline.base_pipeline_item import BasePipelineItem


class BaseGrouper(BasePipelineItem):
    """
    This module adds support to grouping items. It must implement the following methods:
    """

    def __init__(self, configuration):
        super(BaseGrouper, self).__init__(configuration)

    def group_batch(self, batch):
        """
        Returns the grouped batch.
        """
        raise NotImplementedError