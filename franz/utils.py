import logging
import time
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.structs import OffsetAndMetadata


class OffsetManager(object):

    def __init__(self, consumer, commit_interval):
        self.consumer = consumer
        self.commit_interval = commit_interval
        self.has_group = False
        self.to_commit = {}
        self.last_commit = time.monotonic()

    def report_consumed(self, tp, offset):
        self.to_commit[tp] = OffsetAndMetadata(offset + 1, '')

    def commit(self):
        this_commit = time.monotonic()
        if self.consumer.config['group_id'] and self.to_commit:
            logging.info('Starting offset commit')
            self.consumer.commit(offsets=self.to_commit)
            logging.info('Finished offset commit')
        self.last_commit = this_commit
        self.to_commit = {}

    def should_commit(self):
        return (time.monotonic() - self.last_commit) > self.commit_interval

    def maybe_commit(self):
        if self.should_commit():
            self.commit()

    def reset(self):
        logging.info('Resetting offset manager')
        self.last_commit = time.monotonic()
        self.to_commit = {}


class OffsetManagerRebalanceListener(ConsumerRebalanceListener):

    def __init__(self, offset_manager):
        self.offset_manager = offset_manager

    def on_partitions_revoked(self, revoked):
        logging.info('Partitions revoked - committing outstanding offsets')
        self.offset_manager.commit()

    def on_partitions_assigned(self, assigned):
        logging.info('Partitions assigned - resetting outstanding offsets')
        self.offset_manager.reset()
