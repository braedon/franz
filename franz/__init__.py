import click
import json
import logging
import re
import sys
import time
import rfc3339

from kafka import KafkaConsumer, KafkaProducer
from kafka.partitioner.default import DefaultPartitioner
from kafka.protocol.commit import GroupCoordinatorRequest, OffsetFetchRequest
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from kafka.structs import TopicPartition

from .utils import OffsetManager, OffsetManagerRebalanceListener


def key_serializer(key):
    if isinstance(key, str):
        return key.encode('utf-8')


def value_serializer(value):
    if isinstance(value, str):
        return value.encode('utf-8')


def key_deserializer(key):
    if key is not None:
        return key.decode('utf-8')


def value_deserializer(value):
    if value is not None:
        return value.decode('utf-8')


def assign_consumer(topic_dict, consumer):
    consumer.assign(topic_dict.keys())


def seek_consumer(topic_dict, consumer):
    for tp, (start, end) in topic_dict.items():
        if start == 0:
            consumer.seek_to_beginning(tp)
        else:
            consumer.seek(tp, start)


def slice_consumer(topic_dict, consumer):
    assign_consumer(topic_dict, consumer)
    seek_consumer(topic_dict, consumer)

    topic_dict = topic_dict.copy()
    for m in consumer:
        wanted = True

        tp = TopicPartition(m.topic, m.partition)

        if tp not in topic_dict:
            wanted = False

        start, end = topic_dict[tp]

        if m.offset < start:
            wanted = False

        if end is not None:
            if m.offset >= (end - 1):
                del topic_dict[tp]
                assign_consumer(topic_dict, consumer)

            if m.offset >= end:
                wanted = False

        if wanted:
            yield m

        if not topic_dict:
            return


CONTEXT_SETTINGS = {
    'help_option_names': ['-h', '--help']
}


@click.group(context_settings=CONTEXT_SETTINGS)
def main():
    pass


@click.command()
@click.argument('when')
@click.argument('topic', nargs=-1)
@click.option('-b', '--bootstrap-brokers', default='localhost',
              help='Addresses of brokers in a Kafka cluster to talk to.' +
                   ' Brokers should be separated by commas' +
                   ' e.g. broker1,broker2.' +
                   ' Ports can be provided if non-standard (9092)' +
                   ' e.g. broker1:9999. (default: localhost)')
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
@click.option('-vv', '--very-verbose', is_flag=True,
              help='Turn on very verbose logging.')
def watermark(when,
              topic,
              bootstrap_brokers,
              verbose,
              very_verbose):
    '''Find the watermark of a topic, at a particular time.
       By default, connect to a kafka cluster at localhost:9092.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if very_verbose else
               logging.INFO if verbose else
               logging.WARNING
    )
    logging.captureWarnings(True)

    if when.lower() == 'earliest':
        when = OffsetResetStrategy.EARLIEST
    elif when.lower() == 'latest':
        when = OffsetResetStrategy.LATEST
    else:
        when = rfc3339.parse_datetime(when)
        when = int(when.timestamp() * 1000)

    bootstrap_brokers = bootstrap_brokers.split(',')

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_brokers,
        value_deserializer=value_deserializer,
        key_deserializer=key_deserializer,
    )

    client = consumer._client

    node = client.least_loaded_node()

    request = MetadataRequest[1](topic)
    f = client.send(node, request)
    client.poll(future=f)
    if f.failed():
        raise f.exception
    metadata_topics = f.value.topics

    nodes_map = {}
    for t in metadata_topics:
        topic = t[1]
        partitions = t[3]
        for p in partitions:
            partition = p[1]
            leader = p[2]
            if leader not in nodes_map:
                nodes_map[leader] = {}
            if topic not in nodes_map[leader]:
                nodes_map[leader][topic] = []
            nodes_map[leader][topic].append(partition)

    current_offsets = {}
    for node, topic_map in nodes_map.items():

        request = OffsetRequest[1](
            -1,
            [(topic,
              [(partition, when)
               for partition in partitions])
             for topic, partitions in topic_map.items()]
        )
        # Seem to need to poll to allow connections to
        # nodes to complete.
        while not client.ready(node):
            client.poll()
            time.sleep(0.1)
        f = client.send(node, request)
        client.poll(future=f)
        if f.failed():
            raise f.exception

        for t in f.value.topics:
            topic = t[0]
            if topic not in current_offsets:
                current_offsets[topic] = {}
            partitions = t[1]
            for p in partitions:
                partition = p[0]
                offset = p[3]
                current_offsets[topic][partition] = offset

    for topic, partition_offsets in current_offsets.items():
        print('Position: {}[{}]'.format(topic, ','.join('{}={}'.format(p, o) for p, o in sorted(partition_offsets.items()))))

    consumer.close()


@click.command()
@click.argument('topic', nargs=-1)
@click.option('-b', '--bootstrap-brokers', default='localhost',
              help='Addresses of brokers in a Kafka cluster to talk to.' +
                   ' Brokers should be separated by commas' +
                   ' e.g. broker1,broker2.' +
                   ' Ports can be provided if non-standard (9092)' +
                   ' e.g. broker1:9999. (default: localhost)')
@click.option('-k', '--filter-key', multiple=True,
              help='Filter results to a specific key.' +
                   ' Provide option multiple times to filter by multiple keys.' +
                   ' Assumes keys are partitioned with the default partitioner.')
@click.option('-t', '--fetch-timeout', type=float, default=float('inf'),
              help='How many seconds to wait for a message when fetching before ' +
                   'exiting. (default=indefinitely)')
@click.option('-j', '--json-value', is_flag=True,
              help='Parse message values as JSON.')
@click.option('-r', '--readable', is_flag=True,
              help='Display messages in a human readable format:' +
                   ' [topic:partition:offset:key] value')
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
@click.option('-vv', '--very-verbose', is_flag=True,
              help='Turn on very verbose logging.')
def fetch(topic,
          bootstrap_brokers,
          filter_key,
          fetch_timeout,
          json_value,
          readable,
          verbose,
          very_verbose):
    '''Fetch a message, or messages, from a Kafka topic partition.
       By default, connect to a kafka cluster at localhost:9092 and fetch
       the message at the specified offset, outputting in JSON format.'''

    filter_keys = filter_key

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if very_verbose else
               logging.INFO if verbose else
               logging.WARNING
    )
    logging.captureWarnings(True)

    bootstrap_brokers = bootstrap_brokers.split(',')

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_brokers,
        value_deserializer=value_deserializer,
        key_deserializer=key_deserializer,
        consumer_timeout_ms=fetch_timeout * 1000,
    )

    client = consumer._client

    node = client.least_loaded_node()

    topic_dict = {}

    for t in topic:
        # TODO: Parse topic spec properly and provide better error messages.
        match = re.search(r'^([^\[\]]+)(\[([a-zA-Z-_.\d,:=+]+)\])?$', t)
        if not match:
            logging.error('Topic argument "{}" is invalid.'.format(t))
            exit(1)

        topic, _, slice_spec = match.groups()

        # TODO: Fetch current earliest/latest offsets and use to support relative slices

        request = MetadataRequest[0](topics=[topic])
        f = client.send(node, request)
        client.poll(future=f)
        if f.failed():
            raise f.exception
        TOPIC_PARTITIONS = 2
        PARTITION_NUMBER = 1
        all_partitions = sorted(p[PARTITION_NUMBER] for p in f.value.topics[0][TOPIC_PARTITIONS])

        if filter_keys:
            partitioner = DefaultPartitioner()
            filter_key_partitions = {partitioner(key_serializer(k), all_partitions, all_partitions)
                                     for k in filter_keys}
            all_partitions = [p for p in all_partitions if p in filter_key_partitions]

        slices = slice_spec.split(',')
        for s in slices:
            if '=' in s:
                partition, s = s.split('=', 1)

                if ':' in partition:
                    start, end = partition.split(':', 1)

                    if start == '':
                        start = 0
                    else:
                        start = int(start)

                    end = int(end)

                    partitions = [p for p in range(start, end) if p in all_partitions]
                else:
                    partition = int(partition)
                    partitions = [int(partition)] if partition in all_partitions else []
            else:
                partitions = all_partitions

            # TODO: Deal with consumer group start/ends properly...
            consumer_group_offsets = {}

            for partition in partitions:
                if ':' in s:
                    start, end = s.split(':', 1)

                    if start == '':
                        start = 0
                    elif start.isdecimal():
                        start = int(start)
                    else:

                        if start not in consumer_group_offsets:
                            request = GroupCoordinatorRequest[0](consumer_group=start)
                            f = client.send(node, request)
                            client.poll(future=f)
                            if f.failed():
                                raise f.exception
                            coordinator_node = f.value.coordinator_id

                            request = OffsetFetchRequest[1](consumer_group=start, topics=[(topic, partitions)])
                            f = client.send(coordinator_node, request)
                            client.poll(future=f)
                            if f.failed():
                                raise f.exception
                            consumer_group_offsets[start] = f.value

                        TOPIC_PARTITIONS = 1
                        PARTITION_NUMBER = 0
                        PARTITION_OFFSET = 1
                        for p in consumer_group_offsets[start].topics[0][TOPIC_PARTITIONS]:
                            if p[PARTITION_NUMBER] == partition:
                                start = p[PARTITION_OFFSET]
                                break

                    if end == '':
                        end = None
                    elif end[0] == '+':
                        end = start + int(end[1:])
                    elif end.isdecimal():
                        end = int(end)
                    else:

                        if end not in consumer_group_offsets:
                            request = GroupCoordinatorRequest[0](consumer_group=end)
                            f = client.send(node, request)
                            client.poll(future=f)
                            if f.failed():
                                raise f.exception
                            coordinator_node = f.value.coordinator_id

                            request = OffsetFetchRequest[1](consumer_group=end, topics=[(topic, partitions)])
                            f = client.send(coordinator_node, request)
                            client.poll(future=f)
                            if f.failed():
                                raise f.exception
                            consumer_group_offsets[end] = f.value

                        TOPIC_PARTITIONS = 1
                        PARTITION_NUMBER = 0
                        PARTITION_OFFSET = 1
                        for p in consumer_group_offsets[end].topics[0][TOPIC_PARTITIONS]:
                            if p[PARTITION_NUMBER] == partition:
                                # TODO: Should this be offset + 1?
                                end = p[PARTITION_OFFSET]
                                break

                elif s.isdecimal():
                    start = int(s)
                    end = start + 1
                else:

                    if s not in consumer_group_offsets:
                        request = GroupCoordinatorRequest[0](consumer_group=s)
                        f = client.send(node, request)
                        client.poll(future=f)
                        if f.failed():
                            raise f.exception
                        coordinator_node = f.value.coordinator_id

                        request = OffsetFetchRequest[1](consumer_group=s, topics=[(topic, partitions)])
                        f = client.send(coordinator_node, request)
                        client.poll(future=f)
                        if f.failed():
                            raise f.exception
                        consumer_group_offsets[s] = f.value

                    TOPIC_PARTITIONS = 1
                    PARTITION_NUMBER = 0
                    PARTITION_OFFSET = 1
                    for p in consumer_group_offsets[s].topics[0][TOPIC_PARTITIONS]:
                        if p[PARTITION_NUMBER] == partition:
                            # TODO: Should this read the latest consumed message, or
                            #       the next to be consumed?
                            start = p[PARTITION_OFFSET]
                            end = start + 1
                            break

                tp = TopicPartition(topic, partition)
                topic_dict[tp] = (start, end)

    message_count = 0
    try:
        for message in slice_consumer(topic_dict, consumer):
            value = message.value
            value_string = value

            if not filter_keys or message.key in filter_keys:

                if json_value:
                    value = json.loads(value)
                    value_string = json.dumps(value,
                                              indent=True,
                                              ensure_ascii=False,
                                              sort_keys=True)

                if readable:
                    print('[{}:{}:{}:{}] {}'.format(
                          message.topic,
                          message.partition,
                          message.offset,
                          message.key,
                          value_string))
                else:
                    output = {
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset,
                        'key': message.key,
                        'value': value,
                    }
                    json.dump(output, sys.stdout, separators=(',', ':'))
                    sys.stdout.write('\n')

                sys.stdout.flush()

            message_count += 1
            if message_count % 1000 == 0:
                logging.info('Fetched %(message_count)s messages', {'message_count': message_count})

    except KeyboardInterrupt:
        pass

    logging.info('Fetched %(message_count)s messages', {'message_count': message_count})

    consumer.close()


@click.command()
@click.argument('consumer_group')
@click.argument('topic', nargs=-1)
@click.option('-b', '--bootstrap-brokers', default='localhost',
              help='Addresses of brokers in a Kafka cluster to talk to.' +
                   ' Brokers should be separated by commas' +
                   ' e.g. broker1,broker2.' +
                   ' Ports can be provided if non-standard (9092)' +
                   ' e.g. broker1:9999. (default: localhost)')
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
@click.option('-vv', '--very-verbose', is_flag=True,
              help='Turn on very verbose logging.')
def position(consumer_group,
             topic,
             bootstrap_brokers,
             verbose,
             very_verbose):
    '''Seek a consumer group to a location in one or more topic partitions.
       By default, connect to a kafka cluster at localhost:9092.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if very_verbose else
               logging.INFO if verbose else
               logging.WARNING
    )
    logging.captureWarnings(True)

    topic_partitons = []

    for t in topic:
        # TODO: Parse topic spec properly and provide better error messages.
        match = re.search(r'^([^\[\]]+)(\[(\d+(,\d+)*)\])?$', t)
        if not match:
            logging.error('Topic argument "{}" is invalid.'.format(t))
            exit(1)

        topic, _, partition_spec, _ = match.groups()

        if partition_spec:
            partitions = [int(p) for p in partition_spec.split(',')]
        else:
            # TODO: how to find this per topic...
            partitions = range(12)

        for partition in partitions:
            tp = TopicPartition(topic, partition)
            topic_partitons.append(tp)

    if topic_partitons:

        bootstrap_brokers = bootstrap_brokers.split(',')

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_brokers,
            value_deserializer=value_deserializer,
            key_deserializer=key_deserializer,
            group_id=consumer_group,
            enable_auto_commit=False,
        )

        consumer.assign(topic_partitons)

        current_offsets = {}
        for tp in topic_partitons:
            if tp.topic not in current_offsets:
                current_offsets[tp.topic] = {}
            current_offsets[tp.topic][tp.partition] = consumer.position(tp)

        for topic, partition_offsets in current_offsets.items():
            print('Position: {}[{}]'.format(topic, ','.join('{}={}'.format(p, o) for p, o in sorted(partition_offsets.items()))))

        consumer.close()


@click.command()
@click.argument('consumer_group')
@click.argument('topic', nargs=-1)
@click.option('-b', '--bootstrap-brokers', default='localhost',
              help='Addresses of brokers in a Kafka cluster to talk to.' +
                   ' Brokers should be separated by commas' +
                   ' e.g. broker1,broker2.' +
                   ' Ports can be provided if non-standard (9092)' +
                   ' e.g. broker1:9999. (default: localhost)')
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
@click.option('-vv', '--very-verbose', is_flag=True,
              help='Turn on very verbose logging.')
def seek(consumer_group,
         topic,
         bootstrap_brokers,
         verbose,
         very_verbose):
    '''Seek a consumer group to a location in one or more topic partitions.
       By default, connect to a kafka cluster at localhost:9092.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if very_verbose else
               logging.INFO if verbose else
               logging.WARNING
    )
    logging.captureWarnings(True)

    topic_dict = {}

    for t in topic:
        # TODO: how to find this per topic...
        all_partitions = range(8)

        # TODO: Parse topic spec properly and provide better error messages.

        match = re.search(r'^([^\[\]]+)(\[([\d,:=+]+)\])?$', t)
        if not match:
            logging.error('Topic argument "{}" is invalid.'.format(t))
            exit(1)

        topic, _, slice_spec = match.groups()

        # TODO: Fetch current earliest/latest offsets and use to support relative seeks

        slices = slice_spec.split(',')
        for s in slices:
            if '=' in s:
                partition, s = s.split('=', 1)
                partitions = [int(partition)]
            else:
                partitions = all_partitions

            offset = int(s)

            for partition in partitions:
                tp = TopicPartition(topic, partition)
                topic_dict[tp] = offset

    if topic_dict:

        bootstrap_brokers = bootstrap_brokers.split(',')

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_brokers,
            value_deserializer=value_deserializer,
            key_deserializer=key_deserializer,
            group_id=consumer_group,
            enable_auto_commit=False,
        )

        consumer.assign(topic_dict.keys())

        current_offsets = {}
        for tp in topic_dict:
            if tp.topic not in current_offsets:
                current_offsets[tp.topic] = {}
            current_offsets[tp.topic][tp.partition] = consumer.position(tp)

        for topic, partition_offsets in current_offsets.items():
            print('Before: {}[{}]'.format(topic, ','.join('{}={}'.format(p, o) for p, o in sorted(partition_offsets.items()))))

        for tp, offset in topic_dict.items():
            consumer.seek(tp, offset)

        current_offsets = {}
        for tp in topic_dict:
            if tp.topic not in current_offsets:
                current_offsets[tp.topic] = {}
            current_offsets[tp.topic][tp.partition] = consumer.position(tp)

        for topic, partition_offsets in current_offsets.items():
            print('After:  {}[{}]'.format(topic, ','.join('{}={}'.format(p, o) for p, o in sorted(partition_offsets.items()))))

        consumer.commit()

        consumer.close()


@click.command()
@click.argument('topic', nargs=-1)
@click.option('-b', '--bootstrap-brokers', default='localhost',
              help='Addresses of brokers in a Kafka cluster to talk to.' +
                   ' Brokers should be separated by commas' +
                   ' e.g. broker1,broker2.' +
                   ' Ports can be provided if non-standard (9092)' +
                   ' e.g. broker1:9999. (default: localhost)')
@click.option('-g', '--consumer-group', default=None,
              help='The consumer group to use. Offsets will be periodically' +
                   ' committed.' +
                   ' Consumption will start from the committed offsets,' +
                   ' if available.')
@click.option('-t', '--fetch-timeout', type=float, default=float('inf'),
              help='How many seconds to wait for a message when fetching before ' +
                   'exiting. (default=indefinitely)')
@click.option('-e', '--default-earliest-offset', is_flag=True,
              help='Default to consuming from the earlest available offset if' +
                   ' no committed offset is available.')
@click.option('-j', '--json-value', is_flag=True,
              help='Parse message values as JSON.')
@click.option('-r', '--readable', is_flag=True,
              help='Display messages in a human readable format:' +
                   ' [topic:partition:offset:key] value')
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
@click.option('-vv', '--very-verbose', is_flag=True,
              help='Turn on very verbose logging.')
def consume(topic,
            bootstrap_brokers,
            consumer_group,
            fetch_timeout,
            default_earliest_offset,
            json_value,
            readable,
            verbose,
            very_verbose):
    '''Consume messages from a Kafka topic, or topics.
       By default, connect to a kafka cluster at localhost:9092 and consume new
       messages on the topic(s) indefinitely, outputting in JSON format.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if very_verbose else
               logging.INFO if verbose else
               logging.WARNING
    )
    logging.captureWarnings(True)

    bootstrap_brokers = bootstrap_brokers.split(',')

    consumer = KafkaConsumer(
        *topic,
        bootstrap_servers=bootstrap_brokers,
        value_deserializer=value_deserializer,
        key_deserializer=key_deserializer,
        auto_offset_reset='earliest' if default_earliest_offset else 'latest',
        consumer_timeout_ms=fetch_timeout * 1000,
        group_id=consumer_group,
        max_partition_fetch_bytes=32 * 1024 * 1024,  # 32MB, default 1MB
    )

    try:
        start_s = time.monotonic()
        count = 0
        for message in consumer:
            value = message.value
            value_string = value

            if json_value:
                value = json.loads(value)
                value_string = json.dumps(value,
                                          indent=True,
                                          ensure_ascii=False,
                                          sort_keys=True)

            if readable:
                print('[{}:{}:{}:{}] {}'.format(
                      message.topic,
                      message.partition,
                      message.offset,
                      message.key,
                      value_string))
            else:
                output = {
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'key': message.key,
                    'value': value,
                }
                json.dump(output, sys.stdout, separators=(',', ':'))
                sys.stdout.write('\n')

            count += 1

            now_s = time.monotonic()
            duration_s = now_s - start_s
            if duration_s > 5 and count:
                sys.stdout.flush()
                logging.info('Consumed %(count)s messages in %(duration_s)ss, %(rate)s/s',
                             {'count': count, 'duration_s': duration_s, 'rate': count / duration_s})
                count = 0
                start_s = now_s

    except KeyboardInterrupt:
        pass

    consumer.close()


@click.command()
@click.argument('topic')
@click.option('-b', '--bootstrap-brokers', default='localhost',
              help='Addresses of brokers in a Kafka cluster to talk to.' +
                   ' Brokers should be separated by commas' +
                   ' e.g. broker1,broker2.' +
                   ' Ports can be provided if non-standard (9092)' +
                   ' e.g. broker1:9999. (default: localhost)')
@click.option('-j', '--json-value', is_flag=True,
              help='Parse message values as JSON.')
@click.option('-c', '--compression', type=click.Choice(['snappy']),
              help='Which compression to use when producing messages. (default: None)')
@click.option('--produce-buffer-length', type=int, default=1000,
              help='Max messages to buffer waiting for acks from the brokers. (default=1000)')
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
@click.option('-vv', '--very-verbose', is_flag=True,
              help='Turn on very verbose logging.')
def produce(topic,
            bootstrap_brokers,
            json_value,
            compression,
            produce_buffer_length,
            verbose,
            very_verbose):
    '''Produce messages to a Kafka topic.
       By default, connect to a kafka cluster at localhost:9092.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if very_verbose else
               logging.INFO if verbose else
               logging.WARNING
    )
    logging.captureWarnings(True)

    bootstrap_brokers = bootstrap_brokers.split(',')

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_brokers,
        value_serializer=value_serializer,
        key_serializer=key_serializer,
        # TODO: make configurable
        acks='all',
        compression_type=compression,
        # TODO: make these configurable?
        linger_ms=200,
        batch_size=512 * 1024,  # 512kB, default 16kB
    )

    def free_queue_space(produce_buffer, ensure_space=False):
        if not produce_buffer:
            return produce_buffer

        done = 0
        for fut in produce_buffer:
            if fut.is_done:
                fut.get()
                done += 1
            else:
                break

        # If the buffer was full, and we were supposed to clear some space,
        # and we haven't, wait until we can clear the first slot.
        if len(produce_buffer) >= produce_buffer_length and ensure_space and not done:
            # TODO: How long should we wait? How do we set this?
            wait_end_s = time.monotonic() + 0.1
            logging.info('Starting waiting for produce futures')
            for fut in produce_buffer:
                fut.get()
                done += 1

                if time.monotonic() >= wait_end_s:
                    break

            logging.info('Finished waiting for produce futures')

        if not done:
            return produce_buffer

        logging.info('Confirmed production of %(count)s messages', {'count': done})

        new_produce_buffer = produce_buffer[done:]

        return new_produce_buffer

    produce_buffer = []

    try:
        for line in sys.stdin:

            if len(produce_buffer) >= produce_buffer_length:
                produce_buffer = free_queue_space(produce_buffer, ensure_space=True)

            message = json.loads(line)
            value = message['value']
            value_string = value

            if json_value:
                value_string = json.dumps(value,
                                          indent=True,
                                          ensure_ascii=False,
                                          sort_keys=True)

            args = {
                'topic': topic,
                'value': value_string
            }

            if 'key' in message:
                args['key'] = message['key']

            if 'partition' in message:
                args['partition'] = message['partition']

            fut = producer.send(**args)
            produce_buffer.append(fut)

    except KeyboardInterrupt:
        pass

    producer.flush()
    if produce_buffer:
        for fut in produce_buffer:
            fut.get()
        logging.info('Confirmed production of %(count)s messages',
                     {'count': len(produce_buffer)})

    producer.close()


@click.command()
@click.argument('source_topic')
@click.argument('destination_topic')
@click.option('-b', '--bootstrap-brokers', default='localhost',
              help='Addresses of brokers in a Kafka cluster to talk to.' +
                   ' Brokers should be separated by commas' +
                   ' e.g. broker1,broker2.' +
                   ' Ports can be provided if non-standard (9092)' +
                   ' e.g. broker1:9999. (default: localhost)')
@click.option('-g', '--consumer-group', default=None,
              help='The consumer group to use. Offsets will be periodically' +
                   ' committed.' +
                   ' Consumption will start from the committed offsets,' +
                   ' if available.')
@click.option('-t', '--fetch-timeout', type=float, default=float('inf'),
              help='How many seconds to wait for a message when fetching before ' +
                   'exiting. (default=indefinitely)')
@click.option('-e', '--default-earliest-offset', is_flag=True,
              help='Default to consuming from the earlest available offset if' +
                   ' no committed offset is available.')
@click.option('-c', '--compression', type=click.Choice(['snappy']),
              help='Which compression to use when producing messages. (default: None)')
@click.option('--produce-buffer-length', type=int, default=1000,
              help='Max messages to buffer waiting for acks from the brokers. (default=1000)')
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
@click.option('-vv', '--very-verbose', is_flag=True,
              help='Turn on very verbose logging.')
def pipe(source_topic,
         destination_topic,
         bootstrap_brokers,
         consumer_group,
         fetch_timeout,
         default_earliest_offset,
         compression,
         produce_buffer_length,
         verbose,
         very_verbose):
    '''Consume messages from a Kafka topic, and produce to another Kafka topic.
       By default, connect to a kafka cluster at localhost:9092 and consume new
       messages on the source topic indefinitely, producing to the destination topic.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if very_verbose else
               logging.INFO if verbose else
               logging.WARNING
    )
    logging.captureWarnings(True)

    bootstrap_brokers = bootstrap_brokers.split(',')

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_brokers,
        value_deserializer=value_deserializer,
        key_deserializer=key_deserializer,
        auto_offset_reset='earliest' if default_earliest_offset else 'latest',
        consumer_timeout_ms=min(fetch_timeout * 1000, 500),
        group_id=consumer_group,
        enable_auto_commit=False,
        max_partition_fetch_bytes=32 * 1024 * 1024,  # 32MB, default 1MB
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_brokers,
        value_serializer=value_serializer,
        key_serializer=key_serializer,
        # TODO: make configurable
        acks='all',
        compression_type=compression,
        # TODO: make these configurable?
        linger_ms=200,
        batch_size=512 * 1024,  # 512kB, default 16kB
    )

    commit_interval = consumer.config['auto_commit_interval_ms'] / 1000
    offset_manager = OffsetManager(consumer, commit_interval)
    rebalance_listener = OffsetManagerRebalanceListener(offset_manager)

    consumer.subscribe(topics=[source_topic], listener=rebalance_listener)

    def free_queue_space(produce_buffer, ensure_space=False):
        if not produce_buffer:
            return produce_buffer

        done = 0
        for (tp, offset, fut) in produce_buffer:
            if fut.is_done:
                fut.get()
                offset_manager.report_consumed(tp, offset)
                done += 1
            else:
                break

        # If the buffer was full, and we were supposed to clear some space,
        # and we haven't, wait until we can clear at least the first slot.
        if len(produce_buffer) >= produce_buffer_length and ensure_space and not done:
            # TODO: How long should we wait? How do we set this?
            wait_end_s = time.monotonic() + 0.1
            logging.info('Starting waiting for produce futures')
            for tp, offset, fut in produce_buffer:
                fut.get()
                offset_manager.report_consumed(tp, offset)
                done += 1

                if time.monotonic() >= wait_end_s:
                    break

            logging.info('Finished waiting for produce futures')

        if not done:
            return produce_buffer

        logging.info('Confirmed production of %(count)s messages', {'count': done})

        new_produce_buffer = produce_buffer[done:]

        return new_produce_buffer

    produce_buffer = []
    last_message_time = time.monotonic()
    try:
        while time.monotonic() < (last_message_time + fetch_timeout):
            for m in consumer:
                last_message_time = time.monotonic()

                if len(produce_buffer) >= produce_buffer_length:
                    produce_buffer = free_queue_space(produce_buffer, ensure_space=True)

                tp = TopicPartition(m.topic, m.partition)
                offset = m.offset
                args = {'topic': destination_topic, 'key': m.key, 'value': m.value}
                fut = producer.send(**args)
                produce_buffer.append((tp, offset, fut))

                if offset_manager.should_commit():
                    produce_buffer = free_queue_space(produce_buffer)
                    offset_manager.maybe_commit()

            if offset_manager.should_commit():
                produce_buffer = free_queue_space(produce_buffer)
                offset_manager.maybe_commit()

    except KeyboardInterrupt:
        pass

    producer.flush()
    if produce_buffer:
        for (tp, offset, fut) in produce_buffer:
            fut.get()
            offset_manager.report_consumed(tp, offset)
        logging.info('Confirmed production of %(count)s messages',
                     {'count': len(produce_buffer)})
        offset_manager.commit()

    producer.close()
    consumer.close(autocommit=False)


main.add_command(watermark)
main.add_command(fetch)
main.add_command(position)
main.add_command(seek)
main.add_command(consume)
main.add_command(produce)
main.add_command(pipe)
