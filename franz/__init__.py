import click
import json
import logging
import re
import sys

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition
from kafka.partitioner.default import DefaultPartitioner


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


def produce_batch(producer, topic, batch):
    futures = []
    for message in batch:
        partition = message.get('partition')
        key = message.get('key')
        value = message.get('value')
        future = producer.send(topic, partition=partition, key=key, value=value)
        futures.append(future)

    producer.flush()
    # Get results now we've flushed, so any errors are thrown.
    for future in futures:
        future.get()


CONTEXT_SETTINGS = {
    'help_option_names': ['-h', '--help']
}


@click.group(context_settings=CONTEXT_SETTINGS)
def main():
    pass


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
              help='How long to wait for a message when fetching before ' +
                   'exiting. (default=indefinitely)')
@click.option('-j', '--json-value', is_flag=True,
              help='Parse message values as JSON.')
@click.option('-r', '--readable', is_flag=True,
              help='Display messages in a human readable format:' +
                   ' [topic:partition:offset:key] value')
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
def fetch(topic,
          bootstrap_brokers,
          filter_key,
          fetch_timeout,
          json_value,
          readable,
          verbose):
    '''Fetch a message, or messages, from a Kafka topic partition.
       By default, connect to a kafka cluster at localhost:9092 and fetch
       the message at the specified offset, outputting in JSON format.'''

    filter_keys = filter_key

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if verbose else logging.INFO
    )
    logging.captureWarnings(True)

    topic_dict = {}

    for t in topic:
        # TODO: how to find this per topic...
        all_partitions = list(range(24))

        if filter_keys:
            partitioner = DefaultPartitioner()
            filter_key_partitions = {partitioner(key_serializer(k), all_partitions, all_partitions)
                                     for k in filter_keys}
            all_partitions = [p for p in all_partitions if p in filter_key_partitions]

        # TODO: Parse topic spec properly and provide better error messages.

        match = re.search(r'^([^\[\]]+)(\[([\d,:=+]+)\])?$', t)
        if not match:
            logging.error('Topic argument "{}" is invalid.'.format(t))
            exit(1)

        topic, _, slice_spec = match.groups()

        # TODO: Fetch current earliest/latest offsets and use to support relative slices

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

            if ':' in s:
                start, end = s.split(':', 1)

                if start == '':
                    start = 0
                else:
                    start = int(start)

                if end == '':
                    end = None
                elif end[0] == '+':
                    end = start + int(end[1:])
                else:
                    end = int(end)
            else:
                start = int(s)
                end = start + 1

            for partition in partitions:
                tp = TopicPartition(topic, partition)
                topic_dict[tp] = (start, end)

    bootstrap_brokers = bootstrap_brokers.split(',')

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_brokers,
        value_deserializer=value_deserializer,
        key_deserializer=key_deserializer,
        consumer_timeout_ms=fetch_timeout,
    )

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
def seek(consumer_group,
         topic,
         bootstrap_brokers,
         verbose):
    '''Seek a consumer group to a location in one or more topic partitions.
       By default, connect to a kafka cluster at localhost:9092.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if verbose else logging.INFO
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
            print('Before: {}[{}]'.format(topic, ','.join('{}={}'.format(p, o) for p, o in partition_offsets.items())))

        for tp, offset in topic_dict.items():
            consumer.seek(tp, offset)

        for topic, partition_offsets in current_offsets.items():
            print('After: {}[{}]'.format(topic, ','.join('{}={}'.format(p, o) for p, o in partition_offsets.items())))

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
              help='How long to wait for a message when fetching before ' +
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
def consume(topic,
            bootstrap_brokers,
            consumer_group,
            fetch_timeout,
            default_earliest_offset,
            json_value,
            readable,
            verbose):
    '''Consume messages from a Kafka topic, or topics.
       By default, connect to a kafka cluster at localhost:9092 and consume new
       messages on the topic(s) indefinitely, outputting in JSON format.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if verbose else logging.INFO
    )
    logging.captureWarnings(True)

    bootstrap_brokers = bootstrap_brokers.split(',')

    consumer = KafkaConsumer(
        *topic,
        bootstrap_servers=bootstrap_brokers,
        value_deserializer=value_deserializer,
        key_deserializer=key_deserializer,
        auto_offset_reset='earliest' if default_earliest_offset else 'latest',
        consumer_timeout_ms=fetch_timeout,
        group_id=consumer_group
    )

    try:
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

            sys.stdout.flush()

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
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
def produce(topic,
            bootstrap_brokers,
            json_value,
            verbose):
    '''Produce messages to a Kafka topic.
       By default, connect to a kafka cluster at localhost:9092.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if verbose else logging.INFO
    )
    logging.captureWarnings(True)

    bootstrap_brokers = bootstrap_brokers.split(',')

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_brokers,
        value_serializer=value_serializer,
        key_serializer=key_serializer,
        # TODO: make configurable
        acks='all'
    )

    try:
        for line in sys.stdin:
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

            producer.send(**args)

    except KeyboardInterrupt:
        pass

    producer.flush()
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
              help='How long to wait for a message when fetching before ' +
                   'exiting. (default=indefinitely)')
@click.option('-e', '--default-earliest-offset', is_flag=True,
              help='Default to consuming from the earlest available offset if' +
                   ' no committed offset is available.')
@click.option('-v', '--verbose', is_flag=True,
              help='Turn on verbose logging.')
def pipe(source_topic,
         destination_topic,
         bootstrap_brokers,
         consumer_group,
         fetch_timeout,
         default_earliest_offset,
         verbose):
    '''Consume messages from a Kafka topic, and produce to another Kafka topic.
       By default, connect to a kafka cluster at localhost:9092 and consume new
       messages on the source topic indefinitely, producing to the destination topic.'''

    logging.basicConfig(
        format='[%(asctime)s] %(name)s.%(levelname)s %(threadName)s %(message)s',
        level=logging.DEBUG if verbose else logging.INFO
    )
    logging.captureWarnings(True)

    bootstrap_brokers = bootstrap_brokers.split(',')

    consumer = KafkaConsumer(
        source_topic,
        bootstrap_servers=bootstrap_brokers,
        value_deserializer=value_deserializer,
        key_deserializer=key_deserializer,
        auto_offset_reset='earliest' if default_earliest_offset else 'latest',
        consumer_timeout_ms=fetch_timeout,
        group_id=consumer_group,
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_brokers,
        value_serializer=value_serializer,
        key_serializer=key_serializer,
        # TODO: make configurable
        acks='all'
    )

    try:
        while True:
            res = consumer.poll(timeout_ms=fetch_timeout)
            if not res and fetch_timeout < float('inf'):
                break

            batch = [{'key': m.key, 'value': m.value}
                     for messages in res.values()
                     for m in messages]
            logging.info('Piping batch of %(batch_size)d messages.',
                         {'batch_size': len(batch)})
            produce_batch(producer, destination_topic, batch)

    except KeyboardInterrupt:
        producer.close()
        consumer.close(autocommit=False)
    else:
        producer.close()
        consumer.close(autocommit=True)


main.add_command(fetch)
main.add_command(seek)
main.add_command(consume)
main.add_command(produce)
main.add_command(pipe)
