import click
import json
import logging
import sys

from kafka import KafkaConsumer

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
        value_deserializer=lambda v: v.decode('utf-8'),
        key_deserializer=lambda k: k.decode('utf-8'),
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


main.add_command(consume)
