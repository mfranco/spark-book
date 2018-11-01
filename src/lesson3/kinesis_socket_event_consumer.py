from datetime import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

import argparse
import json
import os

def parse_entry(msg):
    """
    Event TCP sends sends data in the format
    timestamp:event\n
    """
    values = msg.split(';')
    return {
        'dt': datetime.strptime(
            values[0], '%Y-%m-%d %H:%M:%S.%f'),
        'event': values[1]
    }


def update_global_event_counts(key_value_pairs):
    def update(new_values, accumulator):
        if accumulator is None:
            accumulator = 0
        return sum(new_values, accumulator)

    return key_value_pairs.updateStateByKey(update)

def aggregate_by_event_type(record):
    """
    Step 1. Maps every entry to a dictionary.
    Step 2. Transform the dataset in a set of
        tuples (event, 1)
    Step 3: Applies a reduction by event type
        to count the number of events by type
        in a given interval of time.
    """
    return record\
        .map(lambda x: json.loads(x))\
        .map(parse_entry)\
        .map(lambda record: (record['event'], 1))\
        .reduceByKey(lambda a, b: a+b)


def aggregate_joined_stream(pair):
    key = pair[0]
    values = [val for val in pair[1] if val is not None]
    return(key, sum(values))


def join_aggregation(kinesis_stream, tcp_stream):
    key_value_kinesis = kinesis_stream\
        .map(lambda s: json.loads(s))\
        .map(lambda s: parse_entry(s['value']))\
        .map(lambda record: (record['event'], 1))
    #key_value_kinesis.pprint()
    kinesis_event_counts = update_global_event_counts(key_value_kinesis)
    #kinesis_event_counts.pprint()

    key_value_tcp = tcp_stream.map(parse_entry)\
        .map(lambda record: (record['event'], 1))
    #key_value_tcp.pprint()
    tcp_event_counts = update_global_event_counts(key_value_tcp)
    #tcp_event_counts2.pprint()

    n_counts_joined = kinesis_event_counts.leftOuterJoin(tcp_event_counts)
    n_counts_joined.pprint()
    n_counts_joined.map(aggregate_joined_stream).pprint()



def consume_records(
        interval=1, StreamName=None, region_name='us-west-2', port=9876):
    """
    Create a local StreamingContext with two working
    thread and batch interval
    """
    assert StreamName is not None

    endpoint = 'https://kinesis.{}.amazonaws.com/'.format(region_name)

    sc, stream_context = initialize_context(interval=interval)
    sc.setLogLevel("INFO")
    kinesis_stream = KinesisUtils.createStream(
        stream_context, 'EventLKinesisConsumer', StreamName, endpoint,
        region_name, InitialPositionInStream.LATEST, interval)

    tcp_stream = stream_context.socketTextStream('localhost', port)

    join_aggregation(kinesis_stream, tcp_stream)

    stream_context.start()
    stream_context.awaitTermination()


def initialize_context(interval=1, checkpointDirectory='/tmp'):
    """
    Creates a SparkContext, and a StreamingContext object.
    Initialize checkpointing
    """
    spark_context = SparkContext(appName='EventLKinesisConsumer')
    spark_context.setLogLevel("ERROR")
    stream_context = StreamingContext(spark_context, interval)
    stream_context.checkpoint(checkpointDirectory)
    return spark_context, stream_context


def parse_known_args():
    # AWS credentials should be provided as environ variables
    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        print('Error. Please setup AWS_ACCESS_KEY_ID')
        exit(1)
    elif 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        print('Error. Please setup AWS_SECRET_ACCESS_KEY')
        exit(1)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--interval', required=False, default=1.0,
        help='Interval in seconds', type=float)

    parser.add_argument(
        '--region_name', required=False,
        help='AWS Region', default='us-west-2')

    parser.add_argument('--StreamName', required=True, help='Stream name')

    parser.add_argument(
        '--port', required=False, default=9876,
        help='Port', type=int)

    args, extra_params = parser.parse_known_args()

    return args, extra_params


def main():
    args, extra_params = parse_known_args()
    consume_records(
        interval=args.interval, StreamName=args.StreamName,
        region_name=args.region_name, port=args.port)


if __name__ == '__main__':
    main()
