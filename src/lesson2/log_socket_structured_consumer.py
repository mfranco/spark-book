from pyspark.sql import SparkSession
import argparse


def consume_records(host='localhost', port=9876, port2=12345):
    """
    Create a local StreamingContext with two working
    thread and batch interval
    """
    spark = SparkSession.builder.appName("StructuredLogConsumer").getOrCreate()

    stream = spark.readStream.format("socket")\
        .option("host", host)\
        .option("port", port)\
        .load()

    entries = stream.select(stream.value.alias('entry'))

    query = entries \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false")\
        .start()

    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--port', required=False, default=9876, help='Port', type=int)

    args, extra_params = parser.parse_known_args()

    consume_records(port=args.port)


if __name__ == '__main__':
    main()
