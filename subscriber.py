# pylint: disable=W0602, W0603
'''Rabbit subscriber share library.'''

import sys
import os

import pika

from retry import retry

RABBIT_MQ_HOST = 'localhost'
RABBIT_MQ_CONNECTION_RETRY_TIMES = -1
RABBIT_MQ_CONNECTION_RETRY_DELAY = 1

RABBIT_MQ_SUBSCRIBER_CONNECTION = None
RABBIT_MQ_SUBSCRIBER_CHANNEL = None

SLIB_MQ_DIRECT = 'direct'
SLIB_MQ_FANOUT = 'fanout'

class SSubscriber():
    '''Slibrary subscriber library.'''

    @staticmethod
    @retry(pika.exceptions.ConnectionClosedByBroker,
        tries=RABBIT_MQ_CONNECTION_RETRY_TIMES,
        delay=RABBIT_MQ_CONNECTION_RETRY_DELAY)
    @retry(pika.exceptions.AMQPChannelError,
        tries=RABBIT_MQ_CONNECTION_RETRY_TIMES,
        delay=RABBIT_MQ_CONNECTION_RETRY_DELAY)
    @retry(pika.exceptions.AMQPConnectionError,
        tries=RABBIT_MQ_CONNECTION_RETRY_TIMES,
        delay=RABBIT_MQ_CONNECTION_RETRY_DELAY)
    def init(source_queue, callback_function, host='localhost'):
        '''Initialize Rabbit MQ subscriber.'''

        global RABBIT_MQ_HOST
        global RABBIT_MQ_SUBSCRIBER_CONNECTION
        global RABBIT_MQ_SUBSCRIBER_CHANNEL

        RABBIT_MQ_HOST = host

        if RABBIT_MQ_SUBSCRIBER_CONNECTION is None:
            SSubscriber.connect()

        try:
            if RABBIT_MQ_SUBSCRIBER_CONNECTION is None:
                raise pika.exceptions.AMQPChannelError

            RABBIT_MQ_SUBSCRIBER_CHANNEL.queue_declare(queue=source_queue)

            RABBIT_MQ_SUBSCRIBER_CHANNEL.basic_consume(
                queue=source_queue,
                on_message_callback=callback_function,
                auto_ack=True)

            print('Waiting for messages... To exit press CTRL+C.')

            RABBIT_MQ_SUBSCRIBER_CHANNEL.start_consuming()
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.AMQPChannelError,
            pika.exceptions.AMQPConnectionError):

            print('init function throws exception.')

            RABBIT_MQ_SUBSCRIBER_CONNECTION.close()
            RABBIT_MQ_SUBSCRIBER_CHANNEL = None
            RABBIT_MQ_SUBSCRIBER_CONNECTION = None

            raise

    @staticmethod
    @retry(pika.exceptions.ConnectionClosedByBroker,
        tries=RABBIT_MQ_CONNECTION_RETRY_TIMES,
        delay=RABBIT_MQ_CONNECTION_RETRY_DELAY)
    @retry(pika.exceptions.AMQPChannelError,
        tries=RABBIT_MQ_CONNECTION_RETRY_TIMES,
        delay=RABBIT_MQ_CONNECTION_RETRY_DELAY)
    @retry(pika.exceptions.AMQPConnectionError,
        tries=RABBIT_MQ_CONNECTION_RETRY_TIMES,
        delay=RABBIT_MQ_CONNECTION_RETRY_DELAY)
    def connect():
        '''Connect to rabbit mq server and return channel.'''

        print('Connect to Rabbit MQ server...')

        global RABBIT_MQ_SUBSCRIBER_CONNECTION
        global RABBIT_MQ_SUBSCRIBER_CHANNEL

        param = (pika.ConnectionParameters(host=RABBIT_MQ_HOST))
        RABBIT_MQ_SUBSCRIBER_CONNECTION = pika.BlockingConnection(param)
        RABBIT_MQ_SUBSCRIBER_CHANNEL = RABBIT_MQ_SUBSCRIBER_CONNECTION.channel()

        print('Connect to Rabbit MQ server successfully...')

        return RABBIT_MQ_SUBSCRIBER_CHANNEL

def customized_callback_function(channel, method, properties, body):
    '''Customized subscriber callback function.'''
    print(f"Received {body}")

if __name__ == '__main__':
    try:
        SSubscriber.init('hello', customized_callback_function)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
