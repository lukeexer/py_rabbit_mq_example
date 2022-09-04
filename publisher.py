# pylint: disable=W0602, W0603
'''Rabbit publisher share library.'''

import pika

from retry import retry

RABBIT_MQ_HOST = 'localhost'
RABBIT_MQ_ACCOUNT = None
RABBIT_MQ_PASSWORD = None
RABBIT_MQ_CONNECTION_RETRY_TIMES = -1
RABBIT_MQ_CONNECTION_RETRY_DELAY = 1

RABBIT_MQ_PUBLISHER_CONNECTION = None
RABBIT_MQ_PUBLISHER_CHANNEL = None

class SPublisher():
    '''Slibrary publisher library.'''

    @staticmethod
    def init(account, password, host='localhost'):
        '''Initialize Rabbit MQ library.'''

        global RABBIT_MQ_HOST
        global RABBIT_MQ_ACCOUNT
        global RABBIT_MQ_PASSWORD

        RABBIT_MQ_HOST = host
        RABBIT_MQ_ACCOUNT = account
        RABBIT_MQ_PASSWORD = password

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

        global RABBIT_MQ_ACCOUNT
        global RABBIT_MQ_PASSWORD
        global RABBIT_MQ_PUBLISHER_CONNECTION
        global RABBIT_MQ_PUBLISHER_CHANNEL

        if RABBIT_MQ_PUBLISHER_CONNECTION is None:
            credentials = pika.PlainCredentials(RABBIT_MQ_ACCOUNT, RABBIT_MQ_PASSWORD)
            param = pika.ConnectionParameters(host=RABBIT_MQ_HOST, credentials=credentials)
            RABBIT_MQ_PUBLISHER_CONNECTION = pika.BlockingConnection(param)

        if RABBIT_MQ_PUBLISHER_CHANNEL is None:
            RABBIT_MQ_PUBLISHER_CHANNEL = RABBIT_MQ_PUBLISHER_CONNECTION.channel()

        print('Connect to Rabbit MQ server successfully...')

        return RABBIT_MQ_PUBLISHER_CHANNEL

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
    def pub_direct(target_queue, message_body):
        '''Publish message into direct type queue.'''

        global RABBIT_MQ_PUBLISHER_CONNECTION
        global RABBIT_MQ_PUBLISHER_CHANNEL

        if RABBIT_MQ_PUBLISHER_CONNECTION is None:
            SPublisher.connect()

        try:
            if RABBIT_MQ_PUBLISHER_CHANNEL is None:
                raise pika.exceptions.AMQPChannelError

            exchange_name = 'direct.all'
            queue_name = target_queue
            RABBIT_MQ_PUBLISHER_CHANNEL.exchange_declare(
                exchange=exchange_name,
                exchange_type='direct',
                passive=False,
                durable=False,
                auto_delete=False)
            RABBIT_MQ_PUBLISHER_CHANNEL.queue_declare(queue=queue_name, durable=False)
            RABBIT_MQ_PUBLISHER_CHANNEL.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=queue_name)

            RABBIT_MQ_PUBLISHER_CHANNEL.basic_publish(
                exchange=exchange_name,
                routing_key=queue_name,
                body=message_body)
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.AMQPChannelError,
            pika.exceptions.AMQPConnectionError):

            print('init function throws exception.')

            RABBIT_MQ_PUBLISHER_CONNECTION.close()
            RABBIT_MQ_PUBLISHER_CHANNEL = None
            RABBIT_MQ_PUBLISHER_CONNECTION = None

            raise

        print('Direct message sent.')

    @staticmethod
    def pub_fanout(target_exchange, message_body):
        '''Publish message into fanout type queue.'''

        global RABBIT_MQ_PUBLISHER_CONNECTION
        global RABBIT_MQ_PUBLISHER_CHANNEL

        if RABBIT_MQ_PUBLISHER_CONNECTION is None:
            SPublisher.connect()

        try:
            if RABBIT_MQ_PUBLISHER_CHANNEL is None:
                raise pika.exceptions.AMQPChannelError

            exchange_name = target_exchange
            RABBIT_MQ_PUBLISHER_CHANNEL.exchange_declare(
                exchange=exchange_name,
                exchange_type='fanout',
                passive=False,
                durable=False,
                auto_delete=False)

            RABBIT_MQ_PUBLISHER_CHANNEL.basic_publish(
                exchange=exchange_name,
                routing_key='',
                body=message_body)
        except (
            pika.exceptions.ConnectionClosedByBroker,
            pika.exceptions.AMQPChannelError,
            pika.exceptions.AMQPConnectionError):

            print('init function throws exception.')

            RABBIT_MQ_PUBLISHER_CONNECTION.close()
            RABBIT_MQ_PUBLISHER_CHANNEL = None
            RABBIT_MQ_PUBLISHER_CONNECTION = None

            raise

        print('Fanout message sent.')

if __name__ == '__main__':
    SPublisher.init('user', 'user')

    # Direct publish only need to publish message to a specified target queue.
    SPublisher.pub_direct('hello.1', 'Hello1 La.')
    SPublisher.pub_direct('hello.1', 'Hello1 Lai.')
    SPublisher.pub_direct('hello.1', 'Hello1 Laii.')
    SPublisher.pub_direct('hello.1', 'Hello1 Laiii.')

    SPublisher.pub_direct('hello.2', 'Hello2 La.')
    SPublisher.pub_direct('hello.2', 'Hello2 Lai.')
    SPublisher.pub_direct('hello.2', 'Hello2 Laii.')
    SPublisher.pub_direct('hello.2', 'Hello2 Laiii.')

    # Fanout publish only need to publish message to a specified target exchange.
    SPublisher.pub_fanout('fanout.all', 'Hello.fanout La.')
    SPublisher.pub_fanout('fanout.all', 'Hello.fanout Lai.')
    SPublisher.pub_fanout('fanout.all', 'Hello.fanout Laii.')
    SPublisher.pub_fanout('fanout.all', 'Hello.fanout Laiii.')
