import json
import logging
import os
import time
import pika
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from .db import Session, Event, Receipt, serialize

def consume(message):
    with Session() as db:
        if db.get(Receipt, message['id']):
            return False
        db.add(Receipt(id=message['id']))
        try:
            db.commit()
            return True
        except IntegrityError:
            db.rollback()
            return False

def run():
    while True:
        try:
            parameters = pika.URLParameters(os.environ['RABBITMQ_URL'])
            parameters.blocked_connection_timeout = 10
            parameters.socket_timeout = 3
            parameters.connection_attempts = 3
            parameters.retry_delay = 1
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.exchange_declare('relief.events', exchange_type='fanout', durable=True)
            channel.queue_declare('relief.dead', durable=True)
            channel.queue_declare('relief.worker', durable=True, arguments={'x-dead-letter-exchange':'','x-dead-letter-routing-key':'relief.dead'})
            channel.queue_bind('relief.worker','relief.events')
            channel.confirm_delivery()
            # Transactional outbox: publish before marking; receipts absorb re-delivery after a crash.
            while connection.is_open:
                with Session() as db:
                    for event in db.scalars(select(Event).where(Event.published == 0).order_by(Event.clock).limit(100)):
                        channel.basic_publish('relief.events','',json.dumps(serialize(event)),properties=pika.BasicProperties(delivery_mode=2, message_id=event.id))
                        event.published = 1
                    db.commit()
                method, props, body = channel.basic_get('relief.worker', auto_ack=False)
                if method:
                    try:
                        consume(json.loads(body))
                        channel.basic_ack(method.delivery_tag)
                    except Exception:
                        # One redelivery, then durable dead letter; malformed events cannot spin forever.
                        channel.basic_nack(method.delivery_tag,requeue=not method.redelivered)
                connection.process_data_events(time_limit=0.2)
                time.sleep(0.1)
        except Exception as exc:
            logging.warning(json.dumps({'worker':'reconnecting','error':str(exc)}))
            time.sleep(2)

if __name__ == '__main__':
    run()
