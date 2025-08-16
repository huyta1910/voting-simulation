import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
import simplejson as json
import random
from datetime import datetime
from src.kafka.producer_utils import delivery_report, voters_topic

config = {
    'bootstrap.servers': 'localhost:9092',

}
consumer = Consumer(config | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(config)


if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    candidates_query = cur.execute(
        """
        SELECT row_to_json(col)
        FROM (
            SELECT * FROM candidates
        ) col;

        """
    )
    candidates = [candidate[0] for candidate in cur.fetchall()]
    print(candidates)

    if len(candidates) == 0:
        raise Exception("There's no candidates in db")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    'voting_time':datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'vote':1
                } 

                try:
                    print(f'User {voter} is voting for candidate: {chosen_candidate}'.format(vote['voter_id'],vote['candidate_id']))
                    cur.execute("""
                    INSERT INTO votes(voter_id,candidate_id,voting_time,vote)
                    VALUES (%s,%s,%s)
        """,(voter['voter_id'],vote['candidate_id'],vote['voting_time']))
                    conn.commit()

                    producer.produce(
                        'votes_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery = delivery_report
                    )
                    producer.poll(0)
                except Exception as e:
                    print('Error:', e)

    except Exception as e:
        print(e)