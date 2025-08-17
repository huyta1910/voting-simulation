import psycopg2
import simplejson as json
from confluent_kafka import SerializingProducer
from src.data.db_utils import create_tables, insert_voters
from src.kafka.kafka_jobs import delivery_report, voters_topic
from src.data.fetch_data import generate_voter_data, generate_candidate_data

TARGET_VOTERS_COUNT = 1000

if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
    create_tables(conn, cur)

    # get candidates from db
    cur.execute("SELECT * FROM candidates")
    candidates = cur.fetchall()
    print(candidates)

    if len(candidates) == 0:
        for i in range(3):
            candidate = generate_candidate_data(i, 3)
            print(candidate)
            cur.execute("""
                INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['biography'],
                candidate['campaign_platform'], candidate['photo_url']))
            conn.commit()
    try:
        for i in range(TARGET_VOTERS_COUNT):
            voter_data = generate_voter_data()
            insert_voters(conn, cur, voter_data)

            producer.produce(
                voters_topic,
                key=voter_data["voter_id"],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )

            print(f'Produced voter {i} \ndata: {voter_data}')
            producer.flush()
    except KeyboardInterrupt:
        print(f"\nStream stopped by user at {i}/{TARGET_VOTERS_COUNT}.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nJob Finished.")
        conn.close()
        print("Database connection closed.")