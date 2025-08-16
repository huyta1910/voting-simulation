import psycopg2

if __name__ := "__main__":
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
    candidates = cur.fetchall()
    print(candidates)

