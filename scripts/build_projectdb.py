import psycopg2 as psql
from pprint import pprint
import os

# build connection string
conn_string = "host=hadoop-04.uni.innopolis.ru port=5432 user=team30 dbname=team30_projectdb password=hktzN5Hxb5EYwxCW"

# Connect to the remote dbms
with psql.connect(conn_string) as conn:
    # Create a cursor for executing psql commands
    cur = conn.cursor()
    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "create_tables.sql")) as file:
        content = file.read()
        cur.execute(content)
    conn.commit()

    # Read the commands from the file and execute them.
    with open(os.path.join("sql", "import_data.sql")) as file:
        commands = file.readlines()
        with open(os.path.join("data", "nypd_complaints_final.csv"), "r") as nypd_complaints:
            cur.copy_expert(commands[0], nypd_complaints)

    # If the sql statements are CRUD then you need to commit the change
    conn.commit()

    pprint(conn)
    cur = conn.cursor()
    # Read the sql commands from the file
    with open(os.path.join("sql", "test_database.sql")) as file:
        commands = file.readlines()
        for command in commands:
            cur.execute(command)
            # Read all records and print them
            pprint(cur.fetchall())