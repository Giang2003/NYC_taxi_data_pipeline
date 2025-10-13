import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine


class PostgresSQLClient:
    def __init__(self, database, user, password, host="127.0.0.1", port="5432"):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def get_sqlalchemy_engine(self):
        """Create a SQLAlchemy engine with the database in the DSN."""
        url = (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        return create_engine(url)

    def create_conn(self):
        # Establishing the connection
        conn = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        # Creating a cursor object using the cursor() method
        return conn

    def execute_query(self, query, params=None):
        conn = self.create_conn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    if params is not None:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)
            print(f"Query has been executed successfully!")
        finally:
            conn.close()

    def execute_values(self, query, values_list):
        """Execute batch insert using psycopg2.extras.execute_values."""
        conn = self.create_conn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, query, values_list)
            print("Batch insert executed successfully!")
        finally:
            conn.close()

    def get_columns(self, table_name):
        """Return list of column names for the given table using information_schema."""
        # Split schema and table
        if "." in table_name:
            schema_name, pure_table_name = table_name.split(".", 1)
        else:
            schema_name, pure_table_name = "public", table_name

        query = (
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "ORDER BY ordinal_position"
        )
        conn = self.create_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, (schema_name, pure_table_name))
                rows = cur.fetchall()
                return [r[0] for r in rows]
        finally:
            conn.close()