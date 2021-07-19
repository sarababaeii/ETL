import psycopg2
import psycopg2.extras
from sql_codes import SQLCodes


class Database:
    def __init__(self, host, database_name, user, password):
        self.connection = psycopg2.connect(
            host=host,
            database=database_name,
            user=user,
            password=password)

    def get_tables(self):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(str(SQLCodes.fetch_all_tables))
        tables = cursor.fetchall()
        cursor.close()
        return tables

    def get_foreign_keys(self):
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(str(SQLCodes.fetch_all_foreign_keys))
        constraints = cursor.fetchall()
        cursor.close()
        return constraints

    def fetch_all_rows(self, node):
        where_dict = {"table_schema": node.table_schema, "table_name": node.table_name}
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""SELECT *
                          FROM information_schema.columns
                          WHERE table_schema = %(table_schema)s
                          AND table_name   = %(table_name)s
                          ORDER BY ordinal_position""",
                       where_dict)
        rows = cursor.fetchall()
        cursor.close()
        print(rows)

    # def delete_from_table(self, , warehouse):
