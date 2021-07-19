from enum import Enum


class SQLCodes(Enum):
    fetch_all_tables = """SELECT table_schema, table_name
                          FROM information_schema.tables
                          WHERE table_schema != 'pg_catalog'
                          AND table_schema != 'information_schema'
                          AND table_type='BASE TABLE'
                          ORDER BY table_schema, table_name"""

    fetch_all_foreign_keys = """SELECT kcu.table_schema || '.' ||kcu.table_name as foreign_table,
                                 '>-' as rel,
                                 rel_tco.table_schema || '.' || rel_tco.table_name as primary_table,
                                 string_agg(kcu.column_name, ', ') as fk_columns,
                                 kcu.constraint_name
                          FROM information_schema.table_constraints tco
                                join information_schema.key_column_usage kcu
                                          on tco.constraint_schema = kcu.constraint_schema
                                          and tco.constraint_name = kcu.constraint_name
                                join information_schema.referential_constraints rco
                                          on tco.constraint_schema = rco.constraint_schema
                                          and tco.constraint_name = rco.constraint_name
                                join information_schema.table_constraints rel_tco
                                          on rco.unique_constraint_schema = rel_tco.constraint_schema
                                          and rco.unique_constraint_name = rel_tco.constraint_name
                          WHERE tco.constraint_type = 'FOREIGN KEY'
                          group by kcu.table_schema,
                                     kcu.table_name,
                                     rel_tco.table_name,
                                     rel_tco.table_schema,
                                     kcu.constraint_name
                          order by kcu.table_schema,
                                     kcu.table_name"""

    def __str__(self):
        return str(self.value)
