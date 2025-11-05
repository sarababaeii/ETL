A universal Extract, Transform, and Load (ETL) pipeline for arbitrary PostgreSQL databases.
It utilizes a topological sort on the DAG of tasks to automatically detect dependencies between database tables.
Includes a data warehouse component capable of reconstructing and analyzing the state of the database at any point in time.
