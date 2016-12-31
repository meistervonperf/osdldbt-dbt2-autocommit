echo # osdldbt-dbt2-autocommit

A modification of osdldbt-dbt2 to use autocommit transaction provided with PostgreSQL.
This is effective for performance improvement as it eliminates the need for transfering 
BEGIN and COMMIT (or ROLLBACK) messages between the DB server and each client.
