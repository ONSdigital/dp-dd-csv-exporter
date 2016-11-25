# dp-dd-csv-exporter
Export csv files from the data discovery database filtered by dimension.

# Database creation
----
You will need to create a postgres database and user to run the tests out of the box
- login to postgres: psql -U your_super_user
- create dd role: CREATE ROLE data_discovery LOGIN PASSWORD 'password';
- create dd db: CREATE DATABASE 'data_discovery' OWNER 'data_discovery';