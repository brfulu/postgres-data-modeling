

#### Create user and default database
```
sudo -u postgres psql
postgres=# CREATE DATABASE studentdb;
postgres=# CREATE USER student WITH ENCRYPTED PASSWORD 'student';
postgres=# ALTER USER student WITH SUPERUSER;
```