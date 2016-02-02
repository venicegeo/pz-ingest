#!/usr/bin/env bash

# Add repository and install software
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main" >> /etc/apt/sources.list'
wget --quiet -O - http://apt.postgresql.org/pub/repos/apt/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get -y update
sudo apt-get -y install postgresql-9.4-postgis pgadmin3 postgresql-contrib

# Enable extensions
sudo -u postgres psql postgres -c "CREATE EXTENSION adminpack"
sudo -u postgres psql postgres -c "CREATE EXTENSION postgis"

# Open necessary ports. This is for development only, after all.
sudo echo "host    all             all             0.0.0.0/0               md5" >> /etc/postgresql/9.4/main/pg_hba.conf
sudo echo "listen_addresses = '*'" >> /etc/postgresql/9.4/main/postgresql.conf
  
# Create a user
sudo -u postgres createuser -d -i -l -r -s piazza
sudo -u postgres psql postgres -c "ALTER USER piazza WITH PASSWORD 'piazza';"

# Create the database
sudo -u postgres createdb piazza -O piazza
sudo -u postgres psql piazza -c "CREATE EXTENSION postgis"

# Restart the service
sudo service postgresql restart