set -e   # Exit immediately if a command exits with a non-zero status.

pg_restore \
        -v \    #Verbose mode ON_ERROR_STOP=1 Stop the restore process if an error occurs
        --no--owner \   # Do not output commands to set ownership of objects to match the original database
        --no-privilages \       # Do not output commands to set access privileges of objects to match the original database
        -U $POSTGRES_USER \     # Connect as the given user
        -d $POSTGRES_DB /docker-entrypoint-initdb.d/data/dump  # Restore the dump file to the database

# if [-d /docker-entrypoint-initdb.d]



psql \
    -v ON_ERROR_STOP=1 \
    --username $POSTGRES_USER \
    --dbname $POSTGRES_DB \
    < /docker-entrypoint-initdb.d/data.dump>