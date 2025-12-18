#!/bin/bash
BACKUP_DIR="/opt/spark/backups/gold"
CONTAINER_NAME="hive-metastore-postgresql"
DB_USER="hive"
DB_NAME="ecommerce_gold"
DATE=$(date +%Y%m%d_%H%M%S)
FILENAME="backup_${DB_NAME}_${DATE}.sql.gz"

mkdir -p $BACKUP_DIR

docker exec -e PGPASSWORD=hive $CONTAINER_NAME pg_dump -U $DB_USER $DB_NAME | gzip > "$BACKUP_DIR/$FILENAME"


if [ $? -eq 0 ]; then
    echo "Backup success: $BACKUP_DIR/$FILENAME"
    
    find $BACKUP_DIR -name "backup_ecommerce_gold_*.sql.gz" -mtime +7 -delete
else
    echo "Backup failed!"
    exit 1
fi