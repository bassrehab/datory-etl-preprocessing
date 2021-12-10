#!/bin/sh
set -x
BASE_NM=`basename $0`
BASE_PATH=`dirname $0`
echo $BASE_PATH

if [ "$BASE_PATH" = "." ]; then
    BASE_PATH=`pwd`
fi

export ENV=`echo $BASE_PATH | rev | awk -F "/" '{ print $5"/"$6"/"$7"/"$8"/"$9}'| rev | sed 's/\/\///g'`
export ENVNM=`echo $BASE_PATH | rev | awk -F "/" '{ print $4}'| rev`
export FRAME_BASE_PROP=/$ENV/$ENVNM
export DATABASE_PROP=$FRAME_BASE_PROP/edf/config/database.properties
export APPLICATION_PROPERTIES=$FRAME_BASE_PROP/edf/config/cluster.application.properties

METADATA_DRIVER_INFO=`grep 'JDBC.Driver' $DATABASE_PROP |  head -1 | awk -F "=" '{print $2}'`
METADATA_URI=`grep 'JDBC.ConnectionURL' $DATABASE_PROP |  head -1 | awk -F "=" '{print $2}'`
METADATA_USERNAME=`grep 'JDBC.Username' $DATABASE_PROP |  head -1 | awk -F "=" '{print $2}'`
METADATA_PASSWORD=`grep -v "^\s*[#\;]\|^\s*$" $DATABASE_PROP | grep 'JDBC.Password' |  head -1 |  cut -d '=' -f 2-`

echo "Modifying for the database configurations"

echo "" >> $APPLICATION_PROPERTIES
echo "metadata-database-driver=$METADATA_DRIVER_INFO" >> $APPLICATION_PROPERTIES
echo "metadata-jdbc-connection-string=$METADATA_URI" >> $APPLICATION_PROPERTIES
echo "metadata-database-username=$METADATA_USERNAME" >> $APPLICATION_PROPERTIES
echo "metadata-database-password=$METADATA_PASSWORD" >> $APPLICATION_PROPERTIES

echo "Copying the Application Properties to HDFS"

USER=`whoami`
hadoop fs -mkdir /user/$USER/amh
hadoop fs -chmod a+x /user/$USER/amh
hadoop fs -put -f $APPLICATION_PROPERTIES hdfs:///user/$USER/amh

