#!/bin/bash

echo "starting to load data to greenplum ............"
BASE_DIR=/home/sridharpaladugu
PROJ_DIR=$BASE_DIR/workspace/CreditCardTransactionGenerator
DATA_DIR=$PROJ_DIR/data
STAGE_DIR=$PROJ_DIR/stage
ARCHIVE_DIR=$PROJ_DIR/processed

echo "BASE_DIR = $BASE_DIR"
echo "PROJ_DIR = $PROJ_DIR"
echo "DATA_DIR = $DATA_DIR"
echo "STAGE_DIR = $STAGE_DIR"

LD_LIBRARY_PATH=/home/sridharpaladugu/workspace/CreditCardTransactionGenerator/greenplum
export LD_LIBRARY_PATH

timestamp()
{
 date +"%Y-%m-%d %T"
}

while true
do
	echo "*********** STARTING NEW MICRO BATCH ********************************"
	echo "Checking if files arrived ............."
	COUNT=$(ls -l $DATA_DIR/transactions*.csv | wc -l)
	if [ $COUNT -ge 1 ]
	then
		echo "moving top 2 files to staging foler $STAGE_DIR ........"
		for file in `ls -tr $DATA_DIR/transactions*.csv | head -n 2`; do mv $file $STAGE_DIR; done
		echo "launching gpfdist ........"
		$PROJ_DIR/greenplum/gpfdist -d $STAGE_DIR/ -p 8081 > $PROJ_DIR/logs/$(date "+%Y.%m.%d-%H.%M.%S")_gpfdist.log 2>&1 &
		echo "doing parallel inserts into greenplum table .........."
		psql -h sp-pde-gpdb.eastus2.cloudapp.azure.com -U gpadmin -d gpadmin -w -f $PROJ_DIR/insert_gp.sql
		echo "fetching row count ........."
		psql -h sp-pde-gpdb.eastus2.cloudapp.azure.com -U gpadmin -d gpadmin -w -f $PROJ_DIR/query_gp_sql
		echo "stopping gpfdist ......"
		ps -ef | grep gpfdist | grep -v grep | awk '{print $2}' | xargs kill -9 &> /dev/null
		echo "moving processed files to archive folder $ARCHIVE_DIR ......." 
		for file in `ls -tr $STAGE_DIR/transactions*.csv | head -n 2`; do mv $file $ARCHIVE_DIR; done
		sleep 2
	else
		echo "No files arrived yet, sleeping for 10 seconds .........."
		sleep 10
	fi

	echo "*********** FINISHED MICRO BATCH ********************************"
done
