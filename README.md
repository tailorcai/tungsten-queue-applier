Prefetch
--------

get Tungsten Replicator 2.0.4 from: [http://code.google.com/p/tungsten-replicator/](http://code.google.com/p/tungsten-replicator/)

get Kestrel from: [https://github.com/robey/kestrel](https://github.com/robey/kestrel)


Build
-----
Install tungsten-replicator.jar into repository and compile, and the required libs will be download by itself.

    $ mvn install:install-file -DgroupId=com.continuent.tungsten.replicator -DartifactId=tungsten-replicator -Dversion=2.0.4 -Dpackaging=jar -Dfile=/path/to/tungsten-replicator/lib/tungsten_replicator.jar
    $ mvn install

Get your compiled version out from your maven's repository

    $ cp ~/.m2/repository/com/ganji/ganji_tungsten/1.1-SNAPSHOT/ganji_tungsten-1.1-SNAPSHOT.jar /path/to/tungsten-replicator/lib
    $ cp ~/.m2/repository/net/spy/memcached/spymemcached/2.7.3/spymemcached-2.7.3.jar  /path/to/tungsten-replicator/lib


Setup MySQL
-----------
Change binlog format to ROW and create a queue_info db for the queue replicator to store the replication information.

    set global binlog_format="ROW";
    create database cdc_queue_info;

Configure Tungsten-Replicator
-----------------------------

Install tungstren replicator with a master only install. remember to replace the path and usename, password part with your settings.

    #!/bin/sh 
    TUNGSTEN_HOME=$HOME/replication/master 
    MASTER=localhost 
    to/path/tungsten-replicator-2.0.4/tools/tungsten-installer \\
        --master-slave \\
        --master-host=$MASTER \\
        --datasource-user=root \\
        --datasource-password=root \\
        --service-name=mysql2queue \\
        --home-directory=$TUNGSTEN_HOME \\
        --cluster-hosts=$MASTER \\
        --start-and-report 


Change tungsten configuration which is located under your $TUNGSTEN_HOME/replication/master/tungsten/tungsten-replicator/conf
you can do the change directly under master pipeline, but i will setup it under a brand new pipeline "direct"
    vi static-mysql2queue.properties

    change:
    replicator.role=master
    to:
    replicator.role=direct


	change:
	replicator.pipelines=master,slave
	to:
	replicator.pipelines=master,slave,direct
	replicator.pipeline.direct=binlog-to-q,q-to-mc
	replicator.pipeline.direct.stores=queue
	replicator.pipeline.direct.autoSync=true

	replicator.stage.q-to-mc=com.continuent.tungsten.replicator.pipeline.SingleThreadStageTask
	replicator.stage.q-to-mc.extractor=queue
	replicator.stage.q-to-mc.applier=mcqueue
	replicator.stage.q-to-mc.filters=mysqlsessions
	end # here we reuse the binglog-to-q stage from master pipeline

	add:
	replicator.stage.binlog-to-q.filters=colnames

Define new mcqueue applier by putting the following lines at the bottom of APPLIERS section

	# McQueue
	replicator.applier.mcqueue=com.ganji.tungsten.replicator.applier.GanjiMcQueueApplier
	replicator.applier.mcqueue.db=cdc_queue_info   # this is the db used for store queue appiler info, you should create the db manually
	replicator.applier.mcqueue.queueAddr=127.0.0.1:22144  # this is the kestrel queue port
	replicator.applier.mcqueue.queueName=dev_cdc_source   # this is queue name used to receive update


Change mysql extractor from relay log to binlog

	change:
	replicator.extractor.dbms.binlog_dir=/var/log/mysql
	replicator.extractor.dbms.useRelayLogs=true
	to:
	replicator.extractor.dbms.binlog_dir=/var/log/mysql
	replicator.extractor.dbms.useRelayLogs=false
	end

#!!! make sure you have the permission to read mysql binlog

Start
-----

	$TUNGSTEN_HOME/replication/master/tungsten/tungsten-replicator/bin/replicator start
	Starting Tungsten Replicator Service...


	$TUNGSTEN_HOME/replication/master/tungsten/tungsten-replicator/bin/trepctl status 
	NAME                     VALUE
	----                     -----
	appliedLastEventId     : mysql-bin.000012:0000000000001261;152
	appliedLastSeqno       : 6
	appliedLatency         : 1.437
	clusterName            : default
	currentEventId         : mysql-bin.000012:0000000000001261
	currentTimeMillis      : 1333341363533
	dataServerHost         : localhost
	extensions             : 
	host                   : null
	latestEpochNumber      : 6
	masterConnectUri       : thl://:/
	masterListenUri        : thl://localhost:2112/
	maximumStoredSeqNo     : 4
	minimumStoredSeqNo     : 0
	offlineRequests        : NONE
	pendingError           : NONE
	pendingErrorCode       : NONE
	pendingErrorEventId    : NONE
	pendingErrorSeqno      : -1
	pendingExceptionMessage: NONE
	resourcePrecedence     : 99
	rmiPort                : -1
	role                   : direct
	seqnoType              : java.lang.Long
	serviceName            : mysql2queue
	serviceType            : local
	simpleServiceName      : mysql2queue
	siteName               : default
	sourceId               : localhost
	state                  : ONLINE               <------------------- ONLINE means everything is ok
	timeInStateSeconds     : 4.154
	uptimeSeconds          : 5.625
	Finished status command...

Check
-----
Create a database in your mysql called cdc_test , and create a table, try insert something into the table

	# telnet 127.0.0.1 22144
	stats
	stats
	STAT uptime 10667
	STAT time 1333341396
	STAT version 2.1.3
	STAT curr_items 3
	STAT total_items 3
	STAT bytes 237
	STAT curr_connections 1
	STAT total_connections 33
	STAT cmd_get 0
	STAT cmd_set 3
	STAT cmd_peek 0
	STAT get_hits 0
	STAT get_misses 0
	STAT bytes_read 548
	STAT bytes_written 7609
	STAT queue_dev_cdc_source_items 3                        <------- items in the queue
	STAT queue_dev_cdc_source_bytes 237
	STAT queue_dev_cdc_source_total_items 3
	STAT queue_dev_cdc_source_logsize 300
	STAT queue_dev_cdc_source_expired_items 0
	STAT queue_dev_cdc_source_mem_items 3
	STAT queue_dev_cdc_source_mem_bytes 237
	STAT queue_dev_cdc_source_age 0
	STAT queue_dev_cdc_source_discarded 0
	STAT queue_dev_cdc_source_waiters 0
	STAT queue_dev_cdc_source_open_transactions 0
	END

