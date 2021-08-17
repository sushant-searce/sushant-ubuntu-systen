create database searce;
use searce;


CREATE EXTERNAL TABLE `cta_ended`(`impressionType` STRING, `region` STRING, `sdkVersion` INTEGER, `locale` STRING, `clientTime`	INTEGER, `requestTime` INTEGER, `cityId` INTEGER, `time` INTEGER, `eventName` STRING, `impressionId`	STRING, `partnerId`	STRING, `userId` STRING, `apiKey` STRING, `glanceId` STRING, `sessionId` STRING, `sessionMode`	STRING, `stateId` INTEGER, `loadDuration` INTEGER, `isOfflinePeek` BOOLEAN, `failed` BOOLEAN, `duration` INTEGER, `hour` INTEGER) Partitioned by (`process_date` date) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-hourly/cta_ended';


CREATE EXTERNAL TABLE `cta_started`(`region` STRING,`impressionType` STRING,`stateId` INTEGER,`sdkVersion` INTEGER,`locale` STRING,`clientTime` INTEGER,`eventName` STRING,`impressionId` STRING,`partnerId` STRING,`userId` STRING,`apiKey` STRING,`isOfflinePeek` BOOLEAN,`glanceId` STRING,`sessionId` STRING,`sessionMode` STRING,`requestTime` INTEGER,`cityId` INTEGER,`time` INTEGER,`hour` INTEGER) Partitioned by (`process_date` date) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-hourly/cta_started';


CREATE EXTERNAL TABLE `orctb`(`apiKey` STRING,`cityId` bigint,`clientTime` bigint,`eventName` STRING,`glanceId` STRING,`impressionId` STRING,`impressionType` STRING,`locale` STRING,`partnerId` STRING,`region` STRING,`requestTime` bigint,`sdkVersion` bigint,`sessionId` STRING,`sessionMode` STRING,`source` STRING,`stateId` bigint,`time` bigint,`userId` STRING,`year` int,`month` int,`day` int,`hour` int) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searcetest';


CREATE EXTERNAL TABLE `glance_ended`(`impressionType` STRING,`region` STRING,`stateId` INTEGER,`sdkVersion` INTEGER,`locale` STRING,`clientTime` INTEGER,`requestTime` INTEGER,`eventName` STRING,`impressionId` STRING,`partnerId` STRING,`userId` STRING,`apiKey` STRING,`holdDuration` INTEGER,`sessionId` STRING,`glanceId` STRING,`sessionMode` STRING,`source` STRING,`cityId` INTEGER,`time` INTEGER,`duration` INTEGER,`year` INTEGER,`month` INTEGER,`day` INTEGER,`hour` INTEGER,`gpid` STRING) Partitioned by (`process_date` date) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-hourly/glance_ended';


CREATE EXTERNAL TABLE `glance_like`(`impressionType` STRING,`region` STRING,`stateId` INTEGER,`sdkVersion` INTEGER,`locale` STRING,`clientTime` INTEGER,`requestTime` INTEGER,`eventName` STRING,`impressionId` STRING,`partnerId` STRING,`userId` STRING,`apiKey` STRING,`glanceId` STRING,`sessionMode` STRING,`sessionId` STRING,`cityId` INTEGER,`time` INTEGER,`source` STRING,`isLiked` BOOLEAN,`hour` INTEGER) Partitioned by (`process_date` date) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-hourly/glance_like';


CREATE EXTERNAL TABLE `searce.glance_shared_test`(`impressionType` string,`region` string,`stateId` int,`sdkVersion` int,`locale`	string, `clientTime` int,`eventName` string,`impressionId` string,`partnerId` string,`userId` string,`apiKey` string,`glanceId` string,`sessionId`	string,`sessionMode` string,`requestTime` int,`cityId` int,`time` int,`source` string, `hour` int) Partitioned by (`process_date` date) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-hourly/glance_sharedv2';
 
 
LOAD DATA INPATH '/hdfs/dir/folder/to/orc/files/' INTO TABLE MyDB.TEST;




CREATE EXTERNAL TABLE `peek_ended`(`impressionType` STRING,`region` STRING,`stateId` INTEGER,`sdkVersion` INTEGER,`locale` STRING,`clientTime` INTEGER,`requestTime` INTEGER,`eventName` STRING,`impressionId` STRING,`partnerId` STRING,`userId` STRING,`apiKey` STRING,`glanceId` STRING,`sessionMode` STRING,`sessionId` STRING,`isOfflinePeek` BOOLEAN,`duration` INTEGER,`cityId` INTEGER,`time` INTEGER,`hour` INTEGER) Partitioned by (`process_date` date) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-hourly/peek_ended';


CREATE EXTERNAL TABLE `peek_started`(`impressionType` STRING, `region` STRING, `stateId` INTEGER, `sdkVersion` INTEGER, `locale` STRING, `clientTime` INTEGER, `eventName` STRING, `impressionId` STRING, `partnerId` STRING, `userId` STRING, `apiKey` STRING, `isOfflinePeek` BOOLEAN, `glanceId` STRING, `sessionId` STRING, `sessionMode` STRING, `requestTime` INTEGER, `peekSource` STRING, `cityId` INTEGER, `time` INTEGER,	`hour` INTEGER) Partitioned by (`process_date` date) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-hourly/peek_started';


CREATE EXTERNAL TABLE `video_ended`(`region` STRING,`sdkVersion` INTEGER,`locale` STRING,`clientTime` INTEGER,`requestTime` INTEGER,`cityId` INTEGER,`time` INTEGER,`eventName` STRING,`userId` STRING,`playCallDuration` INTEGER,`apiKey` STRING,`impressionId` STRING,`partnerId` STRING,`impressionType` STRING,`glanceId` STRING,`playStartDuration` INTEGER,`sessionId` STRING,`sessionMode` STRING,`stateId` INTEGER,`loadDuration` INTEGER,`failed` BOOLEAN,`duration` INTEGER,`hour` INTEGER) Partitioned by (`process_date` date) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-hourly/video_ended';


CREATE EXTERNAL TABLE `video_started`(`region` STRING,`stateId` INTEGER,`sdkVersion` INTEGER,`locale` STRING,`clientTime` INTEGER,`userId` STRING,`apiKey` STRING,`eventName` STRING,`impressionId` STRING,`partnerId` STRING,`impressionType` STRING,`glanceId` STRING,`sessionId` STRING,`sessionMode` STRING,`requestTime` INTEGER,`cityId` INTEGER,`time` INTEGER,`hour` INTEGER) Partitioned by (`process_date` date) STORED AS ORC LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-hourly/video_started';