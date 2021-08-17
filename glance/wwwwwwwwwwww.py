CREATE EXTERNAL TABLE video_started(
         `apiKey` string,
         `cityId` BIGINT,
         `clientTime` BIGINT,
         `eventName` string,
         `glanceId` string,
         `impressionId` string,
         `impressionType` string,
         `locale` string,
         `partnerId` string,
         `region` string,
         `requestTime` BIGINT,
         `sdkVersion` BIGINT,
         `sessionId` string,
         `sessionMode` string,
         `stateId` BIGINT,
         `time` BIGINT,
         `userId` string,
         `year` integer,
         `month` integer,
         `day` integer,
         `hour` integer
         )
         PARTITIONED BY(`process_date` DATE) 
         STORED AS ORC 
         LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/video_started';



CREATE EXTERNAL TABLE peek_started(
         `apiKey` string,
         `cityId` BIGINT,
         `clientTime` BIGINT,
         `eventName` string,
         `glanceId` string,
         `impressionId` string,
         `impressionType` string,
         `isOfflinePeek` boolean,
         `locale` string,
         `partnerId` string,
         `peekSource` string,
         `region` string,
         `requestTime` BIGINT,
         `sdkVersion` BIGINT,
         `sessionId` string,
         `sessionMode` string,
         `stateId` BIGINT,
         `time` BIGINT,
         `userId` string,
         `year` integer,
         `month` integer,
         `day` integer,
         `hour` integer
         )
         PARTITIONED BY(`process_date` DATE) 
         STORED AS ORC 
         LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/peek_started';



CREATE EXTERNAL TABLE peek_ended(
         `apiKey` string,
         `cityId` BIGINT,
         `clientTime` BIGINT,
         `duration` BIGINT,
         `eventName` string,
         `glanceId` string,
         `impressionId` string,
         `impressionType` string,
         `isOfflinePeek` boolean,
         `locale` string,
         `partnerId` string,
         `region` string,
         `requestTime` BIGINT,
         `sdkVersion` BIGINT,
         `sessionId` string,
         `sessionMode` string,
         `stateId` BIGINT,
         `time` BIGINT,
         `userId` string,
         `year` integer,
         `month` integer,
         `day` integer,
         `hour` integer
         )
         PARTITIONED BY(`process_date` DATE) 
         STORED AS ORC 
         LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/peek_ended';


CREATE EXTERNAL TABLE cta_started(
         `apiKey` string,
         `cityId` BIGINT,
         `clientTime` BIGINT,
         `eventName` string,
         `glanceId` string,
         `impressionId` string,
         `impressionType` string,
         `isOfflinePeek` boolean,
         `locale` string,
         `partnerId` string,
         `region` string,
         `requestTime` BIGINT,
         `sdkVersion` BIGINT,
         `sessionId` string,
         `sessionMode` string,
         `stateId` BIGINT,
         `time` BIGINT,
         `userId` string,
         `year` integer,
         `month` integer,
         `day` integer,
         `hour` integer
         )
         PARTITIONED BY(`process_date` DATE) 
         STORED AS ORC 
         LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/cta_started';


CREATE EXTERNAL TABLE cta_ended(
         `apiKey` string,
         `cityId` BIGINT,
         `clientTime` BIGINT,
         `duration` BIGINT,
         `eventName` string,
         `failed` boolean,
         `glanceId` string,
         `impressionId` string,
         `impressionType` string,
         `isOfflinePeek` boolean,
         `loadDuration` BIGINT,
         `locale` string,
         `partnerId` string,
         `region` string,
         `requestTime` BIGINT,
         `sdkVersion` BIGINT,
         `sessionId` string,
         `sessionMode` string,
         `stateId` BIGINT,
         `time` BIGINT,
         `userId` string,
         `year` integer,
         `month` integer,
         `day` integer,
         `hour` integer
         )
         PARTITIONED BY(`process_date` DATE) 
         STORED AS ORC 
         LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/cta_ended';


CREATE EXTERNAL TABLE video_ended(
    `apiKey` string,
    `cityId` BIGINT,
    `clientTime` BIGINT,
    `duration` BIGINT,
    `eventName` string,
    `failed` boolean,
    `glanceId` string,
    `impressionId` string,
    `impressionType` string,
    `loadDuration` BIGINT,
    `locale` string,
    `partnerId` string,
    `playCallDuration` BIGINT,
    `playStartDuration` BIGINT,
    `region` string,
    `requestTime` BIGINT,
    `sdkVersion` BIGINT,
    `sessionId` string,
    `sessionMode` string,
    `stateId` BIGINT,
    `time` BIGINT,
    `userId` string,
    `year` integer,
    `month` integer,
    `day` integer,
    `hour` integer
    )
    PARTITIONED BY(`process_date` DATE) 
    STORED AS ORC 
    LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/video_ended';


CREATE EXTERNAL TABLE glance_ended(
         `apiKey` string,
         `cityId` integer,
         `clientTime` bigint,
         `day` integer,
         `duration` bigint,
         `eventName` string,
         `glanceId` string,
         `holdDuration` bigint,
         `hour` integer,
         `impressionId` string,
         `impressionType` string,
         `locale` string,
         `month` integer,
         `partnerId` string,
         `region` string,
         `requestTime` bigint,
         `sdkVersion` integer,
         `sessionId` string,
         `sessionMode` string,
         `source` string,
         `stateId` integer,
         `time` bigint,
         `userId` string,
         `year` integer
         )
         PARTITIONED BY(`process_date` DATE) 
         STORED AS ORC 
         LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/glance_ended';

CREATE EXTERNAL TABLE glance_started(
         `apiKey` string,
         `bubbleImpressionId` string,
         `cityId` integer,
         `clientTime` bigint,
         `day` integer,
         `eventName` string,
         `glanceId` string,
         `glancePosition` integer,
         `hour` integer,
         `impressionId` string,
         `impressionType` string,
         `isFeatureBank` boolean,
         `liveStoriesCount` bigint,
         `locale` string,
         `month` integer,
         `networkType` string,
         `notificationCount` bigint,
         `partnerId` string,
         `region` string,
         `requestTime` bigint,
         `sdkVersion` integer,
         `sessionId` string,
         `sessionMode` string,
         `source` string,
         `stateId` integer,
         `time` bigint,
         `userId` string,
         `year` integer
         )
         PARTITIONED BY(`process_date` DATE) 
         STORED AS ORC 
         LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/glance_started';

CREATE EXTERNAL TABLE glance_shared(
    `apiKey` string,
    `cityId` bigint,
    `clientTime` bigint,
    `eventName` string,
    `glanceId` string,
    `impressionId` string,
    `impressionType` string,
    `locale` string,
    `partnerId` string,
    `region` string,
    `requestTime` bigint,
    `sdkVersion` bigint,
    `sessionId` string,
    `sessionMode` string,
    `source` string,
    `stateId` bigint,
    `time` bigint,
    `userId` string,
    `year` integer,
    `month` integer,
    `day` integer,
    `hour` integer
    )
    PARTITIONED BY(`process_date` DATE) 
    STORED AS ORC 
    LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/glance_shared';


CREATE EXTERNAL TABLE glance_like(
    `apiKey` string,
    `cityId` bigint,
    `clientTime` bigint,
    `eventName` string,
    `glanceId` string,
    `impressionId` string,
    `impressionType` string,
    `isLiked` boolean,
    `locale` string,
    `partnerId` string,
    `region` string,
    `requestTime` bigint,
    `sdkVersion` bigint,
    `sessionId` string,
    `sessionMode` string,
    `source` string,
    `stateId` bigint,
    `time` bigint,
    `userId` string,
    `year` integer,
    `month` integer,
    `day` integer,
    `hour` integer
    )
    PARTITIONED BY(`process_date` DATE) 
    STORED AS ORC 
    LOCATION 'gs://glanceaztogcspoc/analytics/searce-orc-final-hour/glance_like';



msck repair table cta_ended;
msck repair table cta_started;
msck repair table glance_ended;
msck repair table glance_like;
msck repair table glance_started;
msck repair table glance_shared;
msck repair table peek_ended;
msck repair table peek_started;
msck repair table video_ended;
msck repair table video_started;


ALTER TABLE glance_ended ADD COLUMNS(gpid int);
ALTER TABLE glance_started ADD COLUMNS(gpid int);

drop table cta_ended;
drop table cta_started;
drop table peek_ended;
drop table peek_started;
drop table video_ended;
drop table video_started;