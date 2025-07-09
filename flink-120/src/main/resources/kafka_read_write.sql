create table source_table (
  `user_id` bigint,
  `item_id` bigint,
  `behavior` string,
  `kafka_ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  `ts` as proctime()
) with (
  'connector' = 'kafka',
  'topic' = 'source_topic',
  'properties.bootstrap.servers' = '192.168.2.200:9092',
  'properties.group.id' = 'flink_read',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json'
);

create table sink_table (
  `user_id` bigint,
  `item_id` bigint,
  `behavior` string,
  `kafka_ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) with (
  'connector' = 'kafka',
  'topic' = 'sink_topic',
  'properties.bootstrap.servers' = '192.168.2.200:9092',
  'format' = 'json'
);

insert into sink_table
select
  `user_id`,
  `item_id`,
  `behavior`,
  `kafka_ts`
from source_table;
