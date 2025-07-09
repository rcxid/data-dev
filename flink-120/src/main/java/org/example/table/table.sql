-- create
create table table_name (
  id bigint,
  product string,
  order_time timestamp(3),
  -- 事件时间
  watermark for order_time as order_time - interval '5' second
  -- 处理时间
  ts as proctime()
) with (
  ...
);


insert into table_name
select
  ...
from xxx;