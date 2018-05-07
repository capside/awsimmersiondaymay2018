#name string,id int,nametype string,recclass string,mass(g) string,fall string,year date ,reclat double,reclong double,GeoLocation string
SELECT manual_meteoritos.name, manual_meteoritos."mass(g)", manual_meteoritos.fall, manual_meteoritos.year
FROM manual_meteoritos
WHERE ST_DISTANCE (ST_POINT(manual_meteoritos.reclat,manual_meteoritos.reclong),ST_POINT(40.41,-3.7)) < 6.0
order by  manual_meteoritos."mass(g)" desc


CREATE EXTERNAL TABLE `meteoritos`(
  `name` string, 
  `id` bigint, 
  `nametype` string, 
  `recclass` string, 
  `mass (g)` double, 
  `fall` string, 
  `year` string, 
  `reclat` double, 
  `reclong` double, 
  `geolocation` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://immersiondaymay2018/meteoritos/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='crawler-meteoritos', 
  'averageRecordSize'='85', 
  'classification'='csv', 
  'columnsOrdered'='true', 
  'compressionType'='none', 
  'delimiter'=',', 
  'objectCount'='1', 
  'recordCount'='58566', 
  'sizeKey'='4978151', 
  'skip.header.line.count'='1', 
  'typeOfData'='file')


SELECT meteoritos.name, meteoritos."mass (g)", meteoritos.fall, meteoritos.year
FROM meteoritos
WHERE ST_DISTANCE (ST_POINT(meteoritos.reclat,meteoritos.reclong),ST_POINT(40.41,-3.7)) < 6.0
order by  meteoritos."mass (g)" desc
