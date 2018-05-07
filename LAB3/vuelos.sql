

CREATE EXTERNAL TABLE `vuelos_parquet`(
  `yr` int, 
  `quarter` int, 
  `month` int, 
  `dayofmonth` int, 
  `dayofweek` int, 
  `flightdate` string, 
  `uniquecarrier` string, 
  `airlineid` int, 
  `carrier` string, 
  `tailnum` string, 
  `flightnum` string, 
  `originairportid` int, 
  `originairportseqid` int, 
  `origincitymarketid` int, 
  `origin` string, 
  `origincityname` string, 
  `originstate` string, 
  `originstatefips` string, 
  `originstatename` string, 
  `originwac` int, 
  `destairportid` int, 
  `destairportseqid` int, 
  `destcitymarketid` int, 
  `dest` string, 
  `destcityname` string, 
  `deststate` string, 
  `deststatefips` string, 
  `deststatename` string, 
  `destwac` int, 
  `crsdeptime` string, 
  `deptime` string, 
  `depdelay` int, 
  `depdelayminutes` int, 
  `depdel15` int, 
  `departuredelaygroups` int, 
  `deptimeblk` string, 
  `taxiout` int, 
  `wheelsoff` string, 
  `wheelson` string, 
  `taxiin` int, 
  `crsarrtime` int, 
  `arrtime` string, 
  `arrdelay` int, 
  `arrdelayminutes` int, 
  `arrdel15` int, 
  `arrivaldelaygroups` int, 
  `arrtimeblk` string, 
  `cancelled` int, 
  `cancellationcode` string, 
  `diverted` int, 
  `crselapsedtime` int, 
  `actualelapsedtime` int, 
  `airtime` int, 
  `flights` int, 
  `distance` int, 
  `distancegroup` int, 
  `carrierdelay` int, 
  `weatherdelay` int, 
  `nasdelay` int, 
  `securitydelay` int, 
  `lateaircraftdelay` int, 
  `firstdeptime` string, 
  `totaladdgtime` int, 
  `longestaddgtime` int, 
  `divairportlandings` int, 
  `divreacheddest` int, 
  `divactualelapsedtime` int, 
  `divarrdelay` int, 
  `divdistance` int, 
  `div1airport` string, 
  `div1airportid` int, 
  `div1airportseqid` int, 
  `div1wheelson` string, 
  `div1totalgtime` int, 
  `div1longestgtime` int, 
  `div1wheelsoff` string, 
  `div1tailnum` string, 
  `div2airport` string, 
  `div2airportid` int, 
  `div2airportseqid` int, 
  `div2wheelson` string, 
  `div2totalgtime` int, 
  `div2longestgtime` int, 
  `div2wheelsoff` string, 
  `div2tailnum` string, 
  `div3airport` string, 
  `div3airportid` int, 
  `div3airportseqid` int, 
  `div3wheelson` string, 
  `div3totalgtime` int, 
  `div3longestgtime` int, 
  `div3wheelsoff` string, 
  `div3tailnum` string, 
  `div4airport` string, 
  `div4airportid` int, 
  `div4airportseqid` int, 
  `div4wheelson` string, 
  `div4totalgtime` int, 
  `div4longestgtime` int, 
  `div4wheelsoff` string, 
  `div4tailnum` string, 
  `div5airport` string, 
  `div5airportid` int, 
  `div5airportseqid` int, 
  `div5wheelson` string, 
  `div5totalgtime` int, 
  `div5longestgtime` int, 
  `div5wheelsoff` string, 
  `div5tailnum` string)
PARTITIONED BY ( 
  `year` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://athena-examples-us-east-1/flight/parquet/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='crawler-flights', 
  'averageRecordSize'='19', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='60', 
  'recordCount'='167497743', 
  'sizeKey'='4463574900', 
  'typeOfData'='file')

SELECT origin, dest, count(*) as delays
FROM vuelos_parquet
WHERE arrdelayminutes > 60
GROUP BY origin, dest
ORDER BY 3 DESC
LIMIT 10;

SELECT origin, dest, carrier, count(*) as delays
FROM vuelos_parquet
WHERE arrdelayminutes > 60
GROUP BY origin, dest, carrier
ORDER BY 4 DESC
LIMIT 10;
