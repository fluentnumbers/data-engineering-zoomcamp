CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_dtc-de-333333/data/fhv/fhv_tripdata_2019-*.parquet']
);

SELECT count(*) FROM `trips_data_all.fhv_tripdata`;


SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `trips_data_all.fhv_tripdata`;


CREATE OR REPLACE TABLE `trips_data_all.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `trips_data_all.fhv_tripdata`;


SELECT count(*) FROM `trips_data_all.fhv_tripdata`
WHERE PUlocationID is null AND DOlocationID is null


CREATE OR REPLACE TABLE `trips_data_all.fhv_partitioned_tripdata`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS (
  SELECT * FROM `trips_data_all.fhv_tripdata`
);

SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM  `trips_data_all.fhv_partitioned_tripdata`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
  -- AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');


SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `trips_data_all.fhv_nonpartitioned_tripdata`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
  -- AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');