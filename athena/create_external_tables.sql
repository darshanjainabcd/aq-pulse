create database if not exists aq_pulse;
create external table if not exists aq_pulse.air_quality_silver (
  city_id string, city string, country string, latitude double, longitude double, timezone string, event_timestamp timestamp, pm10 double, pm2_5 double, carbon_monoxide double, nitrogen_dioxide double, sulphur_dioxide double, ozone double, european_aqi double, record_key string, ingest_timestamp timestamp ) partitioned by (event_date date) stored as parquet location 's3://CHANGE_ME/silver/air_quality/';
