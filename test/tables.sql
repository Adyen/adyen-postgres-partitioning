DROP TABLE IF EXISTS public.test_partition CASCADE;
CREATE TABLE public.test_partition (
    id bigint,
    vendor_id integer,
    tpep_pickup_datetime text,
    tpep_dropoff_datetime text,
    passenger_count text,
    trip_distance text,
    pickup_longitude numeric,
    pickup_latitude numeric,
    rate_code_id text,
    store_and_fwd_flag text,
    dropoff_longitude numeric,
    dropoff_latitude numeric,
    payment_type text,
    fare_amount text,
    extra text,
    mta_tax text,
    tip_amount text,
    tolls_amount text,
    improvement_surcharge text,
    total_amount text,
    pickup_location_id text,
    dropoff_location_id text,
    congestion_surcharge text,
    junk1 text,
    junk2 text,
    trip_date date
);

ALTER TABLE public.test_partition
    ADD CONSTRAINT test_partition_pkey PRIMARY KEY (id);

DROP TABLE IF EXISTS public.test_partition_date_template;
DROP TABLE IF EXISTS public.test_partition_date CASCADE;
CREATE TABLE public.test_partition_date (
    id bigint,
    vendor_id integer,
    tpep_pickup_datetime text,
    tpep_dropoff_datetime text,
    passenger_count text,
    trip_distance text,
    pickup_longitude numeric,
    pickup_latitude numeric,
    rate_code_id text,
    store_and_fwd_flag text,
    dropoff_longitude numeric,
    dropoff_latitude numeric,
    payment_type text,
    fare_amount text,
    extra text,
    mta_tax text,
    tip_amount text,
    tolls_amount text,
    improvement_surcharge text,
    total_amount text,
    pickup_location_id text,
    dropoff_location_id text,
    congestion_surcharge text,
    junk1 text,
    junk2 text,
    trip_date date
);

ALTER TABLE public.test_partition_date
    ADD CONSTRAINT test_partition_date_pkey PRIMARY KEY (id);


DROP TABLE IF EXISTS public.test_partition_datetime CASCADE;
CREATE TABLE public.test_partition_datetime (
    id bigint,
    vendor_id integer,
    tpep_pickup_datetime text,
    tpep_dropoff_datetime text,
    passenger_count text,
    trip_distance text,
    pickup_longitude numeric,
    pickup_latitude numeric,
    rate_code_id text,
    store_and_fwd_flag text,
    dropoff_longitude numeric,
    dropoff_latitude numeric,
    payment_type text,
    fare_amount text,
    extra text,
    mta_tax text,
    tip_amount text,
    tolls_amount text,
    improvement_surcharge text,
    total_amount text,
    pickup_location_id text,
    dropoff_location_id text,
    congestion_surcharge text,
    junk1 text,
    junk2 text,
    trip_date timestamp with time zone
);

ALTER TABLE public.test_partition_datetime
    ADD CONSTRAINT test_partition_datetime_pkey PRIMARY KEY (id, trip_date);

DROP TABLE IF EXISTS public.test_partition_inh CASCADE;
CREATE TABLE public.test_partition_inh (
    id bigint NOT NULL,
    vendor_id integer,
    tpep_pickup_datetime text,
    tpep_dropoff_datetime text,
    passenger_count text,
    trip_distance text,
    pickup_longitude numeric,
    pickup_latitude numeric,
    rate_code_id text,
    store_and_fwd_flag text,
    dropoff_longitude numeric,
    dropoff_latitude numeric,
    payment_type text,
    fare_amount text,
    extra text,
    mta_tax text,
    tip_amount text,
    tolls_amount text,
    improvement_surcharge text,
    total_amount text,
    pickup_location_id text,
    dropoff_location_id text,
    congestion_surcharge text,
    junk1 text,
    junk2 text,
    trip_date date
);

ALTER TABLE public.test_partition_inh
    ADD CONSTRAINT test_partition_inh_pkey PRIMARY KEY (id);
