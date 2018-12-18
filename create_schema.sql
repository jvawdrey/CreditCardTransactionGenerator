
drop schema if exists credit_trans cascade;

create schema credit_trans;

drop table if exists credit_trans.transactions;

drop external table if exists credit_trans.gpfdist_transactions;

CREATE TABLE credit_trans.transactions (
   account_id integer,
   account_number text,
   card_type text,
   fraud_flag boolean,
   location_id integer,
   merchant_city text,
   merchant_city_alias text,
   merchant_lat double precision,
   merchant_long double precision,
   merchant_name text,
   merchant_state varchar(2),
   posting_date timestamp,
   rlb_location_key text,
   transaction_amount double precision,
   transaction_date timestamp,
   transaction_id integer
)WITH (appendonly=true)
distributed randomly;



create external TABLE credit_trans.gpfdist_transactions (
   account_id integer,
   account_number text,
   card_type text,
   fraud_flag boolean,
   location_id integer,
   merchant_city text,
   merchant_city_alias text,
   merchant_lat double precision,
   merchant_long double precision,
   merchant_name text,
   merchant_state varchar(3),
   posting_date text,
   rlb_location_key text,
   transaction_amount double precision,
   transaction_date text,
   transaction_id integer
)
LOCATION ('gpfdist://sp-pde-kafka.eastus2.cloudapp.azure.com:8081/*.csv')
FORMAT  'CSV' (HEADER);

--(DELIMITER AS ',' NULL AS 'null');




