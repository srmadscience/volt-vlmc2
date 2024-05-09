DROP TABLE t_transaction IF EXISTS;
DROP TABLE t_actual_transaction IF EXISTS;
DROP VIEW SV_DUP_COUNT IF EXISTS;
DROP STREAM t_dup_transaction IF EXISTS;


CREATE STREAM s_dup_transaction 
PARTITION ON COLUMN transaction_id 
EXPORT TO TARGET dups  (
  id                       BIGINT             NOT NULL ,
  transaction_id         VARCHAR(100)        NOT NULL,
  sender_id              VARCHAR(50)        NOT NULL,
  receiver_id            VARCHAR(50)        NOT NULL,
  currency               VARCHAR(50)        NOT NULL,
  amount                 INTEGER            NOT NULL,
  datetime_sent          TIMESTAMP          NOT NULL,
  cycle_date             VARCHAR(8)         NOT NULL,
  cycle_number           VARCHAR(3)         NOT NULL,
  status_code            VARCHAR(50)        NOT NULL,
  fields_bag             VARBINARY(16384)   NOT NULL,
  insert_timestamp       TIMESTAMP  DEFAULT CURRENT_TIMESTAMP  NOT NULL,
);


CREATE VIEW sv_dup_count AS
SELECT transaction_id, count(*) how_many, max(insert_timestamp) insert_timestamp
FROM s_dup_transaction
GROUP BY transaction_id;

create index sdc_ix1 on sv_dup_count(insert_timestamp, transaction_id);


CREATE TABLE t_actual_transaction (
  transaction_id         VARCHAR(100)        NOT NULL,
  tran_state             varchar(1)         NOT NULL,
  sender_id              VARCHAR(50)        NOT NULL,
  receiver_id            VARCHAR(50)        NOT NULL,
  currency               VARCHAR(50)        NOT NULL,
  amount                 INTEGER            NOT NULL,
  datetime_sent          TIMESTAMP          NOT NULL,
  cycle_date             VARCHAR(8)         NOT NULL,
  cycle_number           VARCHAR(3)         NOT NULL,
  status_code            VARCHAR(50)        NOT NULL,
  insert_timestamp       TIMESTAMP  DEFAULT CURRENT_TIMESTAMP  NOT NULL,
  PRIMARY KEY (transaction_id)
);

PARTITION TABLE t_actual_transaction ON COLUMN transaction_id;

create index tat_purge_idx on t_actual_transaction(insert_timestamp,transaction_id);

create table banks( bname varchar(15) not null primary key);

insert into banks (bname) values ('UNION BANK');
insert into banks (bname) values ('HSBC');
insert into banks (bname) values ('RBS');

create view bank_matrix_view as
select sender_id, receiver_id, currency, cycle_date, cycle_number
, status_code, count(*) how_many, sum(amount) exposure
from   t_actual_transaction 
group by sender_id, receiver_id, currency, cycle_date, cycle_number, status_code;

create view multilat_view_outbound as
select sender_id,  cycle_date, cycle_number,currency, status_code, count(*) how_many, sum(amount * -1 )  exposure
from   t_actual_transaction 
group by sender_id,  cycle_date, cycle_number,currency, status_code;


create view multilat_view_inbound as
select receiver_id,  cycle_date, cycle_number,currency, status_code, count(*) how_many, sum(amount) exposure
from   t_actual_transaction 
group by receiver_id,  cycle_date, cycle_number, currency, status_code;


load classes ../../vocalink_intro.jar;

CREATE PROCEDURE 
   PARTITION ON TABLE t_actual_transaction COLUMN transaction_id
   FROM CLASS vlink.LoadTran;

CREATE PROCEDURE GetTran 
      PARTITION ON TABLE t_actual_transaction COLUMN transaction_id
    AS SELECT * 
    FROM t_actual_transaction 
    WHERE transaction_id = ?;
   
CREATE PROCEDURE 
    FROM CLASS vlink.BankExposure;

CREATE PROCEDURE 
    FROM CLASS vlink.BankExposure2;

