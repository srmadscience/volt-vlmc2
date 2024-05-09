CREATE TABLE messages (
   transactionId varchar(100) NOT NULL UNIQUE,
   legId SMALLINT NOT NULL,
   messageId VARCHAR(100) NOT NULL,
   settlementDate VARCHAR(30),
   settlementCycle INT,
   amount INT,
   creditorId VARCHAR(100),
   debitorId VARCHAR(100),
   instructingId VARCHAR(100),
   instructedId VARCHAR(100),
   creditorAgentId VARCHAR(100),
   debtorAgentId VARCHAR(100),
   rawMessage VARCHAR(10000),
   id timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
) ;

CREATE INDEX id_index ON MESSAGES (id);

PARTITION TABLE messages ON COLUMN transactionId;

CREATE PROCEDURE select_message
PARTITION ON TABLE messages COLUMN transactionId
AS
BEGIN
   SELECT       transactionId, legId, messageId, settlementDate, settlementCycle, amount, creditorId, debitorId, instructingId, instructedId, creditorAgentId, debtorAgentId, rawMessage  FROM messages WHERE transactionId = ?;
END;

-- the jar file needs to be in the same location where sqlcmd is run when loading the schema,
-- or otherwise change this script accordingly
load classes /tmp/procedures.jar;

CREATE PROCEDURE
   PARTITION ON TABLE messages COLUMN transactionId
   FROM CLASS vlmcmessages.InsertAndDeleteMessage;

drop procedure getBalances if exists;
drop view credits if exists;
drop view debits if exists;

create view credits(creditorId, ct, credit) as
select creditorId, count(*) ct, sum(amount)
from messages
group by creditorId;

create index credits_ix1 on credits (creditorId);

create view debits(debitorId, ct, debit) as
select debitorId, count(*) ct, sum(amount)
from messages
group by debitorId;

create index debits_ix1 on debits (debitorId);

-- create view debits_time(whenithappened,debitor, ct, debit) as
-- select truncate(minute,whenithappened) whenithappened,debitor, count(*) ct, sum(amount)
-- from transactions
-- group by  truncate(minute,whenithappened), debitor;
--
-- create index debitst_ix1 on debits_time (debitor);

create procedure getBalances
  select coalesce(cr.creditorId, de.debitorId) as bank, coalesce(cr.credit, 0) - coalesce(de.debit, 0) as balance
  from credits as cr full join debits as de on cr.creditorId=de.debitorId;