drop procedure testview if exists;
drop view credits if exists;
drop view debits_time if exists;
drop view debits if exists;
drop table transactions if exists;

create table transactions (
  creditor varchar,
  debitor varchar,
  amount int,
  whenithappened timestamp
);



create view credits(creditor, ct, credit) as 
select creditor, count(*) ct, sum(amount) 
from transactions 
group by creditor;

create index credits_ix1 on credits (creditor);

create view debits(debitor, ct, debit) as 
select debitor, count(*) ct, sum(amount) 
from transactions 
group by debitor;

create index debits_ix1 on debits (debitor);

create view debits_time(whenithappened,debitor, ct, debit) as 
select truncate(minute,whenithappened) whenithappened,debitor, count(*) ct, sum(amount) 
from transactions 
group by  truncate(minute,whenithappened), debitor;

create index debitst_ix1 on debits_time (debitor);

insert into transactions values ('Alice','Bill', 100, NOW);
insert into transactions values ('Bill','Alice', 10, NOW);

select * from credits;
select * from debits;

create procedure testview as
select cr.creditor as bank, coalesce(cr.credit, 0) - coalesce(de.debit, 0) as balance
from credits as cr full join debits as de on cr.creditor=de.debitor;

exec testview;

