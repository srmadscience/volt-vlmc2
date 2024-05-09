create view multilat_view_outbound as
select sender_id,  currency, cycle_date, cycle_number
, status_code, count(*) how_many, sum(amount * -1 )  exposure
from   t_actual_transaction 
group by sender_id, currency, cycle_date, cycle_number, status_code;

create view multilat_view_inbound as
select receiver_id,  currency, cycle_date, cycle_number
, status_code, count(*) how_many, sum(amount) exposure
from   t_actual_transaction 
group by receiver_id, currency, cycle_date, cycle_number, status_code;


select o.sender_id , o.currency, o.cycle_date, o.cycle_number,  o.how_many, o.exposure + i.exposure
from multilat_view_outbound o
   , multilat_view_inbound i
where o.currency = i.currency
and   o.cycle_date = i.cycle_date
and   o.cycle_number = i.cycle_number
and   o.sender_id = i.receiver_id

