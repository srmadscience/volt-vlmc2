SELECT b.bname, mvo.sender_id, mvo.exposure
FROM banks b
   , multilat_view_outbound mvo
   , multilat_view_inbound mvi
WHERE b.bname = mvo.sender_id
AND   b.bname = mvi.receiver_id;
