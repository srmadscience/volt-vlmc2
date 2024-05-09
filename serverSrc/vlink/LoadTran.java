
package vlink;

import java.util.Date;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class LoadTran extends VoltProcedure {

    // @formatter:off

    
    public static final SQLStmt checkDupTran = new SQLStmt("SELECT HOW_MANY "
            + "FROM sv_dup_count "
            + "WHERE transaction_id = ?;");

    public static final SQLStmt minActualDate = new SQLStmt("SELECT min(insert_timestamp) mindate "
            + "FROM t_actual_transaction;");

    public static final SQLStmt minViewDate = new SQLStmt("SELECT min(insert_timestamp) mindate "
            + "FROM sv_dup_count;");

    public static final SQLStmt purgeStatementActual = new SQLStmt("DELETE  "
            + "FROM t_actual_transaction "
            + "WHERE insert_timestamp BETWEEN ? AND ? "
            + "ORDER BY insert_timestamp, transaction_id "
            + "LIMIT ?;");

    public static final SQLStmt purgeStatementSView = new SQLStmt("DELETE  "
            + "FROM sv_dup_count "
            + "WHERE insert_timestamp BETWEEN ? AND ?;");

    public static final SQLStmt insertActualTran = new SQLStmt(
            "INSERT INTO t_actual_transaction (transaction_id, sender_id,receiver_id,"
            + "currency,amount,datetime_sent, cycle_date,cycle_number,status_code,tran_state ) "
            + "VALUES (?,?,?,?,?,?,?,?,?,?);");

    public static final SQLStmt insertDupTran = new SQLStmt(
            "INSERT INTO s_dup_transaction ( id, transaction_id, sender_id,receiver_id,"
            + "currency,amount,datetime_sent, cycle_date,cycle_number,status_code,fields_bag ) "
            + "VALUES (?,?,?,?,?,?,?,?,?,?,?);");

    private static final String NOT_DONE = "X";

    // @formatter:on

    /**
     * Used for debugging / testing.
     */
    boolean chatty = false;

    final byte STATE_HAPPY = 42;
    final byte STATE_DUPLICATE = 101;

    final int ROWS_PER_DELETE = 2;

      public VoltTable[] run(String transaction_id, String sender_id, String receiver_id, String currency, int amount,
            TimestampType datetime_sent, String cycle_date, String cycle_number, byte[] fields_bag,
            int maxDurationSecondsActual,  int maxDurationSecondsView) throws VoltAbortException {

        final long eventId = this.getUniqueId();

        // Make sure we have a record of this request. Send it to Kafka and update the dup view
        voltQueueSQL(insertDupTran, eventId, transaction_id, sender_id, receiver_id, currency, amount, datetime_sent,
                cycle_date, cycle_number, NOT_DONE, fields_bag);
        
        // See if we have > 1 mention of this record. We've just inserted what *should*
        // be the first one...
        voltQueueSQL(checkDupTran, transaction_id);
        
        // Get oldest transaction in this partition 
        voltQueueSQL(minActualDate);
        
        // Get oldest dup check in this partition 
        voltQueueSQL(minViewDate);
               
        long dupCount = 0;
        VoltTable[] firstResults = voltExecuteSQL();

        if (firstResults[1].advanceRow()) {
            dupCount = firstResults[1].getLong("HOW_MANY");
        }

        if (dupCount > 1) {
            // We already have one of these...
            this.setAppStatusCode(STATE_DUPLICATE);

        } else {
            // Create a transaction
            this.setAppStatusCode(STATE_HAPPY);
            voltQueueSQL(insertActualTran, transaction_id, sender_id, receiver_id, currency, amount, datetime_sent,
                    cycle_date, cycle_number, NOT_DONE, "0");

        }

        //
        // Get rid of old data
        //
        
        //
        // First - transactions
        //
        Date minDate = new TimestampType(this.getTransactionTime()).asExactJavaDate();

        if (firstResults[2].advanceRow()) {

            TimestampType ts = firstResults[2].getTimestampAsTimestamp("MINDATE");
            if (ts != null) {
                minDate = ts.asExactJavaDate();
            }

            // Calculate purge date...
            Date now = this.getTransactionTime();
            Date purgeDate = new Date(now.getTime() - (maxDurationSecondsActual * 1000));

            if (purgeDate.after(minDate)) {
                voltQueueSQL(purgeStatementActual, minDate, purgeDate, ROWS_PER_DELETE);
            }

        }

        //
        // Second - The dup prevention view, which has a seperate retention schedule
        //
        if (firstResults[3].advanceRow()) {

            TimestampType ts = firstResults[3].getTimestampAsTimestamp("MINDATE");
            if (ts != null) {
                minDate = ts.asExactJavaDate();
            }

            // Calculate purge date...
            Date now = this.getTransactionTime();
            Date purgeDate = new Date(now.getTime() - (maxDurationSecondsView * 1000));
            
            // Tweak purge date to a small range if it's more than 1 second...
            if (purgeDate.getTime() - minDate.getTime() > 1000 ) {
                purgeDate = new Date(minDate.getTime() + 1000);
            }

            if (purgeDate.after(minDate)) {
                voltQueueSQL(purgeStatementSView, minDate, purgeDate);
            }

        }

        return voltExecuteSQL(true);

    }

    
}
