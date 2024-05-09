
package vlink;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class BankExposure2 extends VoltProcedure {

    // @formatter:off

    
//    public static final SQLStmt checkExposure = new SQLStmt("select SENDER_ID ,  RECEIVER_ID , CURRENCY,"
//            + "  CYCLE_DATE,  CYCLE_NUMBER , STATUS_CODE , HOW_MANY , EXPOSURE , 0 netpos "
//            + "FROM bank_matrix_view "
//            + "WHERE cycle_date = ? "
//            + "AND cycle_number = ? "
//            + "ORDER BY sender_id, receiver_id;");
//    
    public static final SQLStmt getInbound = new SQLStmt("SELECT receiver_id, sum(exposure) exposure,  sum(how_many) how_many  "
            + " FROM multilat_view_inbound "
            + "WHERE cycle_date = ? "
            + "AND cycle_number = ? "
            + "GROUP BY receiver_id "
            + "ORDER BY receiver_id ; ");
    
    public static final SQLStmt getOutBound = new SQLStmt("SELECT sum(exposure) exposure,  sum(how_many) how_many  "
            + " FROM multilat_view_outbound "
            + "WHERE cycle_date = ? "
            + "AND cycle_number = ? "
            + " AND sender_id = ? ");
    
   // public static final SQLStmt getBanks = new SQLStmt("SELECT bname FROM banks ORDER BY bname;");

    // @formatter:on

    /**
     * Used for debugging / testing.
     */
    boolean chatty = false;

    public VoltTable[] run(String cycleDate, String cycleNumber) throws VoltAbortException {

        // voltQueueSQL(checkExposure, cycleDate, cycleNumber);
        //voltQueueSQL(getBanks);
        voltQueueSQL(getInbound, cycleDate, cycleNumber);

        VoltTable[] inbounds = voltExecuteSQL();

        VoltTable multilatExposure = new VoltTable(new VoltTable.ColumnInfo("BANK_NAME", VoltType.STRING),
                new VoltTable.ColumnInfo("INBOUND_VALUE", VoltType.BIGINT),
                new VoltTable.ColumnInfo("OUTBOUND_VALUE", VoltType.BIGINT),
                new VoltTable.ColumnInfo("INBOUND_COUNT", VoltType.BIGINT),
                new VoltTable.ColumnInfo("OUTBOUND_COUNT", VoltType.BIGINT),
                new VoltTable.ColumnInfo("NET_EXPOSURE", VoltType.BIGINT));

        while (inbounds[0].advanceRow()) {
            String bname = inbounds[0].getString("receiver_id");
            voltQueueSQL(getOutBound, cycleDate, cycleNumber, bname);

            VoltTable[] results = voltExecuteSQL();

            long inboundValue = 0;
            long outboundValue = 0;
            long inboundCount = 0;
            long outboundCount = 0;

            //if (results[0].advanceRow()) {
                inboundValue = inbounds[0].getLong("EXPOSURE");
                inboundCount = inbounds[0].getLong("HOW_MANY");
           // }

            if (results[0].advanceRow()) {
                outboundValue = results[0].getLong("EXPOSURE");
                outboundCount = results[0].getLong("HOW_MANY");
            }

            multilatExposure.addRow(bname, inboundValue, outboundValue, inboundCount,outboundCount,  ( outboundValue + inboundValue));

        }

        VoltTable[] results = new VoltTable[1];

        results[0] = multilatExposure;
        return results;

    }

    /**
     * By default VoltTable objects are 'final'. This method allows us to create
     * a writable one.
     * 
     * @param oldVoltTable
     *            VoltTable we want a writable copy of.
     * @return A writable copy of oldVoltTable
     */
    private static VoltTable getWritableCopy(VoltTable oldVoltTable) {

        if (oldVoltTable == null) {
            return null;
        }

        VoltTable copy = new VoltTable(oldVoltTable.getTableSchema());

        while (oldVoltTable.advanceRow()) {
            copy.add(oldVoltTable.cloneRow());
        }

        return copy;
    }
}
