
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

public class DemoReplication {

    public static void main(String[] args) {
        try {
            Client c = connectVoltDB("172.20.32.11");
            Random r = new Random();

            int votes = 10000000;

            for (int i = 0; i < votes; i++) {

   
                ClientResponse cr = c.callProcedure("@Statistics", "DRCONSUMER", 1);

                for (int z = 0; z < cr.getResults().length; z++) {
                    msg(cr.getResults()[z].toFormattedString());
                }

                while (cr.getResults()[0].advanceRow()) {
                    String repHost = cr.getResults()[0].getString("HOSTNAME");
                    long reprate1m = cr.getResults()[0].getLong("REPLICATION_RATE_1M");

                    msg("Host/Rate=" + repHost + " " + reprate1m);
                }

                while (cr.getResults()[1].advanceRow()) {
                    String repHost = cr.getResults()[1].getString("HOSTNAME");
                    long partId =  cr.getResults()[0].getLong("PARTITION_ID"); 
                    TimestampType ts = cr.getResults()[1].getTimestampAsTimestamp("LAST_APPLIED_TIMESTAMP ");
                    long lagMs = (System.currentTimeMillis() - ts.getTime());

                    msg("Host/Rate=" + repHost + " " + partId + " " + ts.toString() + " " + lagMs);
                }

                Thread.sleep(1000);
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


    private static Client connectVoltDB(String hostname) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            // msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setMaxOutstandingTxns(20000);
            config.setMaxTransactionsPerSecond(200000);
            config.setTopologyChangeAware(true);
            config.setReconnectOnConnectionLoss(true);

            client = ClientFactory.createClient(config);

            client.createConnection(hostname);

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    public static void msg(String message) {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);
    }
}
