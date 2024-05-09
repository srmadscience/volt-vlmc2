
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

public class Demo {
    
    static final int BANK_COUNT = 1000;

    public static void main(String[] args) {
        try {
            Client c = connectVoltDB("localhost");
            Random r = new Random();
            
            boolean manyBanks = true;
            
            if (manyBanks) {
                final String[] bankNames = 
                    {"UNION BANK","HSBC","RBS"};
                
                for (int b=0; b < bankNames.length; b++) {
                    for (int i=0; i < BANK_COUNT; i++) {
                        c.callProcedure("BANKS.upsert", bankNames[b] + i);
                    }
                }
                
            }

            int transactions = 100000000;

            for (int i = 0; i < transactions; i++) {

                int amount = (int) (r.nextGaussian() * 1000);

                if (amount < 0) {
                    amount = amount * -1;
                }
                ComplainOnErrorCallback cb = new ComplainOnErrorCallback();

                long transaction_id = i;
                String sender_id = getBankId(r,"X",manyBanks) ;
                String receiver_id = getBankId(r,sender_id,manyBanks);
                String currency = "GBP";
                TimestampType datetime_sent = new TimestampType(new Date(System.currentTimeMillis()));
                String cycle_date = "20180529";
                String cycle_number = "001";
                String fields_bag = "jfjdfjdfjdsfjsfjs";

                c.callProcedure(cb, "LoadTran", "T" + transaction_id, sender_id, receiver_id, currency, amount,
                        datetime_sent, cycle_date, cycle_number, fields_bag.getBytes(), 60, 180);
                
                
                
               if (transaction_id % 1000000 == 999999) {
                   c.drain();
                   final long startBEMs = System.currentTimeMillis();
                   ClientResponse cr = c.callProcedure("BankExposure2", cycle_date, cycle_number);
                   msg(transaction_id + " transactions: BankExposure Took " + (System.currentTimeMillis() - startBEMs) + "ms "
                           + " and returned " + cr.getResults()[0].getRowCount() + " rows");
                   //msg(cr.getResults()[0].toFormattedString());
               }
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private static String getBankId(Random r, String firstBank, boolean manyBanks) {

        boolean notDone = true;
        String bank = "";

        while (notDone) {

            int amount = (int) (r.nextGaussian() * 10);

            if (amount < 0) {
                amount = amount * -1;
            }

            if (amount < 3) {
                bank = "RBS";
            } else  if (amount < 4) {
                bank = "UNION BANK";
            } else {
                bank = "HSBC";               
            }

 
            if (!bank.equals(firstBank)) {
                notDone = false;
            }

        }
        
        if (manyBanks) {
            bank = bank + r.nextInt(BANK_COUNT);
        }

        return bank;
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
