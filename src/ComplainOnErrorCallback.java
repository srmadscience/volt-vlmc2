

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class ComplainOnErrorCallback implements ProcedureCallback {

    @Override
    public void clientCallback(ClientResponse arg0) throws Exception {
        
        if (arg0.getStatus() != ClientResponse.SUCCESS) {
            System.err.println("Error Code " + arg0.getStatusString());
        } else {
            byte b =  arg0.getAppStatus();
            
            if (b != 42) {
                System.err.println("Byte Code " + arg0.getAppStatus());
            }
        }
        
      

    }

}
