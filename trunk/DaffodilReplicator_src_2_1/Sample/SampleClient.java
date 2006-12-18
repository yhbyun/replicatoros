import com.daffodilwoods.replication.ReplicationServer;
import com.daffodilwoods.replication._ReplicationServer;
import com.daffodilwoods.replication._Subscription;
import java.rmi.RemoteException;
import java.sql.Timestamp;
import java.io.BufferedReader;
import java.io.*;

public class SampleClient {

  // The following constants specifies driver name , user-name and password
  // for Daffodil DB as Database Server on Subscriber(Server side).
 // manav = System Name
  private static final String DAFFODILDB_DRIVER = "in.co.daffodil.db.rmi.RmiDaffodilDBDriver",
                              DAFFODILDB_URL = "jdbc:daffodilDB://manav:3456/clientDB;create=true",
                              DAFFODILDB_USER = "daffodil",
                              DAFFODILDB_PASSWORD = "daffodil";

  // Constants to define the Subscription name
  private static final String SUB_NAME   = "Sub01";
  // Constant to define the port number of Replication Server (Client side)
  private static final int PORT_NUMBER   = 3002;
  // Constant to define the System name or IP address of the system
  // on which replication server is running
  private static final String SYS_NAME   = "manav";

  // Constant to define the Publication name to which the subscription will be subscribed
  private static final String PUB_NAME   = "Pub01";
  // Constant to define the system name on which Publication resides
  private static final String REMOTER_SERVER_NAME   = "manav";
  // Constant to define the port nuber on which publisher side replication server is running
  private static final int REMOTER_PORT_NO   = 3001;

  private static void start() {
    try {
      // Get an instance of the ReplicationServer by providing the Url, driver,
      // user name and password to connect to the Subscriber database 'clientDB'.
      _ReplicationServer rsClient = ReplicationServer.getInstance(PORT_NUMBER,SYS_NAME);
      System.out.println("rsClient:"+rsClient);
      rsClient.setDataSource(DAFFODILDB_DRIVER, DAFFODILDB_URL,
                                       DAFFODILDB_USER, DAFFODILDB_PASSWORD);
      // Gets an instance of _Subscription if subscription named SUB_NAME exists,
      // returns null otherwise
      _Subscription sub = rsClient.getSubscription(SUB_NAME);
      if(sub == null) {
        // Creates the Subscription named SUB_NAME which requires the Publication
        // name as a second argument
        sub = rsClient.createSubscription(SUB_NAME,PUB_NAME);
        // sets the remote server name
        sub.setRemoteServerUrl(REMOTER_SERVER_NAME);
        // sets the remote replication server port number
        sub.setRemoteServerPortNo(REMOTER_PORT_NO);
        // registers the newly created subscription with the replication server
        sub.subscribe();
       }
       else {
          // sets the remote server name
        sub.setRemoteServerUrl(REMOTER_SERVER_NAME);
        // sets the remote replication server port number
        sub.setRemoteServerPortNo(REMOTER_PORT_NO);
        // registers the newly created subscription with the replication server
          }

      // For information related to snapshot process refer to user_guide manual sent along with this file
      sub.getSnapShot();

      Database database = new Database(DAFFODILDB_DRIVER, DAFFODILDB_URL,
                                       DAFFODILDB_USER, DAFFODILDB_PASSWORD);

      // display data at client
      System.out.println(" Data after Snapshot operation at client... ");
      System.out.println("[ Country Table ]");
      database.displayRows("Country");
      System.out.println("[ State Table ]");
      database.displayRows("State");


      // inserting records in Country and state on Client side
      database.InsertRecordInCountry(5);
      database.InsertRecordInCountry(6);
      database.InsertRecordInCountry(7);
      database.InsertRecordsInState(5);
      database.InsertRecordsInState(6);
      database.InsertRecordsInState(7);

      // display data at client
      System.out.println(" Data after insertion at client... ");
      System.out.println("[ Country Table ]");
      database.displayRows("Country");
      System.out.println("[ State Table ]");
      database.displayRows("State");

      // For information related to synchronize process refer to user_guide manual sent along with this file
      sub.synchronize();

      // display data at client
      System.out.println(" Data after synchronize at client... ");
      System.out.println("[ Country Table ]");
      database.displayRows("Country");
      System.out.println("[ State Table ]");
      database.displayRows("State");


      BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
      System.out.println("\n Press [Enter] for Pull data ");
      input.readLine();

      sub.pull();

      // display data after Pull been done
      System.out.println(" Data after Pull at Client... ");
      System.out.println("[ Country Table ]");
      database.displayRows("Country");
      System.out.println("[ State Table ]");
      database.displayRows("State");



      // inserting records in Country and state on Client side
      database.InsertRecordInCountry(11);
      database.InsertRecordInCountry(12);
      database.InsertRecordInCountry(13);
      database.InsertRecordsInState(11);
      database.InsertRecordsInState(12);
      database.InsertRecordsInState(13);

      // display data at client
      System.out.println(" Data after insertion at client... ");
      System.out.println("[ Country Table ]");
      database.displayRows("Country");
      System.out.println("[ State Table ]");
      database.displayRows("State");


      //adding schedule which repeats itself after 1 hour,continuous schedule i.e realtime schedule can also be added
      //sub.addSchedule("sch1", "sub1", "realtime", SYS_NAME, "3001", "", "push", null,0);
      Timestamp tm1= new Timestamp(System.currentTimeMillis());
      Timestamp tm = new Timestamp(tm1.getYear(),tm1.getMonth(),tm1.getDate(),tm1.getHours(),tm1.getMinutes()+2,0,0);

      sub.addSchedule("sch1", SUB_NAME, "nonrealtime", SYS_NAME, "3001", "hour",
                      "Push", tm, 1);
      System.out.println("\n Non RealTime Schedule has been added and started for Push ");
      System.out.println(" Please Wait for atleast 2 minutes............... ");


      Thread.currentThread().sleep(218000);

      // display data at client
      System.out.println(" Data after Push at client...(No Change on Client Side) ");
      System.out.println("[ Country Table ]");
      database.displayRows("Country");
      System.out.println("[ State Table ]");
      database.displayRows("State");

      //edit the schedule in case publisher's url or port no is changed
//      sub.editSchedule("sch1", SUB_NAME, SYS_NAME, "3001");

      //To remove the schedule created
      sub.removeSchedule("sch1",SUB_NAME);

      System.exit(00);
    }
    catch (Exception ex) {
      System.out.println("Caught exception: " + ex);
      ex.printStackTrace();
    }
  }

  public static void main(String[] args) {
    (new SampleClient()).start();
  }

}
