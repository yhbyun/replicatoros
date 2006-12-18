import com.daffodilwoods.replication.ReplicationServer;
import com.daffodilwoods.replication._ReplicationServer;
import com.daffodilwoods.replication._Publication;
import java.rmi.RemoteException;
import java.io.BufferedReader;
import java.io.*;

public class SampleServer {

  // The following constants specifies driver name , user-name and password
  // for Daffodil DB as Database Server on Publication(Server side).
  // manav = System Name
  private static final String DAFFODILDB_DRIVER = "in.co.daffodil.db.rmi.RmiDaffodilDBDriver",
                              DAFFODILDB_URL = "jdbc:daffodilDB://localhost:3456/serverDB;create=true",
                              DAFFODILDB_USER = "daffodil",
                              DAFFODILDB_PASSWORD = "daffodil";

  // Constant to define the Publication name
  private static final String PUB_NAME   = "Pub01";
  // Constant to define the port number of Replication Server (Server side)
  private static final int PORT_NUMBER   = 3001;
  // Constant to define the System name or IP address of the system
  // on which replication server is running
  private static final String SYS_NAME   = "vikas";

  private static void start() {
    try {

      // Creates country and state table and populates them
      Database database = new Database(DAFFODILDB_DRIVER, DAFFODILDB_URL,
                                       DAFFODILDB_USER, DAFFODILDB_PASSWORD);
      database.initialize();

      // Get an instance of the ReplicationServer by providing the Url, driver,
      // user name and password to connect to the Publisher database named
      // 'serverDB'.
      _ReplicationServer rsServer = ReplicationServer.getInstance(PORT_NUMBER,SYS_NAME);
      rsServer.setDataSource(DAFFODILDB_DRIVER, DAFFODILDB_URL,
                                       DAFFODILDB_USER, DAFFODILDB_PASSWORD);

      // Gets an instance of _Publication if publication named PUB_NAME exists,
      // returns null otherwise.
      _Publication pub = rsServer.getPublication(PUB_NAME);
      if(pub == null) {
        // Creates the publication named PUB_NAME, and the tables to be published under
        // publication must be specified as an second argument.
        pub = rsServer.createPublication(PUB_NAME,new String[]{"COUNTRY","STATE"});
        // set the conflict resolver as publisher_wins, for more information,
        // refer to replication pdf mannual.
        pub.setConflictResolver(pub.publisher_wins); // Optional-- by default its set to publisher_wins
        // Sets filter clause on one or more tables to be published.
        pub.setFilter("COUNTRY"," CID > 0 "); // Optional
        // registers the newly created publication with the replication server.
        pub.publish();
      }

        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("\n Press [Enter] to display the data after synchronization ");
        input.readLine();
        // display data after synchronize been done
        System.out.println(" Data after synchronize at server... ");
        System.out.println("[ Country Table ]");
        database.displayRows("Country");
        System.out.println("[ State Table ]");
        database.displayRows("State");


        System.out.println("\n Press [Enter] to insert data for Pull on Server Side ");
        input.readLine();
        // inserting records in Country and state on Server side
        database.InsertRecordInCountry(8);
        database.InsertRecordInCountry(9);
        database.InsertRecordInCountry(10);
        database.InsertRecordsInState(8);
        database.InsertRecordsInState(9);
        database.InsertRecordsInState(10);


        // display data at client
        System.out.println(" Data after insertion at Serevr... ");
        System.out.println("[ Country Table ]");
        database.displayRows("Country");
        System.out.println("[ State Table ]");
        database.displayRows("State");


        System.out.println("\n Press [Enter] to display the data after Pull ");
        input.readLine();
        // display data after Pull been done
        System.out.println(" Data after Pull at server... ");
        System.out.println("[ Country Table ]");
        database.displayRows("Country");
        System.out.println("[ State Table ]");
        database.displayRows("State");



        System.out.println("\n Non RealTime Schedule has been added and started for Push by Subscriber");
        System.out.println(" Please Wait for atleast 2 minutes............... ");

        System.out.println(
            "\n Press [Enter] to display the data after Push");
        input.readLine();
        // display data after push been done
        System.out.println(" Data after Push(done when schedule is running) at server... ");
        System.out.println("[ Country Table ]");
        database.displayRows("Country");
        System.out.println("[ State Table ]");
        database.displayRows("State");


        System.exit(00);
    }
    catch (Exception ex) {
      System.out.println("Caught exception: " + ex);
      ex.printStackTrace();
    }
  }

  public static void main(String[] args) {
    (new SampleServer()).start();
  }

}
