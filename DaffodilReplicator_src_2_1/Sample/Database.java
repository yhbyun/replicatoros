import java.sql.*;
import java.util.Arrays;

/**
 * Program to create and populate the database.
 */

public class Database {

  private String driver;
  private String url;
  private String user;
  private String password;
  private Connection con;

  public Database(String driver0, String url0, String user0, String password0) throws
      Exception {
    driver = driver0;
    url = url0;
    user = user0;
    password = password0;
    connect();
  }

  public void initialize() throws Exception {

    try {
      createTables();
      populateTables();
    }
    catch (Exception ex) {
    }
  }

  private void connect() throws Exception {
    // Load the database driver and get the Connection to the database
    Class.forName(driver);
    con = DriverManager.getConnection(url, user, password);
  }

  private void createTables() throws Exception {
    Statement stt = con.createStatement();
    stt.execute(
        " create table country ( cid int primary key , cname varchar(20)) ");
    stt.execute(" create table state ( sid int primary key , sname varchar(20) , cid int )");
    stt.close();
  }

  private void populateTables() throws Exception {
    Statement stt = con.createStatement();
    stt.execute(" insert into country values ( 1 , 'India')");
    stt.execute(" insert into country values ( 2 , 'USA')");
    stt.execute(" insert into country values ( 3 , 'Japan')");
    stt.execute(" insert into country values ( 4 , 'China')");
    stt.execute(" insert into state values   ( 1 , 'Haryana' , 1)");
    stt.execute(" insert into state values   ( 2 , 'Maxico'  , 2)");
    stt.execute(" insert into state values   ( 3 , 'Tokyo'   , 3)");
    stt.execute(" insert into state values   ( 4 , 'Paris'   , 4)");
    stt.close();
  }

  public void InsertRecordInCountry( int cid ) throws Exception {
    Statement stt = con.createStatement();
    stt.execute(" insert into country values ( "+cid+" , 'India')");
    stt.close();
  }

  public void InsertRecordsInState( int sid) throws Exception {
    Statement stt = con.createStatement();
    stt.execute(" insert into state values   ( "+sid+" , 'Haryana' , 1)");
    stt.close();
  }

  public void displayRows(String tableName) throws SQLException {
    ResultSet rs = con.createStatement().executeQuery("Select * from "+tableName);
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();
    Object[] displayColumn = new Object[columnCount];
    for (int i = 1; i <= columnCount; i++)
      displayColumn[i - 1] = metaData.getColumnName(i);
    System.out.println(Arrays.asList(displayColumn));
    while (rs.next()) {
      Object[] columnValues = new Object[columnCount];
      for (int i = 1; i <= columnCount; i++)
        columnValues[i - 1] = rs.getObject(i);
      System.out.println(Arrays.asList(columnValues));
    }
  }

}