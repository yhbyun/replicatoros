/**
 * Copyright (c) 2003 Daffodil Software Ltd all rights reserved.
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 * There are special exceptions to the terms and conditions of the GPL
 * as it is applied to this software. See the GNU General Public License for more details.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this prog232ram; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

package com.daffodilwoods.repconsole;

import java.sql.*;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.tree.*;

import com.daffodilwoods.replication.*;
import java.rmi.*;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;
import java.util.ArrayList;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class SelectTable
    extends JDialog
    implements FocusListener, KeyListener {
  JPanel panel1 = new JPanel();
  JButton jButton_Select = new JButton();
  JButton jButtonDeselect = new JButton();

  DefaultListModel dlmDatabase = new DefaultListModel();
  JList jListDatabaseTables = new JList(dlmDatabase);

  DefaultListModel dlmSelect = new DefaultListModel();
  JList jListSelectedTables = new JList(dlmSelect);

  JLabel jLabel1 = new JLabel();
  String pubName, conflictResolver;
  _ReplicationServer repServer;
  _Publication pub;
  AbstractDataBaseHandler dbh;
  JButton jButton1 = new JButton();
  JButton jButton2 = new JButton();
  JButton jButton3 = new JButton();
  JButton jButton4 = new JButton();
  JDialog createPublication;
  DefaultTreeModel defaultTreeModel;
  DefaultMutableTreeNode pubRootNode;
  JLabel jLabel2 = new JLabel();
  JEditorPane help = new JEditorPane();
  JScrollPane jscrollpane_td;
  JScrollPane jscrollpane_st;
  Border border1;
  TitledBorder titledBorder1;
  TitledBorder titledBorder2;
  Border border2;
  String operationType;

  public SelectTable(Frame frame, String title, String operationType0,
                     boolean modal) {
    super(frame, title, modal);
    try {
      operationType = operationType0;
      jbInit();
      pack();
    }
    catch (Exception ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
  }

  public SelectTable(DefaultTreeModel defaultTreeModel,
                     DefaultMutableTreeNode pubRootNode, String pubName,
                     _ReplicationServer repServer, String conflictResolver,
                     JDialog createPublication, String operationType0) {

    this(StartRepServer.getMainFrame(), "Select Table", operationType0, true);
    this.pubName = pubName;
    this.repServer = repServer;
    this.conflictResolver = conflictResolver;
    this.createPublication = createPublication;
    this.defaultTreeModel = defaultTreeModel;
    this.pubRootNode = pubRootNode;
    if (operationType.equalsIgnoreCase(RepConstants.create_Publication)) {
      initListWithDatabaseTablesForCreate();
      jButton4.setEnabled(false);
    }
    else if (operationType.equalsIgnoreCase(RepConstants.addTable_Publication)) {
      initListWithDatabaseTablesForAdd();
      jButton4.setEnabled(false);
    }
    else {
      initListWithDatabaseTablesForDrop();
      jButton2.setVisible(false);
    }
  }

  private void jbInit() throws Exception {
    border1 = BorderFactory.createEmptyBorder();
    titledBorder1 = new TitledBorder("");
    titledBorder2 = new TitledBorder("");
    border2 = new EtchedBorder(EtchedBorder.RAISED, Color.white,
                               new Color(148, 145, 140));
    panel1.setLayout(null);

    jButton_Select.setBounds(new Rectangle(240, 112, 52, 22));
    jButton_Select.setEnabled(true);
    jButton_Select.setFont(new java.awt.Font("Dialog", 1, 15));
    jButton_Select.setText(">>");
    jButton_Select.addActionListener(new
                                     SelectTable_jButton_Select_actionAdapter(this));
    jButtonDeselect.setBounds(new Rectangle(240, 141, 52, 22));
    jButtonDeselect.setFont(new java.awt.Font("Dialog", 1, 15));
    jButtonDeselect.setText("<<");
    jButtonDeselect.addActionListener(new
                                      SelectTable_jButtonDeselect_actionAdapter(this));
    jListDatabaseTables.setFont(new java.awt.Font("Dialog", 0, 12));

    jListDatabaseTables.addFocusListener(this);
    jListSelectedTables.addFocusListener(this);

    jListSelectedTables.setFont(new java.awt.Font("Dialog", 0, 12));

    jLabel1.setFont(new java.awt.Font("Serif", 3, 25));
    jLabel1.setForeground(SystemColor.infoText);
    jLabel1.setText("Select Tables For Publication");
    jLabel1.setBounds(new Rectangle(100, 4, 324, 27));

    jButton1.setBounds(new Rectangle(90, 264, 81, 26));
    jButton1.setEnabled(true);
    jButton1.setFont(new java.awt.Font("Dialog", 1, 12));
    jButton1.setFocusPainted(true);
    jButton1.setText("<Back");
    if (operationType != RepConstants.create_Publication) {
      jButton1.setVisible(false);
    }

    jButton1.addActionListener(new SelectTable_jButton1_actionAdapter(this));
    jButton2.setBounds(new Rectangle(178, 264, 81, 26));
    jButton2.setEnabled(true);
    jButton2.setFont(new java.awt.Font("Dialog", 1, 12));
    jButton2.setFocusPainted(true);
    jButton2.setText("Next>");
    jButton2.addActionListener(new SelectTable_jButton2_actionAdapter(this));

    jButton3.setBounds(new Rectangle(354, 263, 81, 26));
    jButton3.setFont(new java.awt.Font("Dialog", 1, 12));
    jButton3.setActionCommand("Cancle");
    jButton3.setFocusPainted(true);
    jButton3.setText("Cancel");
    jButton3.addActionListener(new SelectTable_jButton3_actionAdapter(this));

    jButton4.setBounds(new Rectangle(266, 264, 81, 26));
    jButton4.setEnabled(true);
    jButton4.setFont(new java.awt.Font("Dialog", 1, 12));
    jButton4.setFocusPainted(true);
    jButton4.setText("Finish");
    jButton4.addActionListener(new SelectTable_jButton4_actionAdapter(this));

    jLabel2.setFont(new java.awt.Font("Dialog", 1, 11));
    jLabel2.setBorder(border2);
    jLabel2.setText("");
    jLabel2.setBounds(new Rectangle(10, 55, 510, 165));

    help.setBackground(UIManager.getColor("Button.background"));
    help.setEnabled(true);
    help.setFont(new java.awt.Font("Dialog", 2, 12));
    help.setDoubleBuffered(false);
    help.setRequestFocusEnabled(false);
    help.setDisabledTextColor(Color.black);
    help.setEditable(false);
    if (operationType.equalsIgnoreCase(RepConstants.dropTable_Publication))
      help.setText("Select tables to be dropped from published");
    else
      help.setText("Select tables to be published");

    help.setBounds(new Rectangle(9, 221, 399, 38));

    jscrollpane_td = new JScrollPane(jListDatabaseTables);
    jscrollpane_td.setFont(new java.awt.Font("Dialog", 1, 11));
    jscrollpane_td.setBounds(new Rectangle(21, 59, 210, 153));

    jscrollpane_st = new JScrollPane(jListSelectedTables);
    jscrollpane_st.setBounds(new Rectangle(301, 59, 210, 153));

    titledBorder2.setTitleFont(new java.awt.Font("Dialog", 0, 12));
    titledBorder1.setTitleFont(new java.awt.Font("Dialog", 0, 12));
    getContentPane().add(panel1);

    panel1.add(jLabel1, null);
    panel1.add(jLabel2, null);
    panel1.add(jscrollpane_td);
    panel1.add(jscrollpane_st);
    panel1.add(jButton_Select, null);
    panel1.add(jButtonDeselect, null);
    panel1.add(help, null);
    panel1.add(jButton4, null);
    panel1.add(jButton3, null);
    panel1.add(jButton1, null);
    panel1.add(jButton2, null);

//    jListDatabaseTables.grabFocus();

    jscrollpane_st.setBorder(new TitledBorder("Selected Tables"));
    jscrollpane_td.setBorder(new TitledBorder("Tables in Database"));
  }

  void initListWithDatabaseTablesForCreate() {
    ResultSet rsTableNames = null;
    try {
      jListDatabaseTables.setAutoscrolls(true);
      Connection connection = repServer.getDefaultConnection();
      rsTableNames = connection.getMetaData().getTables(null, null,
          "%", new String[] {"TABLE"});
      int i = 0;
      String databaseProductName = connection.getMetaData().
          getDatabaseProductName();

      while (rsTableNames.next()) {
        if (!((databaseProductName.equalsIgnoreCase(Utility.FireBird_ProductName)) || (databaseProductName.equalsIgnoreCase(Utility.MySQL_ProductName)))) {
          if (!rsTableNames.getString("TABLE_SCHEM").equalsIgnoreCase("SYSTEM")) {
            if (rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.publication_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.subscription_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.bookmark_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.rep_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.log_Table) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.schedule_Table) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.ignoredColumns_Table) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.trackReplicationTablesUpdation_Table) ||
                /////////Here consider Rep_Shadow ... specifically....
                rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                "rep_shadow_") ||
                rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                "r_s_")) {


              continue;
            }

            dlmDatabase.addElement(rsTableNames.getString("TABLE_SCHEM") + "." +
                                   rsTableNames.getString("TABLE_NAME"));

          }
        }
        else {
          if (rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
              RepConstants.publication_TableName) ||
              rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
              RepConstants.subscription_TableName) ||
              rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
              RepConstants.bookmark_TableName) ||
              rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
              RepConstants.rep_TableName) ||
              rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
              RepConstants.log_Table) ||
              rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
              RepConstants.schedule_Table) ||
              rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
              RepConstants.trackReplicationTablesUpdation_Table) ||
              rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
              RepConstants.ignoredColumns_Table) ||
              /////////Here consider Rep_Shadow ... specifically....
              rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
              "rep_shadow_") ||
              rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
              "r_s_")) {
            continue;
          }
          dlmDatabase.addElement(rsTableNames.getString("TABLE_NAME"));
        }
      }
    }
    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    finally {
      try {
        if (rsTableNames != null) {
          rsTableNames.close();
        }
      }
      catch (SQLException ex1) {
      }
    }
  }

  void initListWithDatabaseTablesForAdd() {
    ResultSet rsPublishedTable = null;
    ResultSet rsTableNames = null;
    Statement stt = null;
    try {
      jListDatabaseTables.setAutoscrolls(true);
      Connection connection = repServer.getDefaultConnection();
      rsTableNames = connection.getMetaData().getTables(null, null, "%",
          new String[] {"TABLE"});
      int i = 0;
      //select published tables comapre them to show in case of drop and ignoring in case of adding tables

      stt = connection.createStatement();
      pub = repServer.getPublication(pubName);
      dbh = Utility.getDatabaseHandler( ( (Publication) pub).getConnectionPool(),
                                       pubName);
      StringBuffer sb = new StringBuffer();
      sb.append("select ").append(RepConstants.repTable_tableName2).
          append(
          " from ").append(dbh.getRepTableName()).append(" where ")
          .append(RepConstants.repTable_pubsubName1).append(" = '")
          .append(pubName).append("'");
//System.out.println(sb.toString());
      rsPublishedTable = stt.executeQuery(sb.toString());
      ArrayList publishedTable = new ArrayList();
      while (rsPublishedTable.next()) {
        publishedTable.add(rsPublishedTable.getString(1).substring(
            rsPublishedTable.getString(1).indexOf(".") + 1,
            rsPublishedTable.getString(1).length()));
      }
      dbh = Utility.getDatabaseHandler( ( (Publication) pub).getConnectionPool(),
                                       pubName);
      while (rsTableNames.next()) {
        boolean flag = false;

//System.out.println("publishedTable " +publishedTable.get(j));
//System.out.println("rsTableNames.getString(TABLE_NAME) " +rsTableNames.getString("TABLE_NAME"));
System.out.println(" dbh.getvendorName() ::  "+dbh.getvendorName());
        if(!((dbh.getvendorName() == Utility.DataBase_Firebird) || (dbh.getvendorName()== Utility.DataBase_MySQL ))) {
          for (int j = 0; j < publishedTable.size(); j++) {
            if (!rsTableNames.getString("TABLE_SCHEM").equalsIgnoreCase(
                "SYSTEM")) {
              if (rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.publication_TableName) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.subscription_TableName) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.bookmark_TableName) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.rep_TableName) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.log_Table) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.schedule_Table) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.trackReplicationTablesUpdation_Table) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.ignoredColumns_Table) ||
                  /////////Here consider Rep_Shadow ... specifically....
                  rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                  "rep_shadow_") ||
                  rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                  "r_s_") ||
                  //here already published tables are ignored
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase( (
                  String)
                  publishedTable.get(j))) {
                flag = true;
                break;
              }
            }
          }
          if (!flag)
            dlmDatabase.addElement(rsTableNames.getString("TABLE_SCHEM") +
                                   "." +
                                   rsTableNames.getString("TABLE_NAME"));

        }
        else {
          for (int j = 0; j < publishedTable.size(); j++) {
            if (rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.publication_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.subscription_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.bookmark_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.rep_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.log_Table) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.schedule_Table) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.trackReplicationTablesUpdation_Table) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.ignoredColumns_Table) ||
                /////////Here consider Rep_Shadow ... specifically....
                rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                "rep_shadow_") ||
                rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                "r_s_") ||
                //here already published tables are ignored
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase( (String)
                publishedTable.get(j))) {
              flag = true;
              break;
            }
          }
          if (!flag)
            dlmDatabase.addElement(rsTableNames.getString("TABLE_NAME"));

        }

      }
    }

    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    catch (RemoteException ex) {
      RepConstants.writeERROR_FILE(ex);
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }

    finally {
      try {
        if (rsTableNames != null) {
          rsTableNames.close();
        }
        if (rsPublishedTable != null) {
          rsPublishedTable.close();
        }
        if (stt != null)
          stt.close();
      }
      catch (SQLException ex1) {
      }
    }
  }

  void initListWithDatabaseTablesForDrop() {
    ResultSet rsPublishedTable = null;
    ResultSet rsTableNames = null;
    Statement stt = null;
    try {
      jListDatabaseTables.setAutoscrolls(true);
      Connection connection = repServer.getDefaultConnection();
      rsTableNames = connection.getMetaData().getTables(null, null,
          "%", new String[] {"TABLE"});
      int i = 0;
      //select published tables comapre them to show in case of drop and ignoring in case of adding tables

      stt = connection.createStatement();
      pub = repServer.getPublication(pubName);
      dbh = Utility.getDatabaseHandler( ( (Publication) pub).getConnectionPool(),
                                       pubName);

      StringBuffer sb = new StringBuffer();
      sb.append("select ").append(RepConstants.repTable_tableName2).
          append(
          " from ").append(dbh.getRepTableName()).append(" where ")
          .append(RepConstants.repTable_pubsubName1).append(" = '")
          .append(pubName).append("'");
//System.out.println(sb.toString());
      rsPublishedTable = stt.executeQuery(sb.toString());
      ArrayList publishedTable = new ArrayList();
      ArrayList tablesAddedToList = new ArrayList();

      while (rsPublishedTable.next()) {
        publishedTable.add(rsPublishedTable.getString(1).substring(
            rsPublishedTable.getString(1).indexOf(".") + 1,
            rsPublishedTable.getString(1).length()));
      }

      while (rsTableNames.next()) {
        boolean flag = true;
        for (int j = 0; j < publishedTable.size(); j++) {
            if (!((dbh.getvendorName() == Utility.DataBase_Firebird) ||(dbh.getvendorName() == Utility.DataBase_MySQL ))) {
            if (!rsTableNames.getString("TABLE_SCHEM").equalsIgnoreCase(
                "SYSTEM")) {
//System.out.println(rsTableNames.getString("TABLE_NAME").equalsIgnoreCase((String)publishedTable.get(j)));
              if (rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.publication_TableName) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.subscription_TableName) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.bookmark_TableName) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.rep_TableName) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.log_Table) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.schedule_Table) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.ignoredColumns_Table) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.trackReplicationTablesUpdation_Table) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.trackReplicationTablesUpdation_Table) ||
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                  RepConstants.ignoredColumns_Table) ||
                  /////////Here consider Rep_Shadow ... specifically....
                  rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                  "rep_shadow_") ||
                  rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                  "r_s_") || !
                  rsTableNames.getString("TABLE_NAME").equalsIgnoreCase( (
                  String) publishedTable.get(j))) {
                flag = false;
              }
              else
                flag = true;
              if (flag) {
                tablesAddedToList.add(rsTableNames.getString("TABLE_SCHEM") +
                                      "." +
                                      rsTableNames.getString("TABLE_NAME"));
                continue;
              }

            }
          }
          else {
            if (rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.publication_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.subscription_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.bookmark_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.rep_TableName) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.log_Table) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.schedule_Table) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.trackReplicationTablesUpdation_Table) ||
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase(
                RepConstants.ignoredColumns_Table) ||

                /////////Here consider Rep_Shadow ... specifically....
                rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                "rep_shadow_") ||
                rsTableNames.getString("TABLE_NAME").toLowerCase().startsWith(
                "r_s_") || !
                rsTableNames.getString("TABLE_NAME").equalsIgnoreCase( (String)
                publishedTable.get(j))) {
              flag = false;
            }
            else
              flag = true;
            if (flag) {
              tablesAddedToList.add(rsTableNames.getString("TABLE_NAME"));
              continue;
            }

          }
        }
      }
      for (int j = 0; j < tablesAddedToList.size(); j++)
        dlmDatabase.addElement(tablesAddedToList.get(j));
    }

    catch (SQLException ex) {
      RepConstants.writeERROR_FILE(ex);
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    catch (RepException ex) {
      RepConstants.writeERROR_FILE(ex);
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    catch (RemoteException ex) {
      RepConstants.writeERROR_FILE(ex);
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }

    finally {
      try {
        if (rsTableNames != null) {
          rsTableNames.close();
        }
        if (rsPublishedTable != null) {
          rsPublishedTable.close();
        }
        if (stt != null)
          stt.close();
      }
      catch (SQLException ex1) {
      }
    }
  }

  void jButton_Select_actionPerformed(ActionEvent e) {
    Object[] tableNames = jListDatabaseTables.getSelectedValues();
    for (int i = 0; i < tableNames.length; i++) {
      String tableNameForPublishing = (String) tableNames[i];
      if (! (tableNameForPublishing == null ||
             tableNameForPublishing.equalsIgnoreCase(""))) {
        dlmDatabase.removeElement(tableNameForPublishing);
        dlmSelect.addElement(tableNameForPublishing);
      }
    }
  }

  void jButtonDeselect_actionPerformed(ActionEvent e) {
    Object[] tableNames = jListSelectedTables.getSelectedValues();
    for (int i = 0; i < tableNames.length; i++) {
      String deselectTableNameForPublishing = (String) tableNames[i];
      if (! (deselectTableNameForPublishing == null ||
             deselectTableNameForPublishing.equalsIgnoreCase(""))) {
        dlmDatabase.addElement(deselectTableNameForPublishing);
        dlmSelect.removeElement(deselectTableNameForPublishing);
      }
    }
  }

  void jButton3_actionPerformed(ActionEvent e) {
    hide();
  }

  void jButton2_actionPerformed(ActionEvent e) {
    int selectedNoOfTables = dlmSelect.getSize();
    try {
      if (selectedNoOfTables == 0) {
        throw new RepException("REP012", new Object[] {pubName});
      }
      String[] tables = new String[selectedNoOfTables];
      for (int i = 0; i < selectedNoOfTables; i++) {
        tables[i] = ( (String) dlmSelect.get(i));
      }

      if (operationType.equalsIgnoreCase(RepConstants.create_Publication)) {
        try {
          pub = repServer.createPublication(pubName, tables);
          pub.setConflictResolver(conflictResolver);
           setFilterClause(tables);
        }
        catch (RepException ex1) {
          if (ex1.getRepCode().equalsIgnoreCase("REP0205")) {

                callSelectTableToBreakCycle(tables, ex1);

          }
          else {
            throw ex1;
          }
        }
      }
      else {
        pub = repServer.getPublication(pubName);
      }
      if (operationType.equalsIgnoreCase(RepConstants.addTable_Publication)) {
        setFilterClause(tables);
      }
    }
    catch (RepException ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    catch (RemoteException ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }

  }

  void jButton1_actionPerformed(ActionEvent e) {
    hide();
    createPublication.show();
  }

  void jButton4_actionPerformed(ActionEvent e) {
    int selectedNoOfTables = dlmSelect.getSize();
    try {
      if (selectedNoOfTables == 0) {
        throw new RepException("REP012", new Object[] {pubName});
      }
      String[] tables = new String[selectedNoOfTables];
      for (int i = 0; i < selectedNoOfTables; i++) {
        tables[i] = ( (String) dlmSelect.get(i));
      }
      _Publication pub = null;
      pub = repServer.getPublication(pubName);

      if (operationType.equalsIgnoreCase(RepConstants.dropTable_Publication)) {
        pub.dropTableFromPublication(tables);
      }
      JOptionPane.showMessageDialog(this, "Publication Updated Successfully",
                                    "Success",
                                    JOptionPane.INFORMATION_MESSAGE);
      hide();
    }
    catch (RepException ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    catch (RemoteException ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
  }

  public void keyTyped(KeyEvent keyEvent) {
  }

  public void keyPressed(KeyEvent keyEvent) {
  }

  public void keyReleased(KeyEvent keyEvent) {
  }

  public void focusGained(FocusEvent fe) {
    if ( ( (JList) fe.getSource()).equals(jListDatabaseTables)) {
      if (operationType.equalsIgnoreCase(RepConstants.dropTable_Publication))
        help.setText("Select tables to be dropped from published");
      else
        help.setText("Select tables to be published");
    }
    if ( ( (JList) fe.getSource()).equals(jListSelectedTables)) {
      help.setText("Your selected tables  are in this list ");

    }

  }

  public void focusLost(FocusEvent fe) {
    if (operationType == RepConstants.dropTable_Publication) {
      jButton4.setEnabled(true);
    }
    jButton2.setEnabled(true);
    if (jListDatabaseTables.getSelectedIndices().length < 0) {
      jButton2.setEnabled(false);

    }

  }


  private void callSelectTableToBreakCycle(String[] tables,RepException exception) throws
      RepException {
    boolean cycleInTables=true;
    ArrayList selectedBreakCycleRelation=new ArrayList();
    while(cycleInTables){
    if (exception.getRepCode().equalsIgnoreCase("REP0205")) {
      int userResponse = JOptionPane.showConfirmDialog(this, exception,
          "Error Message", JOptionPane.ERROR_MESSAGE);
      //if pressed OK
      if (userResponse == 0) {
        ArrayList tableNamesInCycle = repServer.getTablesInCycle();

        SelectTableToBreakCycle selectTableToBreakCycle = new
            SelectTableToBreakCycle(defaultTreeModel, pubRootNode, pubName,
                                    repServer, conflictResolver,
                                    createPublication, tableNamesInCycle,selectedBreakCycleRelation);

        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension frameSize = new Dimension(440, 355);
        selectTableToBreakCycle.setBounds( (screenSize.width - frameSize.width) /
                                          2,
                                          (screenSize.height - frameSize.height) /
                                          2, 535, 355);
      if(this instanceof SelectTable){
        this.hide();
      }
       selectTableToBreakCycle.show();
       //get cycle break list
      ArrayList tableToBreakCycle = selectTableToBreakCycle.
            getSelectedRelationToBreakCycle();

        for (int i = 0; i < tableToBreakCycle.size(); i++) {
          String relation=(String)tableToBreakCycle.get(i);
//          System.out.println("relation:"+relation);
          if(!selectedBreakCycleRelation.contains(relation)){
//            System.out.println("adding relation:"+relation);
            selectedBreakCycleRelation.add(relation);
          }
        }
//        call createPublication
        try {
          String[] selectedBreakCycleRelation0 = new String[selectedBreakCycleRelation.size()];
          selectedBreakCycleRelation.toArray(selectedBreakCycleRelation0);
//          for (int i = 0; i < selectedBreakCycleRelation0.length; i++) {
//            System.out.println("selectedBreakCycleRelation0 ::  "+selectedBreakCycleRelation0[i]);
//          }
          pub = repServer.createPublication(pubName, tables, selectedBreakCycleRelation0);
          cycleInTables=false;
          pub.setConflictResolver(conflictResolver);
          setFilterClause(tables);
        }
        catch (RepException ex1) {
          if (!ex1.getRepCode().equalsIgnoreCase("REP0205")) {
            throw ex1;
          }
        }
      }
      else {
        return;
      }
    }
      else {
        return;
      }
    }
    }

    private void setFilterClause(String[] tables){
      SetFilterClause setFilterClause = new SetFilterClause(repServer,
           defaultTreeModel, pubRootNode, pubName, pub, tables, this,
           operationType);
       Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
       Dimension frameSize = new Dimension(430, 300);
       setFilterClause.setBounds( (screenSize.width - frameSize.width) / 2,
                                 (screenSize.height - frameSize.height) / 2,
                                 430,
                                 300);
       hide();
       setFilterClause.show();
    }
}

class SelectTable_jButton_Select_actionAdapter
    implements java.awt.event.ActionListener {
  SelectTable adaptee;

  SelectTable_jButton_Select_actionAdapter(SelectTable adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButton_Select_actionPerformed(e);
  }
}

class SelectTable_jButtonDeselect_actionAdapter
    implements java.awt.event.ActionListener {
  SelectTable adaptee;

  SelectTable_jButtonDeselect_actionAdapter(SelectTable adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonDeselect_actionPerformed(e);
  }
}

class SelectTable_jButton3_actionAdapter
    implements java.awt.event.ActionListener {
  SelectTable adaptee;

  SelectTable_jButton3_actionAdapter(SelectTable adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButton3_actionPerformed(e);
  }
}

class SelectTable_jButton2_actionAdapter
    implements java.awt.event.ActionListener {
  SelectTable adaptee;

  SelectTable_jButton2_actionAdapter(SelectTable adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButton2_actionPerformed(e);
  }
}

class SelectTable_jButton1_actionAdapter
    implements java.awt.event.ActionListener {
  SelectTable adaptee;

  SelectTable_jButton1_actionAdapter(SelectTable adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButton1_actionPerformed(e);
  }
}

class SelectTable_jButton4_actionAdapter
    implements java.awt.event.ActionListener {
  SelectTable adaptee;

  SelectTable_jButton4_actionAdapter(SelectTable adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButton4_actionPerformed(e);
  }
}
