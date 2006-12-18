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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

package com.daffodilwoods.repconsole;

import java.io.*;
import java.sql.*;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.tree.*;
import com.daffodilwoods.replication.*;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class MainFrame
    extends JFrame {
  Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
  Dimension frameSize;
  JPanel contentPane;
  JMenuBar jMenuBar1 = new JMenuBar();
  JLabel statusBar = new JLabel();
  BorderLayout borderLayout1 = new BorderLayout();

  JMenu jMenuConsole = new JMenu();
  JMenuItem jMenuConsole_Exit = new JMenuItem();
  JMenuItem jMenuConsole_Refresh = new JMenuItem();
  JMenu jMenuPublisher = new JMenu();
  JMenu jMenuSubscription = new JMenu();
  JMenu jMenuHelp = new JMenu();

  JMenuItem jMenuPub_Create = new JMenuItem();
  JMenu jMenuPub_Update = new JMenu();
  JMenuItem jMenuPub_Unpublish = new JMenuItem();
  JMenuItem jMenuPub_addTable = new JMenuItem();
  JMenuItem jMenuPub_dropTable = new JMenuItem();

  JMenuItem jMenuSub_Create = new JMenuItem();
  JMenu jMenu_Snapshot = new JMenu();
  JMenuItem jMenuSub_Snapshot = new JMenuItem();
  JMenuItem jMenuSub_SnapshotAfterUpdate = new JMenuItem();
  JMenuItem jMenuSub_synchronize = new JMenuItem();
  JMenuItem jMenuSub_Pull = new JMenuItem();
  JMenuItem jMenuSub_Push = new JMenuItem();
  JMenuItem jMenuSub_Update = new JMenuItem();
  JMenuItem jMenuHelp_aboutus = new JMenuItem();

  _ReplicationServer repServer;

  JPanel jPanel1 = new JPanel();
  JTabbedPane jTabbedRepComponents = new JTabbedPane();

  TitledBorder titledBorder1;

  DefaultMutableTreeNode pubRootNode = new DefaultMutableTreeNode(
      "publications");
  DefaultMutableTreeNode subRootNode = new DefaultMutableTreeNode(
      "subscriptions");
  DefaultMutableTreeNode dataRootNode = new DefaultMutableTreeNode("datasource");

  JTree jTreePublications = new JTree(pubRootNode);
  JTree jTreeSubscriptions = new JTree(subRootNode);
  JTree jTreeDataSource = new JTree(dataRootNode);

  //POPUPS
  JMenuItem jMenuSub_UnSubscribe = new JMenuItem();
  JMenuItem jMenusynchronize = new JMenuItem();
  JMenuItem jMenupull = new JMenuItem();
  JMenuItem jMenupush = new JMenuItem();
  JMenuItem jMenuUnSubscribe = new JMenuItem();
  JMenu jMenuUpdatePub = new JMenu();
  JMenuItem jMenuUnpublish = new JMenuItem();
  JPopupMenu jpopUpMenu = new JPopupMenu();
  JMenuItem jMenuUpdateSub = new JMenuItem();
  JMenuItem jMenuUpdatePub_AddTable = new JMenuItem();
  JMenuItem jMenuUpdatePub_DropTable = new JMenuItem();
  JPopupMenu jpopUpMenu_Publication = new JPopupMenu();

  String subName, pubName;
  String selectedserver;

  //Schedule
  JMenu jMenu_AddRemoveSchedule = new JMenu();
  JMenuItem jMenuSchedule_addSchedule = new JMenuItem();
  JMenuItem jMenuSchedule_editSchedule = new JMenuItem();
  JMenuItem jMenuSchedule_removeSchedule = new JMenuItem();

  JMenu jPopUpMenu_AddRemoveSchedule = new JMenu("Schedule...");
  JMenuItem jPopUpMenuSchedule_addSchedule = new JMenuItem("Add Schedule");
  JMenuItem jPopUpMenuSchedule_editSchedule = new JMenuItem("Edit Schedule");
  JMenuItem jPopUpMenuSchedule_removeSchedule = new JMenuItem("Remove Schedule");

  JMenu jPopUpMenuSnapshot = new JMenu("SnapShot...");
  JMenuItem jMenugetSnapShot = new JMenuItem("SnapShot After Subsribing...");
  JMenuItem jMenugetSnapShotAfterUpdate = new JMenuItem(
      "SnapShot After Updating Subscription...");

  String snapShotAfterSubscribing="AfterSubscribing";
  String snapShotAfterUpdating="AfterUpdating";


  public MainFrame() throws Exception {
    jbInit(selectedserver);
    show();
  }

  public MainFrame(_ReplicationServer repServer0, String selectedserver) {
    repServer = repServer0;
    this.selectedserver = selectedserver;
    enableEvents(AWTEvent.WINDOW_EVENT_MASK);
    try {
      FileOutputStream errorLogFile = new FileOutputStream(PathHandler.
          getErrorFilePath(), true);
      Date dt = new Date(System.currentTimeMillis());
      errorLogFile.write( (dt + "\n").getBytes());
      errorLogFile.close();
    }
    catch (IOException ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
    }
    try {
      jbInit(selectedserver);
      initPubSubTree();
    }
    catch (Exception e) {
      JOptionPane.showMessageDialog(this, e, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
  }

  //Component initialization
  private void jbInit(String selectserver) throws Exception {
    Dimension screen = java.awt.Toolkit.getDefaultToolkit().getScreenSize();
    int width = (int) screen.getWidth();
    int height = (int) screen.getHeight();
    this.setBounds(0, 0, width, height);
    this.setIconImage(new ImageIcon(getClass().getResource("/icons/rep.gif")).
                      getImage());
    contentPane = (JPanel)this.getContentPane();
    titledBorder1 = new TitledBorder("");
    contentPane.setLayout(borderLayout1);
    if (selectserver.equalsIgnoreCase("Pubserver")) {
      this.setTitle("Daffodil Replicator - Publication Server");
    }
    if (selectserver.equalsIgnoreCase("Subserver")) {
      this.setTitle("Daffodil Replicator - Subscription Server");
    }
    statusBar.setText(" ");

    jMenuConsole.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuConsole.setText("Console");
    jMenuConsole_Refresh.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuConsole_Refresh.setAlignmentX( (float) 0.5);
    jMenuConsole_Refresh.setText("Refresh");
    jMenuConsole_Refresh.addActionListener(new
        MainFrame_jMenuConsole_Refresh_actionAdapter(this));
    jMenuConsole_Exit.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuConsole_Exit.setAlignmentX( (float) 0.5);
    jMenuConsole_Exit.setAction(null);
    jMenuConsole_Exit.setActionCommand("");
    jMenuConsole_Exit.setMnemonic('0');
    jMenuConsole_Exit.setText("Exit");
    jMenuConsole_Exit.setAccelerator(javax.swing.KeyStroke.getKeyStroke('X',
        java.awt.event.KeyEvent.ALT_MASK, true));
    jMenuConsole_Exit.addActionListener(new
        MainFrame_jMenuConsole_Exit_actionAdapter(this));

    jMenuPublisher.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuPublisher.setText("Publisher");
    jMenuPub_Create.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuPub_Create.setText("Create Publication...");
    jMenuPub_Create.setAccelerator(javax.swing.KeyStroke.getKeyStroke('P',
        java.awt.event.KeyEvent.CTRL_MASK, true));
    jMenuPub_Create.addActionListener(new
                                      MainFrame_jMenuPub_Create_actionAdapter(this));

    jMenuPub_Update.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuPub_Update.setText("Update Publisher...");

    jMenuPub_Unpublish.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuPub_Unpublish.setText("Drop Publication...");
    jMenuPub_Unpublish.setAccelerator(javax.swing.KeyStroke.getKeyStroke('P',
        java.awt.event.KeyEvent.ALT_MASK, false));
    jMenuPub_Unpublish.addActionListener(new
        MainFrame_jMenuPub_Unpublish_actionAdapter(this));

    jMenuPub_addTable.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuPub_addTable.setText("Add Tables...");
    jMenuPub_addTable.addActionListener(new
        MainFrame_jMenuPub_addTable_actionAdapter(this));

    jMenuPub_dropTable.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuPub_dropTable.setText("Drop Tables...");
    jMenuPub_dropTable.addActionListener(new
        MainFrame_jMenuPub_dropTable_actionAdapter(this));

    jMenuSubscription.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSubscription.setText("Subscriber");

    jMenuSub_Create.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSub_Create.setText("Create Subscription...");
    jMenuSub_Create.setAccelerator(javax.swing.KeyStroke.getKeyStroke('S',
        java.awt.event.KeyEvent.CTRL_MASK, true));
    jMenuSub_Create.addActionListener(new
                                      MainFrame_jMenuSub_Create_actionAdapter(this));

    jMenuSub_Update.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSub_Update.setText("Update Subscription...");
    jMenuSub_Update.addActionListener(new
                                      MainFrame_jMenuSub_Update_actionAdapter(this));

    jMenu_Snapshot.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenu_Snapshot.setText("SnapShot");

    jMenuSub_Snapshot.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSub_Snapshot.setText("Snapshot After Subscribing...");
    jMenuSub_Snapshot.addActionListener(new
        MainFrame_jMenuSub_Snapshot_actionAdapter(this,snapShotAfterSubscribing));

    jMenuSub_SnapshotAfterUpdate.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSub_SnapshotAfterUpdate.setText(
        "SnapShot After Updating Subscription...");
    jMenuSub_SnapshotAfterUpdate.addActionListener(new
        MainFrame_jMenuSub_Snapshot_actionAdapter(this,snapShotAfterUpdating));

    jMenuSub_synchronize.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSub_synchronize.setText("Synchronize...");
    jMenuSub_synchronize.addActionListener(new
        MainFrame_jMenuSub_synchronize_actionAdapter(this));
    jMenuSub_Pull.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSub_Pull.setText("Pull...");
    jMenuSub_Pull.addActionListener(new MainFrame_jMenuSub_pull_actionAdapter(this));
    jMenuSub_Push.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSub_Push.setText("Push...");
    jMenuSub_Push.addActionListener(new MainFrame_jMenuSub_push_actionAdapter(this));
    jMenuSub_UnSubscribe.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSub_UnSubscribe.setText("Drop Subscription...");
    jMenuSub_UnSubscribe.setAccelerator(javax.swing.KeyStroke.getKeyStroke('S',
        java.awt.event.KeyEvent.ALT_MASK, false));
    jMenuSub_UnSubscribe.addActionListener(new
        MainFrame_jMenuSub_UnSubscribe_actionAdapter(this));
    jMenuHelp.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuHelp.setText("Help");
    jMenuHelp_aboutus.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuHelp_aboutus.setText("About Replicator");
    jMenuHelp_aboutus.setAccelerator(javax.swing.KeyStroke.getKeyStroke('H',
        java.awt.event.KeyEvent.CTRL_MASK, false));
    jMenuHelp_aboutus.addActionListener(new
        MainFrame_jMenuHelp_aboutus_actionAdapter(this));

    jPanel1.setLayout(null);
    jTabbedRepComponents.setTabPlacement(JTabbedPane.LEFT);
    jTabbedRepComponents.setFont(new java.awt.Font("Dialog", 0, 12));
    jTabbedRepComponents.setBorder(BorderFactory.createEtchedBorder());
    jTabbedRepComponents.setDebugGraphicsOptions(0);
    jTabbedRepComponents.setBounds(new Rectangle(5, 3, width - 25, height - 100));
    contentPane.setMinimumSize(new Dimension(100, 400));
    contentPane.setPreferredSize(new Dimension(200, 600));
    jTreeSubscriptions.addMouseListener(new
        MainFrame_jTreeSubscriptions_mouseAdapter(this));
    jTreePublications.addMouseListener(new
                                       MainFrame_jTreePublication_mouseAdapter(this));
    jMenuBar1.setFont(new java.awt.Font("Dialog", 0, 12));
    jpopUpMenu.setFont(new java.awt.Font("Dialog", 0, 12));
    jpopUpMenu_Publication.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuUnpublish.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuUnSubscribe.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenusynchronize.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenupull.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenupush.setFont(new java.awt.Font("Dialog", 0, 12));
    this.setJMenuBar(jMenuBar1);
    contentPane.add(statusBar, BorderLayout.SOUTH);
    contentPane.add(jPanel1, BorderLayout.CENTER);

//      jMenuConsole_Exit.setAccelerator(KeyStroke.getKeyStroke());
    JScrollPane jsp = new JScrollPane(jTreePublications);
    JScrollPane jsp1 = new JScrollPane(jTreeSubscriptions);
    JScrollPane jsp2 = new JScrollPane(jTreeDataSource);
//      jsp.setBounds(new Rectangle(5, 3, 740, 546));
    jPanel1.add(jTabbedRepComponents, null);

    jTreeSubscriptions.setRootVisible(true);
    jTreePublications.setRootVisible(true);

    if (selectserver.equalsIgnoreCase("Subserver")) {
      jTabbedRepComponents.add(jsp1, "Subscriptions");
    }
    if (selectserver.equalsIgnoreCase("Pubserver")) {
      jTabbedRepComponents.add(jsp, "Publications");
    }
    jTabbedRepComponents.add(jsp2, "DataSource");

    jMenuBar1.add(jMenuConsole);
    if (selectserver.equalsIgnoreCase("Pubserver")) {
      jMenuBar1.add(jMenuPublisher);
    }
    if (selectserver.equalsIgnoreCase("Subserver")) {
      jMenuBar1.add(jMenuSubscription);
    }
    jMenuBar1.add(jMenuHelp);
    jMenuConsole.add(jMenuConsole_Refresh);
    jMenuConsole.add(jMenuConsole_Exit);

    jMenuPublisher.add(jMenuPub_Create);
    jMenuPublisher.add(jMenuPub_Update);
    jMenuPublisher.add(jMenuPub_Unpublish);

    jMenuSubscription.add(jMenuSub_Create);
    jMenuSubscription.add(jMenuSub_Update);
    jMenuSubscription.add(jMenu_Snapshot);
    jMenu_Snapshot.add(jMenuSub_Snapshot);
    jMenu_Snapshot.add(jMenuSub_SnapshotAfterUpdate);
    jMenuSubscription.add(jMenuSub_synchronize);
    jMenuSubscription.add(jMenuSub_Pull);
    jMenuSubscription.add(jMenuSub_Push);
    jMenuSubscription.add(jMenuSub_UnSubscribe);

    jMenuHelp.add(jMenuHelp_aboutus);
    //Update Publisher
    jMenuPub_Update.add(jMenuPub_addTable);
    jMenuPub_Update.add(jMenuPub_dropTable);
    //schedule
    jMenu_AddRemoveSchedule.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenu_AddRemoveSchedule.setText("Schedule...");

    jMenuSchedule_addSchedule.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSchedule_addSchedule.setText("Add Schedule...");
    jMenuSchedule_addSchedule.setAccelerator(javax.swing.KeyStroke.getKeyStroke(
        'T', java.awt.event.KeyEvent.CTRL_MASK, true));
    jMenuSchedule_addSchedule.addActionListener(new
        MainFrame_jMenuSchedule_add_actionAdapter(this));

    jMenuSchedule_editSchedule.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSchedule_editSchedule.setText("Edit Schedule...");
    jMenuSchedule_editSchedule.addActionListener(new
        MainFrame_jMenuSchedule_edit_actionAdapter(this));

    jMenuSchedule_removeSchedule.setFont(new java.awt.Font("Dialog", 0, 12));
    jMenuSchedule_removeSchedule.setText("Remove Schedule...");
    jMenuSchedule_removeSchedule.setAccelerator(javax.swing.KeyStroke.
                                                getKeyStroke('U',
        java.awt.event.KeyEvent.CTRL_MASK, true));
    jMenuSchedule_removeSchedule.addActionListener(new
        MainFrame_jMenuSchedule_remove_actionAdapter(this));

    jMenu_AddRemoveSchedule.add(jMenuSchedule_addSchedule);
    jMenu_AddRemoveSchedule.add(jMenuSchedule_editSchedule);
    jMenu_AddRemoveSchedule.add(jMenuSchedule_removeSchedule);

    jMenuSubscription.add(jMenu_AddRemoveSchedule);

    jMenugetSnapShot.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSub_Snapshot_actionPerformed(e, subName,snapShotAfterSubscribing);
      }
    });

    jMenugetSnapShotAfterUpdate.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSub_Snapshot_actionPerformed(e, subName,snapShotAfterUpdating);
      }
    });

    jMenuUpdatePub_AddTable.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuPub_Update_actionPerformed(e, pubName,
                                        RepConstants.addTable_Publication);
      }
    });

    jMenuUpdatePub_DropTable.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuPub_Update_actionPerformed(e, pubName,
                                        RepConstants.dropTable_Publication);
      }
    });
    jPopUpMenuSchedule_addSchedule.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSchedule_add_actionPerformed(e, subName);
      }
    });

    jPopUpMenuSchedule_editSchedule.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSchedule_edit_actionPerformed(e, subName);
      }
    });

    jPopUpMenuSchedule_removeSchedule.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSchedule_remove_actionPerformed(e, subName);
      }
    });
  }

  //File | Exit action performed
  public void jMenuConsoleExit_actionPerformed(ActionEvent e) {
    System.exit(0);
  }

  void jMenuConsole_Exit_actionPerformed(ActionEvent e) {
    System.exit(00);
  }

  void jMenuConsole_Refresh_actionPerformed(ActionEvent e) {
    if (selectedserver.equalsIgnoreCase("Pubserver")) {
      pubRootNode.removeAllChildren();
      jpopUpMenu_Publication.removeAll();
      ( (DefaultTreeModel) jTreePublications.getModel()).reload();
    }
    if (selectedserver.equalsIgnoreCase("subserver")) {
      subRootNode.removeAllChildren();
      jpopUpMenu.removeAll();
      ( (DefaultTreeModel) jTreeSubscriptions.getModel()).reload();
    }
    dataRootNode.removeAllChildren();
    ( (DefaultTreeModel) jTreeDataSource.getModel()).reload();
    initPubSubTree();
  }

  // Creating Publication
  void jMenuPub_Create_actionPerformed(ActionEvent e) {
    CreatePublication cp = new CreatePublication(repServer, pubRootNode,
                                                 ( (DefaultTreeModel)
                                                  jTreePublications.getModel()));
    frameSize = new Dimension(390, 250);
    cp.setBounds( (screenSize.width - frameSize.width) / 2,
                 (screenSize.height - frameSize.height) / 2, 390, 250);
    cp.show();
  }

  // Creating Subscription
  void jMenuSub_Create_actionPerformed(ActionEvent e) {
    CreateSubscription cs = new CreateSubscription(repServer, subRootNode,
        ( (DefaultTreeModel) jTreeSubscriptions.getModel()));
    frameSize = new Dimension(400, 300);
    cs.setBounds( (screenSize.width - frameSize.width) / 2,
                 (screenSize.height - frameSize.height) / 2, 400, 300);
    cs.show();
  }

  void jMenuSub_Update_actionPerformed(ActionEvent e, String subName0) {
    UpdateSubscription cp;
    if (subName0 == null) {
      cp = new UpdateSubscription(repServer, false);
    }
    else {
      cp = new UpdateSubscription(repServer, subName0, false);

    }
    frameSize = new Dimension(400, 300);
    cp.setBounds( (screenSize.width - frameSize.width) / 2,
                 (screenSize.height - frameSize.height) / 2, 400, 300);
    cp.show();
  }

  void jMenuSub_Snapshot_actionPerformed(ActionEvent e, String subName0,String snapShotType) {
    GetSnapshot cp;
    if (subName0 == null) {
      cp = new GetSnapshot(repServer, false,snapShotType);
    }
    else {
      cp = new GetSnapshot(repServer, subName0, false,snapShotType);

    }
    frameSize = new Dimension(400, 300);
    cp.setBounds( (screenSize.width - frameSize.width) / 2,
                 (screenSize.height - frameSize.height) / 2, 400, 300);
    cp.show();
  }

  void jMenuSub_synchronize_actionPerformed(ActionEvent e, String subName0) {
    Synchronize cp;
    if (subName0 == null) {
      cp = new Synchronize(repServer, false);
    }
    else {
      cp = new Synchronize(repServer, subName0, false);

    }
    frameSize = new Dimension(375, 275);
    cp.setBounds( (screenSize.width - frameSize.width) / 2,
                 (screenSize.height - frameSize.height) / 2, 375, 275);
//      System.out.println(" HELLO BEFORE SHOWING SYNCHRONIZE ");
    cp.show();
  }

  //Pull
  void jMenuSub_pull_actionPerformed(ActionEvent e, String subName0) {
    Pull cp;
    if (subName0 == null) {
      cp = new Pull(repServer, false);
    }
    else {
      cp = new Pull(repServer, subName0, false);

    }
    frameSize = new Dimension(375, 275);
    cp.setBounds( (screenSize.width - frameSize.width) / 2,
                 (screenSize.height - frameSize.height) / 2, 375, 275);
//      System.out.println(" HELLO BEFORE SHOWING PULL ");
    cp.show();
  }

  //push
  void jMenuSub_push_actionPerformed(ActionEvent e, String subName0) {
    Push cp;
    if (subName0 == null) {
      cp = new Push(repServer, false);
    }
    else {
      cp = new Push(repServer, subName0, false);

    }
    frameSize = new Dimension(375, 275);
    cp.setBounds( (screenSize.width - frameSize.width) / 2,
                 (screenSize.height - frameSize.height) / 2, 375, 275);
//      System.out.println(" HELLO BEFORE SHOWING PUSH ");
    cp.show();
  }

  // Updating Publication
  void jMenuPub_Update_actionPerformed(ActionEvent e, String pubName0,
                                       String operationType) {
    pubName = pubName0;
    UpdatePublication cp;
    if (pubName == null) {
      cp = new UpdatePublication(repServer, pubRootNode,
                                 ( (DefaultTreeModel) jTreePublications.
                                  getModel()), operationType);
      frameSize = new Dimension(390, 250);
      cp.setBounds( (screenSize.width - frameSize.width) / 2,
                   (screenSize.height - frameSize.height) / 2, 390, 250);
      cp.show();
    }
    else {
      SelectTable st = new SelectTable( ( (DefaultTreeModel) jTreePublications.
                                         getModel()),
                                       pubRootNode, pubName,
                                       repServer, null, null,
                                       operationType);
      Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
      Dimension frameSize = new Dimension(440, 325);
      st.setBounds( (screenSize.width - frameSize.width) / 2,
                   (screenSize.height - frameSize.height) / 2, 535, 325);
      st.show();
    }

  }

  // init Pub tree node and sub tree node
  void initPubSubTree() {
    //For pub Node
    ResultSet pubRS, subRS;
    PreparedStatement pstRepTable;
    Connection connection;
    connection = repServer.getDefaultConnection();
    if (selectedserver.equalsIgnoreCase("Pubserver")) {
      initPubTreeStatements(connection);
    }
    else {
      initSubTreeStatements(connection);
    }
    initDataTree();
  }

  private void initDataTree() {
    DefaultMutableTreeNode dataRootLitetral = new DefaultMutableTreeNode(
        "DEFAULT");
    dataRootNode.add(dataRootLitetral);
    DefaultMutableTreeNode driver = new DefaultMutableTreeNode( ( (
        ReplicationServer) repServer).driver);
    DefaultMutableTreeNode URL = new DefaultMutableTreeNode( ( (
        ReplicationServer) repServer).URL);
    dataRootLitetral.add(driver);
    dataRootLitetral.add(URL);
  }

  private void initSubTreeStatements(Connection connection) {
    initSubPopUpMenu();
    ResultSet subRS = null;
    Statement stmt = null;
    try {
      StringBuffer query = new StringBuffer();
      query.append("Select ").append(RepConstants.subscription_subName1)
          .append(" , ").append(RepConstants.subscription_pubName2)
          .append(" from ").append(RepConstants.subscription_TableName);
      stmt = connection.createStatement();
      subRS = stmt.executeQuery(query.toString());
      initSubTree(subRS);
    }
    catch (SQLException ex1) {

      try {
        FileOutputStream errorLogFile = new FileOutputStream(PathHandler.
            getErrorFilePath(), true);
        errorLogFile.write("\n\n\n\n".getBytes());
        errorLogFile.write(ex1.getMessage().getBytes());
        errorLogFile.close();
      }
      catch (IOException ex2) {
        JOptionPane.showMessageDialog(this, ex2, "Error Message",
                                      JOptionPane.ERROR_MESSAGE);
        return;
      }

//         JOptionPane.showMessageDialog(this, ex1, "Error Message", JOptionPane.ERROR_MESSAGE);
//         return;
    }
    finally {
      try {
        if (subRS != null) {
          subRS.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex) {
      }

    }
  }

  private void initPubPopUpMenu() {
    jMenuUpdatePub.setText("Update Publisher...");
    jpopUpMenu_Publication.add(jMenuUpdatePub);
    jMenuUpdatePub_AddTable.setText("Add Tables...");
    jMenuUpdatePub.add(jMenuUpdatePub_AddTable);
    jMenuUpdatePub_DropTable.setText("Drop Tables...");
    jMenuUpdatePub.add(jMenuUpdatePub_DropTable);
    jMenuUnpublish = jpopUpMenu_Publication.add("Drop Publication...");

    jMenuUnpublish.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
//System.out.println("calling actionperformed");
        jMenuPub_Unpublish_actionPerformed(e, pubName);
      }
    });

  }

  private void initSubPopUpMenu() {
    jMenuUpdateSub = jpopUpMenu.add("Update Subscription...");
    jMenusynchronize = jpopUpMenu.add("Synchronize...");
    jMenupull = jpopUpMenu.add("Pull...");
    jMenupush = jpopUpMenu.add("Push...");
    jMenuUnSubscribe = jpopUpMenu.add("Drop Subscription...");

    jpopUpMenu.add(jPopUpMenuSnapshot);
    jPopUpMenuSnapshot.add(jMenugetSnapShot);
    jPopUpMenuSnapshot.add(jMenugetSnapShotAfterUpdate);

    jPopUpMenu_AddRemoveSchedule.add(jPopUpMenuSchedule_addSchedule);
    jPopUpMenu_AddRemoveSchedule.add(jPopUpMenuSchedule_editSchedule);
    jPopUpMenu_AddRemoveSchedule.add(jPopUpMenuSchedule_removeSchedule);
    jpopUpMenu.add(jPopUpMenu_AddRemoveSchedule);

    jMenuUnSubscribe.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSub_UnSubscribe_actionPerformed(e, subName);
      }
    });
    jMenuUpdateSub.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSub_Update_actionPerformed(e, subName);
      }
    });

    jMenusynchronize.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSub_synchronize_actionPerformed(e, subName);
      }
    });
    jMenupull.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSub_pull_actionPerformed(e, subName);
      }
    });
    jMenupush.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        jMenuSub_push_actionPerformed(e, subName);
      }
    });

  }

  private void initPubTreeStatements(Connection connection) {
    initPubPopUpMenu();
    ResultSet pubRS = null;
    Statement stmt = null;
    PreparedStatement pstRepTable;
    StringBuffer query = new StringBuffer();
    try {
      query.append("Select ").append(RepConstants.publication_pubName1)
          .append(" from ").append(RepConstants.publication_TableName);

//         System.out.println(">>>>>>>>>>>>"+query);
      stmt = connection.createStatement();
      pubRS = stmt.executeQuery(query.toString());
//         pubRS = connection.createStatement().executeQuery(query.toString());

      query = new StringBuffer();
      query.append(" Select  ").append(RepConstants.repTable_tableName2)
          .append(" from ").append(RepConstants.rep_TableName)
          .append(" where  ").append(RepConstants.repTable_pubsubName1)
          .append(" =? ").append(" order by ").append(RepConstants.
          repTable_tableId2);
//         System.out.println(">>>>>>>>>>>>"+query);
      pstRepTable = connection.prepareStatement(query.toString());
      initPubTree(pubRS, pstRepTable, connection);
    }
    catch (SQLException ex1) {
      try {
        FileOutputStream errorLogFile = new FileOutputStream(PathHandler.
            getErrorFilePath(), true);
        errorLogFile.write("\n\n\n\n".getBytes());
        errorLogFile.write(ex1.getMessage().getBytes());
        errorLogFile.close();
      }
      catch (IOException ex2) {
        JOptionPane.showMessageDialog(this, ex2, "Error Message",
                                      JOptionPane.ERROR_MESSAGE);
        return;
      }
    }
    finally {
      try {
        if (pubRS != null)
          pubRS.close();
        if (stmt != null)
          stmt.close();
      }
      catch (SQLException ex) {
      }
    }
  }

  private void initSubTree(ResultSet subRS) {
    try {
      while (subRS.next()) {
        String subName = subRS.getString(1);
        String pubName = subRS.getString(2);
        DefaultMutableTreeNode subNode = new DefaultMutableTreeNode(subName);
        subRootNode.add(subNode);
        DefaultMutableTreeNode pubLiteralNode = new DefaultMutableTreeNode(
            "PUBLICATION");
        subNode.add(pubLiteralNode);
        pubLiteralNode.add(new DefaultMutableTreeNode(pubName));
      }
    }
    catch (SQLException ex1) {
      try {
        FileOutputStream errorLogFile = new FileOutputStream(PathHandler.
            getErrorFilePath(), true);
        errorLogFile.write("\n\n\n\n".getBytes());
        errorLogFile.write(ex1.getMessage().getBytes());
        errorLogFile.close();
      }
      catch (IOException ex2) {
        JOptionPane.showMessageDialog(this, ex2, "Error Message",
                                      JOptionPane.ERROR_MESSAGE);
        return;
      }
      JOptionPane.showMessageDialog(this, ex1, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
//      ex.printStackTrace();
    }
  }

  private void initPubTree(ResultSet pubRS, PreparedStatement pstTable,
                           Connection connection) {
    try {
      while (pubRS.next()) {
        String pubName = pubRS.getString(1);
        DefaultMutableTreeNode pubNode = new DefaultMutableTreeNode(pubName);
        pubRootNode.add(pubNode);
        pstTable.setString(1, pubName);
        ResultSet rsTable = pstTable.executeQuery();
        DefaultMutableTreeNode tableLiteralNode = new DefaultMutableTreeNode(
            "TABLES");
        pubNode.add(tableLiteralNode);
        while (rsTable.next()) {
          String tableName = rsTable.getString(1);

          int lastIndex = tableName.lastIndexOf(".");
          String schemaName=null; //=tableName.substring(0,lastIndex);
          if (lastIndex != -1) {
             schemaName=tableName.substring(0,lastIndex);
            tableName = tableName.substring(lastIndex + 1);

          }

          DefaultMutableTreeNode tableNode = new DefaultMutableTreeNode(
              tableName);
          tableLiteralNode.add(tableNode);
          ResultSet rsColumns = connection.getMetaData().getColumns(null, schemaName,
              tableName, "%");
          DefaultMutableTreeNode columnLiteralNode = new DefaultMutableTreeNode(
              "COLUMNS");
          tableNode.add(columnLiteralNode);
          while (rsColumns.next()) {
            String columnName= rsColumns.getString("COLUMN_NAME");
            DefaultMutableTreeNode columnsNode = new DefaultMutableTreeNode(
                columnName);
            columnLiteralNode.add(columnsNode);
          }
        }
      }
    }
    catch (Exception ex1) {
      ex1.printStackTrace();
      try {
        FileOutputStream errorLogFile = new FileOutputStream(PathHandler.
            getErrorFilePath(), true);
        errorLogFile.write("\n\n\n\n".getBytes());
        errorLogFile.write(ex1.getMessage().getBytes());
        errorLogFile.close();
      }
      catch (IOException ex2) {
        JOptionPane.showMessageDialog(this, ex2, "Error Message",
                                      JOptionPane.ERROR_MESSAGE);
        return;
      }
      JOptionPane.showMessageDialog(this, ex1, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
//         ex1.printStackTrace();
    }
  }

  void jMenuPub_Unpublish_actionPerformed(ActionEvent e, String pubName) {
    UnPublish unPublish;
    if (pubName == null) {
      unPublish = new UnPublish(repServer, pubRootNode,
                                ( (DefaultTreeModel) jTreePublications.
                                 getModel()));
      frameSize = new Dimension(325, 205);
      unPublish.setBounds( (screenSize.width - frameSize.width) / 2,
                          (screenSize.height - frameSize.height) / 2, 325, 205);
      unPublish.show();
    }
    else {
      UnPublish.dropPublication(pubName, this, repServer, pubRootNode,
                                ( (DefaultTreeModel) jTreePublications.
                                 getModel()));
    }
//         unPublish = new UnPublish(repServer, pubRootNode,
//                                   ( (DefaultTreeModel) jTreePublications.
//                                    getModel()), pubName);
//
//      frameSize = new Dimension(350, 220);
//      unPublish.setBounds( (screenSize.width - frameSize.width) / 2,
//                          (screenSize.height - frameSize.height) / 2, 350, 220);
//      unPublish.show();
  }

  void jMenuSub_UnSubscribe_actionPerformed(ActionEvent e, String subName) {
    UnSubscribe unSubscribe;
    if (subName == null) {
      unSubscribe = new UnSubscribe(repServer, subRootNode,
                                    ( (DefaultTreeModel)
                                     jTreeSubscriptions.getModel()), false);
    }
    else {
      unSubscribe = new UnSubscribe(repServer, subRootNode,
                                    ( (DefaultTreeModel)
                                     jTreeSubscriptions.getModel()), subName, false);

    }
    frameSize = new Dimension(380, 290);
    unSubscribe.setBounds( (screenSize.width - frameSize.width) / 2,
                          (screenSize.height - frameSize.height) / 2, 380, 290);
    unSubscribe.show();
  }

  void jTreePublication_mouseClicked(MouseEvent e) {
    if (e.BUTTON3 == e.getButton()) {
      TreePath treePath = jTreePublications.getClosestPathForLocation(e.getX(),
          e.getY());
      jTreePublications.setSelectionPath(treePath);
      if (jTreePublications.getSelectionCount() == 1) {
        Object[] selectedPath = treePath.getPath();
//            System.out.println("PATH >>" + Arrays.asList(selectedPath));
//            System.out.println(" HELLO I AM HERE  ");
        if (selectedPath.length == 2) {
//               System.out.println("CLASS GOT " + selectedPath[1].getClass());
          pubName = (String) ( (DefaultMutableTreeNode) selectedPath[1]).
              getUserObject();
          jpopUpMenu_Publication.show(jTreePublications, e.getX(), e.getY());
        }
      }
    }
  }

  void jTreeSubscriptions_mouseClicked(MouseEvent e) {
    if (e.BUTTON3 == e.getButton()) {
      TreePath treePath = jTreeSubscriptions.getClosestPathForLocation(e.getX(),
          e.getY());
      jTreeSubscriptions.setSelectionPath(treePath);
      if (jTreeSubscriptions.getSelectionCount() == 1) {
        Object[] selectedPath = treePath.getPath();
//            System.out.println("PATH >>" + Arrays.asList(selectedPath));
//            System.out.println(" HELLO I AM HERE  ");
        if (selectedPath.length == 2) {
//               System.out.println("CLASS GOT " + selectedPath[1].getClass());
          subName = (String) ( (DefaultMutableTreeNode) selectedPath[1]).
              getUserObject();
          jpopUpMenu.show(jTreeSubscriptions, e.getX(), e.getY());
        }
      }
    }
  }

  void jMenuHelp_aboutus_actionPerformed(ActionEvent e) {
    new AboutReplicator(this).Show();
  }

//Schedule Main methods...
  void jMenuSchedule_add_actionPerformed(ActionEvent e, String subName) {
    AddSchedule addSchedule;
    if (subName == null) {
      addSchedule = new AddSchedule(repServer, subRootNode,
                                    ( (DefaultTreeModel) jTreeSubscriptions.
                                     getModel()), false);
    }
    else {
      addSchedule = new AddSchedule(repServer, subRootNode,
                                    ( (DefaultTreeModel) jTreeSubscriptions.
                                     getModel()), subName, false);
    }
    frameSize = new Dimension(375, 515);
    addSchedule.setBounds( (screenSize.width - frameSize.width) / 2,
                          (screenSize.height - frameSize.height) / 2, 375, 515);
    addSchedule.show();
  }

  void jMenuSchedule_edit_actionPerformed(ActionEvent e, String subName) {
    EditSchedule editSchedule;
    if (subName == null) {
      editSchedule = new EditSchedule(repServer, false);
    }
    else {
      editSchedule = new EditSchedule(repServer, subName, false);
    }
    frameSize = new Dimension(430, 390);
    editSchedule.setBounds( (screenSize.width - frameSize.width) / 2,
                           (screenSize.height - frameSize.height) / 2, 430, 390);
    editSchedule.show();
  }

  void jMenuSchedule_remove_actionPerformed(ActionEvent e, String subName) {
    RemoveSchedule remSchedule;
    if (subName == null) {
      remSchedule = new RemoveSchedule(repServer, subRootNode,
                                       ( (DefaultTreeModel) jTreeSubscriptions.
                                        getModel()), false);
    }
    else {
      remSchedule = new RemoveSchedule(repServer, subRootNode,
                                       ( (DefaultTreeModel) jTreeSubscriptions.
                                        getModel()), subName, false);
    }
    frameSize = new Dimension(380, 290);
    remSchedule.setBounds( (screenSize.width - frameSize.width) / 2,
                          (screenSize.height - frameSize.height) / 2, 375, 270);
    remSchedule.show();
  }

} // main class.

class MainFrame_jMenuConsole_Exit_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuConsole_Exit_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuConsole_Exit_actionPerformed(e);
  }
}

class MainFrame_jMenuConsole_Refresh_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuConsole_Refresh_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuConsole_Refresh_actionPerformed(e);
  }
}

class MainFrame_jMenuHelpAbout_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuHelpAbout_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuConsole_Exit_actionPerformed(e);
  }
}

class MainFrame_jMenuPub_Create_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuPub_Create_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuPub_Create_actionPerformed(e);
  }
}

class MainFrame_jMenuPub_addTable_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuPub_addTable_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuPub_Update_actionPerformed(e, null,
                                            RepConstants.addTable_Publication);
  }
}

class MainFrame_jMenuPub_dropTable_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuPub_dropTable_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuPub_Update_actionPerformed(e, null,
                                            RepConstants.dropTable_Publication);
  }
}

class MainFrame_jMenuSub_Create_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuSub_Create_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSub_Create_actionPerformed(e);
  }
}

class MainFrame_jMenuSub_Update_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuSub_Update_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSub_Update_actionPerformed(e, null);
  }
}

class MainFrame_jMenuSub_Snapshot_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;
  String snapShotType;

  MainFrame_jMenuSub_Snapshot_actionAdapter(MainFrame adaptee,String snapShotType0) {
    this.adaptee = adaptee;
    snapShotType=snapShotType0;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSub_Snapshot_actionPerformed(e, null,snapShotType);
  }
}

class MainFrame_jMenuSub_synchronize_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuSub_synchronize_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSub_synchronize_actionPerformed(e, null);
  }
}

class MainFrame_jMenuSub_pull_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuSub_pull_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSub_pull_actionPerformed(e, null);
  }
}

class MainFrame_jMenuSub_push_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuSub_push_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSub_push_actionPerformed(e, null);
  }
}

class MainFrame_jMenuPub_Unpublish_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuPub_Unpublish_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuPub_Unpublish_actionPerformed(e, null);
  }
}

class MainFrame_jMenuSub_UnSubscribe_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuSub_UnSubscribe_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSub_UnSubscribe_actionPerformed(e, null);
  }
}

class MainFrame_jTreeSubscriptions_mouseAdapter
    extends java.awt.event.MouseAdapter {
  MainFrame adaptee;

  MainFrame_jTreeSubscriptions_mouseAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void mouseClicked(MouseEvent e) {
    adaptee.jTreeSubscriptions_mouseClicked(e);
  }
}

class MainFrame_jTreePublication_mouseAdapter
    extends java.awt.event.MouseAdapter {
  MainFrame adaptee;

  MainFrame_jTreePublication_mouseAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void mouseClicked(MouseEvent e) {
    adaptee.jTreePublication_mouseClicked(e);
  }
}

class MainFrame_jMenuHelp_aboutus_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuHelp_aboutus_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuHelp_aboutus_actionPerformed(e);
  }

}

//Schedule
class MainFrame_jMenuSchedule_add_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuSchedule_add_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSchedule_add_actionPerformed(e, null);
  }
}

class MainFrame_jMenuSchedule_edit_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuSchedule_edit_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSchedule_edit_actionPerformed(e, null);
  }
}

class MainFrame_jMenuSchedule_remove_actionAdapter
    implements java.awt.event.ActionListener {
  MainFrame adaptee;

  MainFrame_jMenuSchedule_remove_actionAdapter(MainFrame adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jMenuSchedule_remove_actionPerformed(e, null);
  }
}
