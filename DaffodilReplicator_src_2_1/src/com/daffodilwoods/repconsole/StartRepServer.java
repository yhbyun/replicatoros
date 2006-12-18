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
import java.net.*;
import java.util.*;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;

import com.daffodilwoods.replication.*;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class StartRepServer
    extends JDialog
    implements FocusListener, KeyListener {

  JPanel panel1 = new JPanel();
  JPanel jPanel1 = new JPanel();
  JLabel jLabel8 = new JLabel();
  JLabel jLabel4 = new JLabel();
  JLabel jLabel2 = new JLabel();
  JTextField jTextPortNo = new JTextField();
  JTextField jTextSystemName = new JTextField();
  JPasswordField jPassword = new JPasswordField();
  JLabel jLabel3 = new JLabel();
  JLabel jLabel7 = new JLabel();
  JTextField jTextUsername = new JTextField();
  JLabel jLabel5 = new JLabel();
  public static JFrame mainFrame;
  DefaultComboBoxModel dComboDriver = new DefaultComboBoxModel();
  private String selectedserver = null;
  RandomAccessFile rdriverFile, rURLFile;
  ArrayList existingDriverName = new ArrayList();
  ArrayList existingURLName = new ArrayList();

  DefaultComboBoxModel dComboURL = new DefaultComboBoxModel();
  TitledBorder titledBorder1;
  JLabel jLabel6 = new JLabel();
  JLabel jLabel1 = new JLabel();
  Border border1;
  JButton jButtonStartServer = new JButton();
  JButton jButtonExit = new JButton();
  Border border2;
  Border border3;
  TitledBorder titledBorder2;
  JComboBox jComboBoxDriver = new JComboBox(dComboDriver);
  JComboBox jComboBoxURL = new JComboBox(dComboURL);
  _ReplicationServer repServer;
  JEditorPane help = new JEditorPane();

  public StartRepServer(Frame frame, String title, boolean modal) {
    super(frame, title, modal);
    try {
      init();
      jbInit();
      pack();
    }
    catch (Exception ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
  }

  /* public StartRepServer(StartServer sw)
   {
       this(null, "Daffodil Replicator", true);
       sw.Dispose();
   }*/

  public StartRepServer(StartServer sw, String selectedserver) {
    this(null, "Daffodil Replicator", true);
    this.selectedserver = selectedserver;
    sw.Dispose();
  }

  private void init() {
    try {
//      try {
//        String localHost = (InetAddress.getLocalHost()).getHostName();
//        jTextSystemName.setText(localHost);
//      }
//      catch (UnknownHostException ex2) {
//        JOptionPane.showMessageDialog(this, ex2, "Error Message",
//                                      JOptionPane.ERROR_MESSAGE);
//        return;
//      }
      String repHome = PathHandler.getRepHome();
      if (repHome == null) {
        JOptionPane.showMessageDialog(this,
                                      "Invalid path " + PathHandler.getRepHomeByUser(),
                                      "Error Message",
                                      JOptionPane.ERROR_MESSAGE);
        System.exit(01);
      }
      rdriverFile = new RandomAccessFile(repHome + File.separator + "dinfo.lg",
                                         "rw");
      rURLFile = new RandomAccessFile(repHome + File.separator + "urlinfo.lg",
                                      "rw");
      try {
        if (rdriverFile.length() <= 0) {
          dComboDriver.addElement("in.co.daffodil.db.jdbc.DaffodilDBDriver");
          dComboURL.addElement("jdbc:daffodilDB_embedded:school;create=true");
          rdriverFile.write("in.co.daffodil.db.jdbc.DaffodilDBDriver\n".
                            getBytes());
          rURLFile.write("jdbc:daffodilDB_embedded:school;create=true\n".
                         getBytes());
        }
        String driverName = rdriverFile.readLine();
        while (driverName != null) {
          driverName = driverName.trim();
          existingDriverName.add(driverName);
          dComboDriver.addElement(driverName);
          driverName = rdriverFile.readLine();
        }
      }
      catch (EOFException ex) {
      }
      catch (IOException ex1) {
        JOptionPane.showMessageDialog(this, ex1, "Error Message",
                                      JOptionPane.ERROR_MESSAGE);
        return;
      }

      try {
        String urlName = rURLFile.readLine();
        while (urlName != null) {
          urlName = urlName.trim();
          existingURLName.add(urlName);
          dComboURL.addElement(urlName);
          urlName = rURLFile.readLine();
        }
      }
      catch (EOFException ex) {
      }
      catch (IOException ex1) {
        JOptionPane.showMessageDialog(this, ex1, "Error Message",
                                      JOptionPane.ERROR_MESSAGE);
        return;
      }

    }
    catch (FileNotFoundException ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
    }
    finally {
      try {
        rURLFile.close();
        rdriverFile.close();
      }
      catch (IOException ex3) {
        // Ignore the exception
      }
    }
  }

  private void jbInit() throws Exception {
    border1 = new EtchedBorder(EtchedBorder.RAISED, Color.white,
                               new Color(148, 145, 140));
    border2 = BorderFactory.createEmptyBorder();
    border3 = BorderFactory.createEmptyBorder();
    titledBorder2 = new TitledBorder("");
    panel1.setLayout(null);
    panel1.setFont(new java.awt.Font("MS Sans Serif", 0, 11));
    panel1.setMaximumSize(new Dimension(32767, 32767));
    panel1.setPreferredSize(new Dimension(500, 500));
    panel1.setBounds(new Rectangle(2, 5, 529, 323));
    jPanel1.setLayout(null);

    jPanel1.setBorder(border1);
    jPanel1.setToolTipText("");
    jPanel1.setBounds(new Rectangle(12, 85, 484, 168));
    jLabel8.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabel8.setPreferredSize(new Dimension(243, 16));
    jLabel8.setHorizontalAlignment(SwingConstants.LEFT);
    jLabel8.setHorizontalTextPosition(SwingConstants.CENTER);
    jLabel8.setText("IP Address");
    jLabel8.setBounds(new Rectangle(10, 116, 90, 16));
    jLabel4.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabel4.setRequestFocusEnabled(true);
    jLabel4.setHorizontalAlignment(SwingConstants.LEFT);
    jLabel4.setHorizontalTextPosition(SwingConstants.CENTER);
    jLabel4.setText("Username");
    jLabel4.setBounds(new Rectangle(10, 69, 90, 16));
    jLabel2.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabel2.setMaximumSize(new Dimension(138, 16));
    jLabel2.setMinimumSize(new Dimension(138, 16));
    jLabel2.setPreferredSize(new Dimension(138, 16));
    jLabel2.setHorizontalAlignment(SwingConstants.LEFT);
    jLabel2.setHorizontalTextPosition(SwingConstants.CENTER);
    jLabel2.setText("Driver");
    jLabel2.setVerticalAlignment(SwingConstants.TOP);
    jLabel2.setVerticalTextPosition(SwingConstants.CENTER);
    jLabel2.setBounds(new Rectangle(10, 18, 90, 16));
    jTextPortNo.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextPortNo.setBounds(new Rectangle(113, 138, 359, 19));
    jTextPortNo.addKeyListener(new StartRepServer_jTextPortNo_keyAdapter(this));
    jTextPortNo.setDocument(new NumericDocument());
    jTextPortNo.setText("3001");
    jTextSystemName.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextSystemName.setSelectedTextColor(Color.white);
    jTextSystemName.setBounds(new Rectangle(113, 114, 359, 19));
    jTextSystemName.addKeyListener(new
                                   StartRepServer_jTextSystemName_keyAdapter(this));
    jPassword.setFont(new java.awt.Font("Dialog", 0, 12));
    jPassword.setText("daffodil");
    jPassword.setBounds(new Rectangle(113, 90, 359, 19));
    jPassword.addKeyListener(new StartRepServer_jPassword_keyAdapter(this));
    jLabel3.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabel3.setToolTipText("");
    jLabel3.setHorizontalAlignment(SwingConstants.LEFT);
    jLabel3.setHorizontalTextPosition(SwingConstants.CENTER);
    jLabel3.setText("URL");
    jLabel3.setBounds(new Rectangle(10, 44, 90, 16));
    jLabel7.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabel7.setPreferredSize(new Dimension(148, 16));
    jLabel7.setHorizontalAlignment(SwingConstants.LEFT);
    jLabel7.setHorizontalTextPosition(SwingConstants.CENTER);
    jLabel7.setText("Server Port No.");
    jLabel7.setBounds(new Rectangle(10, 140, 97, 16));
    jTextUsername.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextUsername.setText("daffodil");
    jTextUsername.setBounds(new Rectangle(113, 67, 359, 19));
    jTextUsername.addKeyListener(new StartRepServer_jTextUsername_keyAdapter(this));
    jLabel5.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabel5.setHorizontalAlignment(SwingConstants.LEFT);
    jLabel5.setHorizontalTextPosition(SwingConstants.CENTER);
    jLabel5.setText("Password");
    jLabel5.setBounds(new Rectangle(10, 93, 90, 16));
    jLabel6.setFont(new java.awt.Font("Serif", 1, 15));
    jLabel6.setForeground(Color.black);
    jLabel6.setText("DataSource Information");
    jLabel6.setBounds(new Rectangle(165, 62, 167, 20));
    jLabel1.setFont(new java.awt.Font("Serif", 3, 25));
    jLabel1.setForeground(SystemColor.infoText);
    jLabel1.setAlignmentX( (float) 0.0);
    jLabel1.setBorder(border2);
    jLabel1.setMaximumSize(new Dimension(184, 30));
    jLabel1.setMinimumSize(new Dimension(184, 30));
    jLabel1.setPreferredSize(new Dimension(184, 30));
    jLabel1.setToolTipText("");
    jLabel1.setHorizontalAlignment(SwingConstants.CENTER);
    jLabel1.setHorizontalTextPosition(SwingConstants.CENTER);
    jLabel1.setText("Daffodil Replicator");
    jLabel1.setVerticalAlignment(SwingConstants.TOP);
    jLabel1.setVerticalTextPosition(SwingConstants.CENTER);
    jLabel1.setBounds(new Rectangle(82, 5, 331, 31));
    jButtonStartServer.setBounds(new Rectangle(239, 295, 122, 26));
    jButtonStartServer.setFont(new java.awt.Font("Serif", 1, 12));
    jButtonStartServer.setAlignmentX( (float) 0.0);
    jButtonStartServer.setRequestFocusEnabled(true);
    jButtonStartServer.setActionCommand("StartServer");
    jButtonStartServer.setText("StartServer");
    jButtonStartServer.addActionListener(new
        StartRepServer_jButtonStartServer_actionAdapter(this));
    jButtonExit.setBounds(new Rectangle(370, 295, 122, 26));
    jButtonExit.setFont(new java.awt.Font("Serif", 1, 12));
    jButtonExit.setText("Exit");
    jButtonExit.addActionListener(new StartRepServer_jButtonExit_actionAdapter(this));

    this.getContentPane().setBackground(SystemColor.control);
    this.setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);

    jTextPortNo.addFocusListener(this);
    jTextSystemName.addFocusListener(this);
    jTextUsername.addFocusListener(this);
    jPassword.addFocusListener(this);
    jComboBoxDriver.addFocusListener(this);
    jComboBoxURL.addFocusListener(this);

    jComboBoxURL.addKeyListener(this);
    jComboBoxDriver.addKeyListener(this);
    jTextPortNo.addKeyListener(this);
    jTextSystemName.addKeyListener(this);
    jTextUsername.addKeyListener(this);
    jPassword.addKeyListener(this);

    jComboBoxDriver.setFont(new java.awt.Font("Dialog", 0, 12));
    jComboBoxDriver.setAutoscrolls(false);
    jComboBoxDriver.setToolTipText("");
    jComboBoxDriver.setEditable(true);
    jComboBoxDriver.setBounds(new Rectangle(113, 17, 359, 20));
    jComboBoxDriver.addKeyListener(new
                                   StartRepServer_jComboBoxDriver_keyAdapter(this));
    jComboBoxURL.setFont(new java.awt.Font("Dialog", 0, 12));
    jComboBoxURL.setEditable(true);
    jComboBoxURL.setBounds(new Rectangle(113, 42, 359, 20));
    jComboBoxURL.addKeyListener(new StartRepServer_jComboBoxURL_keyAdapter(this));

    help.setBackground(UIManager.getColor("Button.background"));
    help.setEnabled(false);
    help.setFont(new java.awt.Font("Dialog", 2, 12));
    help.setDebugGraphicsOptions(0);
    help.setDoubleBuffered(false);
    help.setPreferredSize(new Dimension(508, 22));
    help.setRequestFocusEnabled(false);
    help.setDisabledTextColor(Color.black);
    help.setEditable(false);
    help.setSelectedTextColor(Color.white);
    help.setText("Enter Driver name and URL to connect with database");
    help.setBounds(new Rectangle(15, 258, 498, 24));
    jPanel1.add(jLabel5, null);
    jPanel1.add(jLabel8, null);
    jPanel1.add(jLabel7, null);
    jPanel1.add(jLabel4, null);
    jPanel1.add(jTextSystemName, null);
    jPanel1.add(jPassword, null);
    jPanel1.add(jTextUsername, null);
    jPanel1.add(jComboBoxURL, null);
    jPanel1.add(jComboBoxDriver, null);
    jPanel1.add(jTextPortNo, null);
    jPanel1.add(jLabel2, null);
    jPanel1.add(jLabel3, null);
    panel1.add(jLabel6, null);
    panel1.add(jPanel1, null);
    panel1.add(help, null);
    panel1.add(jButtonStartServer, null);
    panel1.add(jButtonExit, null);
    panel1.add(jLabel1, null);
    this.getContentPane().add(panel1, null);

    this.addWindowListener(new WindowAdapter() {
      public void windowClosing(WindowEvent we) {
        System.exit(0);
      }
    });
    jButtonStartServer.setEnabled(false);
  }

  void jButtonStartServer_actionPerformed(ActionEvent e) {
    jButtonStartServer.setEnabled(false);

    try {
      if (!vefifyFields()) {
        JOptionPane.showMessageDialog(this,
                                      "Enter all the required Information",
                                      "Error Message",
                                      JOptionPane.ERROR_MESSAGE);
        jButtonStartServer.setEnabled(true);
        return;
      }

      String driver = ( (String) jComboBoxDriver.getSelectedItem()).trim();
      String URL = ( (String) jComboBoxURL.getSelectedItem()).trim();
      String userName = jTextUsername.getText().trim();
      if (userName.equals("")) {
        throw new Exception("User name can not be blank");
      }
      String password = jPassword.getText();
      if (jTextSystemName.getText().trim().equals("")) {
        throw new Exception("Enter System IP Address");
      }

      if (!existingDriverName.contains(driver)) {
        addDriverNameToLog(driver);
      }
      if (!existingURLName.contains(URL)) {
        addURLNameToLog(URL);

      }
      int portNo = (Integer.valueOf(jTextPortNo.getText())).intValue();
      if (portNo < 0 || portNo > 65535) {
        throw new NumberFormatException();
      }
      String systemName = jTextSystemName.getText();

      //Core Logic
      if (repServer == null) {
        repServer = ReplicationServer.getInstance(portNo, systemName);
      }
      repServer.setDataSource(driver, URL, userName, password);

      mainFrame = new MainFrame(repServer, selectedserver);
      this.hide();
      mainFrame.setBounds(10, 10,
                          (int) (Toolkit.getDefaultToolkit().getScreenSize().
                                 getWidth()) - 50,
                          (int) (Toolkit.getDefaultToolkit().getScreenSize().
                                 getHeight()) - 50);
      mainFrame.show();
    }
    catch (NumberFormatException ex2) {
      JOptionPane.showMessageDialog(this,
          "Enter only Positive Integer in Server Port No", "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      jButtonStartServer.setEnabled(true);
      return;
    }
    catch (Exception ex) {
//         ex.printStackTrace();
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      jButtonStartServer.setEnabled(true);
      return;
    }
  }

  public static JFrame getMainFrame() {
    return mainFrame;
  }

  void jButtonExit_actionPerformed(ActionEvent e) {
    System.exit(00);
  }

  private void addDriverNameToLog(String driver) {
    try {
      if (!driver.equalsIgnoreCase("")) {
        rdriverFile = new RandomAccessFile(PathHandler.getRepHome() +
                                           File.separator + "dinfo.lg", "rw");
        rdriverFile.seek(rdriverFile.length());
        rdriverFile.write( (driver + "\n").getBytes());
        rdriverFile.close();
      }
    }
    catch (FileNotFoundException ex) {
      JOptionPane.showMessageDialog(this, "File Not Found", "Error Message",
                                    JOptionPane.INFORMATION_MESSAGE);
    }
    catch (IOException ex1) {
      JOptionPane.showMessageDialog(this, "File Not Found", "Error Message",
                                    JOptionPane.INFORMATION_MESSAGE);
    }
  }

  private void addURLNameToLog(String url) {
    try {
      if (!url.equalsIgnoreCase("")) {
        rURLFile = new RandomAccessFile(PathHandler.getRepHome() +
                                        File.separator + "urlinfo.lg", "rw");
        rURLFile.seek(rURLFile.length());
        rURLFile.write( (url + "\n").getBytes());
        rURLFile.close();
      }
    }
    catch (FileNotFoundException ex) {
      JOptionPane.showMessageDialog(this, "File Not Found", "Error Message",
                                    JOptionPane.INFORMATION_MESSAGE);
    }
    catch (IOException ex1) {
      JOptionPane.showMessageDialog(this, "File Not Found", "Error Message",
                                    JOptionPane.INFORMATION_MESSAGE);
    }
  }

  public void focusGained(FocusEvent fe) {
    if (fe.getSource() instanceof JTextField) {
      if ( ( (JTextField) fe.getSource()).equals(jTextPortNo)) {
        help.setText("Enter Port number in this box");
      }
      else if ( ( (JTextField) fe.getSource()).equals(jTextSystemName)) {
        help.setText("Enter System IP Address in this box");
      }
      else if ( ( (JTextField) fe.getSource()).equals(jTextUsername)) {
        help.setText("Enter User Name in this box");
      }
      else if ( ( (JTextField) fe.getSource()).equals(jPassword)) {
        help.setText("Enter Password in this box");
      }
      ( (JTextField) fe.getSource()).selectAll();
    }
    if (fe.getSource() instanceof JComboBox) {
      if ( ( (JComboBox) fe.getSource()).equals(jComboBoxDriver)) {
        help.setText("Enter Driver Name ");
      }
      else if ( ( (JComboBox) fe.getSource()).equals(jComboBoxURL)) {
        help.setText("Enter URL ");
      }
    }
  }

  public void focusLost(FocusEvent fe) {
    jButtonStartServer.setEnabled(true);
    if (jComboBoxDriver.getItemCount() > 0 || jComboBoxURL.getItemCount() > 0) {
      if (jTextSystemName.getText().equals("") ||
          jTextUsername.getText().equals("") || jTextPortNo.getText().equals("") ||
          /*             jPassword.getText().equals("") ||*/
          jComboBoxDriver.getSelectedItem().equals("") ||
          jComboBoxURL.getSelectedItem().equals("")) {

        jButtonStartServer.setEnabled(false);
      }
    }
  }

  public void keyTyped(KeyEvent keyEvent) {
  }

  public void keyPressed(KeyEvent keyEvent) {
  }

  public void keyReleased(KeyEvent keyEvent) {
  }

  boolean vefifyFields() {
    if (jComboBoxDriver.getItemCount() > 0 || jComboBoxURL.getItemCount() > 0) {

      if (jTextSystemName.getText().trim().equals("") ||
          jTextUsername.getText().trim().equals("") ||
          jTextPortNo.getText().trim().equals("") ||
          jComboBoxDriver.getSelectedItem().equals("") ||
          jComboBoxURL.getSelectedItem().equals("")) {
        return false;
      }
    }
    else {
      return false;
    }
    return true;
  }

  void jTextUsername_keyReleased(KeyEvent e) {
    if (e.getKeyCode() == KeyEvent.VK_ENTER) {
      jButtonStartServer_actionPerformed(null);
    }
  }

  void jComboBoxDriver_keyReleased(KeyEvent e) {
    if (e.getKeyCode() == KeyEvent.VK_ENTER) {
      jButtonStartServer_actionPerformed(null);
    }
  }

  void jComboBoxURL_keyReleased(KeyEvent e) {
    if (e.getKeyCode() == KeyEvent.VK_ENTER) {
      jButtonStartServer_actionPerformed(null);
    }
  }

  void jPassword_keyReleased(KeyEvent e) {
    if (e.getKeyCode() == KeyEvent.VK_ENTER) {
      jButtonStartServer_actionPerformed(null);
    }

  }

  void jTextSystemName_keyReleased(KeyEvent e) {
    if (e.getKeyCode() == KeyEvent.VK_ENTER) {
      jButtonStartServer_actionPerformed(null);
    }

  }

  void jTextPortNo_keyReleased(KeyEvent e) {
    if (e.getKeyCode() == KeyEvent.VK_ENTER) {
      jButtonStartServer_actionPerformed(null);
    }
  }
}

class StartRepServer_jButtonStartServer_actionAdapter
    implements java.awt.event.ActionListener {
  StartRepServer adaptee;

  StartRepServer_jButtonStartServer_actionAdapter(StartRepServer adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonStartServer_actionPerformed(e);
  }
}

class StartRepServer_jButtonExit_actionAdapter
    implements java.awt.event.ActionListener {
  StartRepServer adaptee;

  StartRepServer_jButtonExit_actionAdapter(StartRepServer adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonExit_actionPerformed(e);
  }
}

class StartRepServer_jTextUsername_keyAdapter
    extends java.awt.event.KeyAdapter {
  StartRepServer adaptee;

  StartRepServer_jTextUsername_keyAdapter(StartRepServer adaptee) {
    this.adaptee = adaptee;
  }

  public void keyReleased(KeyEvent e) {
    adaptee.jTextUsername_keyReleased(e);
  }
}

class StartRepServer_jComboBoxDriver_keyAdapter
    extends java.awt.event.KeyAdapter {
  StartRepServer adaptee;

  StartRepServer_jComboBoxDriver_keyAdapter(StartRepServer adaptee) {
    this.adaptee = adaptee;
  }

  public void keyReleased(KeyEvent e) {
    adaptee.jComboBoxDriver_keyReleased(e);
  }
}

class StartRepServer_jComboBoxURL_keyAdapter
    extends java.awt.event.KeyAdapter {
  StartRepServer adaptee;

  StartRepServer_jComboBoxURL_keyAdapter(StartRepServer adaptee) {
    this.adaptee = adaptee;
  }

  public void keyReleased(KeyEvent e) {
    adaptee.jComboBoxURL_keyReleased(e);
  }
}

class StartRepServer_jPassword_keyAdapter
    extends java.awt.event.KeyAdapter {
  StartRepServer adaptee;

  StartRepServer_jPassword_keyAdapter(StartRepServer adaptee) {
    this.adaptee = adaptee;
  }

  public void keyReleased(KeyEvent e) {
    adaptee.jPassword_keyReleased(e);
  }
}

class StartRepServer_jTextSystemName_keyAdapter
    extends java.awt.event.KeyAdapter {
  StartRepServer adaptee;

  StartRepServer_jTextSystemName_keyAdapter(StartRepServer adaptee) {
    this.adaptee = adaptee;
  }

  public void keyReleased(KeyEvent e) {
    adaptee.jTextSystemName_keyReleased(e);
  }
}

class StartRepServer_jTextPortNo_keyAdapter
    extends java.awt.event.KeyAdapter {
  StartRepServer adaptee;

  StartRepServer_jTextPortNo_keyAdapter(StartRepServer adaptee) {
    this.adaptee = adaptee;
  }

  public void keyReleased(KeyEvent e) {
    adaptee.jTextPortNo_keyReleased(e);
  }
}
