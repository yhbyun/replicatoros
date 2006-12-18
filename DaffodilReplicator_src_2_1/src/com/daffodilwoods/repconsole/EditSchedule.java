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

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;
import com.daffodilwoods.replication.*;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.*;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class EditSchedule
    extends JDialog
    implements FocusListener, KeyListener {
  JPanel panel1 = new JPanel();
  JLabel jLabel1 = new JLabel();
  JComboBox jComboScheduleName = new JComboBox();
  JTextField jTextSubName = new JTextField();
  JButton jButtonGetSchedule = new JButton();
  JButton jButtonStartSchedule = new JButton();
  JButton jButtonCancle = new JButton();
  _ReplicationServer repServer;
  JLabel jLabel2 = new JLabel();
  JLabel jLabelOldRemoteServerName = new JLabel();
  JLabel jLabelOldRemoteRepPortNo = new JLabel();
  JLabel jLabelNewRemoteServerName = new JLabel();
  JLabel jLabelNewRemoteRepPortNo = new JLabel();
  JLabel jLabelScheduleName = new JLabel();
  JTextField jTextOldRemoteServerName = new JTextField();
  JTextField jTextOldRemoteRepPortNo = new JTextField();
  JTextField jTextNewRemoteServerName = new JTextField();
  JTextField jTextNewRemoteRepPortNo = new JTextField();

  JLabel jLabel6 = new JLabel();
  JEditorPane help = new JEditorPane();
  String subName;
  boolean able = true;
  _Subscription sub = null;
  AbstractDataBaseHandler dbHandler = null;
  Border border1;

  public EditSchedule(Frame frame, String title, boolean modal, boolean able0) {
    super(frame, title, modal);
    able = able0;
    try {
      jbInit();
      pack();
    }
    catch (Exception ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
  }

  public EditSchedule(_ReplicationServer repServer0, boolean able0) {
    this(StartRepServer.getMainFrame(), "Edit Schedule", true, able0);
    repServer = repServer0;
    able = able0;
  }

  public EditSchedule(_ReplicationServer repServer0, String subName0,
                      boolean able0) {
    this(null, "Edit Schedule", true, able0);
    repServer = repServer0;
    subName = subName0;
    able = able0;
    init();
  }

  private void init() {
    try {
      sub = repServer.getSubscription(subName);
      jTextSubName.setText(subName);
      jTextSubName.setEnabled(able);
      if (!jTextNewRemoteRepPortNo.getText().trim().equalsIgnoreCase("") &&
          !jTextNewRemoteServerName.getText().trim().equalsIgnoreCase("")) {
        jButtonStartSchedule.setEnabled(true);
      }
    }
    catch (RepException ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
  }

  private void jbInit() throws Exception {
    border1 = new EtchedBorder(EtchedBorder.RAISED, Color.white,
                               new Color(148, 145, 140));
    panel1.setLayout(null);
    panel1.setAlignmentY( (float) 0.5);
    panel1.setDebugGraphicsOptions(0);

    jLabel1.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabel1.setToolTipText("");
    jLabel1.setHorizontalAlignment(SwingConstants.LEFT);
    jLabel1.setText("Subscription Name");
    jLabel1.setBounds(new Rectangle(20, 65, 141, 23));

    jLabel2.setFont(new java.awt.Font("Serif", 3, 25));
    jLabel2.setForeground(SystemColor.infoText);
    jLabel2.setVerifyInputWhenFocusTarget(true);
    jLabel2.setText("Edit Schedule");
    int center = jLabel2.CENTER;
    jLabel2.setHorizontalAlignment(center);
    jLabel2.setBounds(new Rectangle(130, 12, 167, 27));

    jLabelScheduleName.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelScheduleName.setToolTipText("");
    jLabelScheduleName.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelScheduleName.setText("Schedule Name");
    jLabelScheduleName.setBounds(new Rectangle(20, 91, 161, 23));

    jLabelOldRemoteServerName.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelOldRemoteServerName.setRequestFocusEnabled(true);
    jLabelOldRemoteServerName.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelOldRemoteServerName.setText("Old Publication Server Name");
    jLabelOldRemoteServerName.setBounds(new Rectangle(20, 119, 200, 23));

    jLabelOldRemoteRepPortNo.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelOldRemoteRepPortNo.setRequestFocusEnabled(true);
    jLabelOldRemoteRepPortNo.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelOldRemoteRepPortNo.setText("Old Publication Port No.");
    jLabelOldRemoteRepPortNo.setBounds(new Rectangle(20, 148, 161, 23));

    jLabelNewRemoteServerName.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelNewRemoteServerName.setRequestFocusEnabled(true);
    jLabelNewRemoteServerName.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelNewRemoteServerName.setText("New Publication Server Name");
    jLabelNewRemoteServerName.setBounds(new Rectangle(20, 177, 210, 23));

    jLabelNewRemoteRepPortNo.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelNewRemoteRepPortNo.setRequestFocusEnabled(true);
    jLabelNewRemoteRepPortNo.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelNewRemoteRepPortNo.setText("New Publication Port No.");
    jLabelNewRemoteRepPortNo.setBounds(new Rectangle(20, 206, 161, 23));

    jTextSubName.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextSubName.setText("");
    jTextSubName.setBounds(new Rectangle(230, 63, 167, 23));
    jTextSubName.requestFocus();
    jTextSubName.grabFocus();

    jComboScheduleName.setBounds(new Rectangle(230, 90, 90, 23));

    jButtonGetSchedule.setBounds(new Rectangle(330, 90, 68, 23));
    jButtonGetSchedule.setEnabled(true);
    jButtonGetSchedule.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonGetSchedule.setText("Load");
    jButtonGetSchedule.addActionListener(new
                                         EditSchedule_jButtonGetSchedule_actionAdapter(this));

    jTextOldRemoteServerName.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextOldRemoteServerName.setText("");
    jTextOldRemoteServerName.setEditable(false);
    jTextOldRemoteServerName.setBounds(new Rectangle(230, 117, 167, 23));

    jTextOldRemoteRepPortNo.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextOldRemoteRepPortNo.setText("");
    jTextOldRemoteRepPortNo.setEditable(false);
    jTextOldRemoteRepPortNo.setBounds(new Rectangle(230, 146, 167, 23));

    jTextNewRemoteServerName.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextNewRemoteServerName.setText("");
    jTextNewRemoteServerName.setBounds(new Rectangle(230, 177, 167, 23));

    jTextNewRemoteRepPortNo.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextNewRemoteRepPortNo.setText("");
    jTextNewRemoteRepPortNo.setBounds(new Rectangle(230, 206, 167, 23));

    jButtonStartSchedule.setBounds(new Rectangle(161, 291, 116, 25));
    jButtonStartSchedule.setEnabled(false);
    jButtonStartSchedule.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonStartSchedule.setText("Edit");
    jButtonStartSchedule.addActionListener(new
                                           EditSchedule_jButtonStartSchedule_actionAdapter(this));

    jButtonCancle.setBounds(new Rectangle(286, 291, 116, 25));
    jButtonCancle.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonCancle.setText("Cancel");
    jButtonCancle.addActionListener(new
                                    EditSchedule_jButtonCancle_actionAdapter(this));

    jLabel6.setBorder(border1);
    jLabel6.setPreferredSize(new Dimension(2, 2));
    jLabel6.setToolTipText("");
    jLabel6.setText("");
    jLabel6.setVerticalAlignment(SwingConstants.CENTER);
    jLabel6.setBounds(new Rectangle(12, 51, 400, 190));

     jTextNewRemoteRepPortNo.setDocument(new NumericDocument());

    jTextNewRemoteRepPortNo.addKeyListener(this);
    jTextNewRemoteServerName.addKeyListener(this);
    jTextSubName.addKeyListener(this);
    jTextNewRemoteRepPortNo.addFocusListener(this);
    jTextNewRemoteServerName.addFocusListener(this);
    jTextSubName.addFocusListener(this);

    help.setBackground(UIManager.getColor("Button.background"));
    help.setEnabled(false);
    help.setFont(new java.awt.Font("Dialog", 2, 12));
    help.setAlignmentY( (float) 0.5);
    help.setMinimumSize(new Dimension(352, 21));
    help.setDisabledTextColor(Color.black);
    help.setText(
        "Edit schedule ");
    help.setBounds(new Rectangle(12, 250, 339, 30));
    panel1.add(help, null);
    panel1.add(jComboScheduleName, null);
    panel1.add(jTextOldRemoteServerName, null);
    panel1.add(jTextOldRemoteRepPortNo, null);
    panel1.add(jTextNewRemoteServerName, null);
    panel1.add(jTextNewRemoteRepPortNo, null);
    panel1.add(jTextSubName, null);
    panel1.add(jLabelScheduleName);
    panel1.add(jLabel1, null);
    panel1.add(jLabel2, null);
    panel1.add(jLabelOldRemoteRepPortNo, null);
    panel1.add(jLabelOldRemoteServerName, null);
    panel1.add(jLabelNewRemoteRepPortNo, null);
    panel1.add(jLabelNewRemoteServerName, null);
    panel1.add(jButtonGetSchedule, null);
    panel1.add(jButtonCancle, null);
    panel1.add(jButtonStartSchedule, null);
    panel1.add(jLabel6, null);
    this.getContentPane().add(panel1, BorderLayout.CENTER);
  }

  void jButtonCancle_actionPerformed(ActionEvent e) {
    dispose();
    hide();
  }

  void jButtonStartSchedule_actionPerformed(ActionEvent e) {
    try {
      String subName = jTextSubName.getText().trim();
      if (subName.equalsIgnoreCase("")) {
        throw new RepException("REP093", new Object[] {null});
      }
      String port = jTextNewRemoteRepPortNo.getText().trim();
      if (port.equalsIgnoreCase("")) {
        throw new RepException("REP094", new Object[] {null});
      }
      int remoteRepPortNo = Integer.valueOf(port).intValue();
      if (remoteRepPortNo <= 0) {
       throw new RepException("REP220", new Object[] {null});
      }
      String remoteServerName = jTextNewRemoteServerName.getText().trim();
      if (remoteServerName.equalsIgnoreCase("")) {
        throw new RepException("REP095", new Object[] {null});
      }
      sub = repServer.getSubscription(subName);
      if (sub == null) {
        throw new RepException("REP037", new Object[] {subName});
      }
      sub.setRemoteServerPortNo(remoteRepPortNo);
      sub.setRemoteServerUrl(remoteServerName);

      if (jComboScheduleName.getSelectedItem() == null) {
        throw new RepException("REP207", new Object[] {null});
      }
      String scheduleName = jComboScheduleName.getSelectedItem().toString();
      String newPubServerName = jTextNewRemoteServerName.getText().toString();
      String newPubPortNo = jTextNewRemoteRepPortNo.getText().toString();
        sub.editSchedule(scheduleName, subName,
                         newPubServerName, newPubPortNo);
      JOptionPane.showMessageDialog(this, "Schedule edited Successfully",
                                    "Success", JOptionPane.INFORMATION_MESSAGE);
      hide();
    }
    catch (RepException ex) {
       RepConstants.writeERROR_FILE(ex);
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    catch (Exception ex1) {
      RepConstants.writeERROR_FILE(ex1);
      return;
    }
  }

  public void jButtonGetSchedule_actionPerformed(ActionEvent e) {
    Statement st =null;
    ResultSet rs =null;
    try {
      if(jComboScheduleName.getItemCount()!=0){
        jComboScheduleName.removeAllItems();
      }

      String subName = jTextSubName.getText().trim();
      if (subName.equalsIgnoreCase("")) {
        throw new RepException("REP093", new Object[] {null});
      }
      sub = repServer.getSubscription(subName);
      if (sub == null) {
        throw new RepException("REP037", new Object[] {subName});
      }
      dbHandler = Utility.getDatabaseHandler(((Subscription)sub).getConnectionPool(), subName);
      Connection con = repServer.getDefaultConnection();
       st = con.createStatement();
       rs = st.executeQuery("select * from " +
                                     dbHandler.getScheduleTableName() +
                                     " where " +
                                     RepConstants.subscription_subName1 +
                                     " = '" + subName + "'");

      if (rs.next()) {
        jComboScheduleName.addItem(rs.getString(RepConstants.schedule_Name));
        jTextOldRemoteServerName.setText(rs.getString(4));
        jTextOldRemoteRepPortNo.setText(rs.getString(5));
      }
      else {
        throw new RepException("REP210", new Object[] {subName});
      }
    }
    catch (RepException ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }
    catch (Exception ex) {
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (st != null) {
          st.close();
        }
      }
      catch (SQLException ex1) {
      }
    }
  }

  public void focusGained(FocusEvent fe) {
    if ( ( (JTextField) fe.getSource()).equals(jTextNewRemoteRepPortNo)) {
      help.setText("Enter New Port number in this box");
    }
    else if ( ( (JTextField) fe.getSource()).equals(jTextSubName)) {
      help.setText("Enter Subscription Name in this box");
    }
    else if ( ( (JTextField) fe.getSource()).equals(jTextNewRemoteServerName)) {
      help.setText("Enter New Server Name in this box");
    }

    if ( (jTextNewRemoteRepPortNo.getText().equals("") ||
          jTextNewRemoteServerName.getText().equals(""))) {
      jButtonStartSchedule.setEnabled(false);
    }
  }

  public void focusLost(FocusEvent fe) {
    jButtonStartSchedule.setEnabled(true);
    if (jTextNewRemoteRepPortNo.getText().equals("") ||
        jTextNewRemoteServerName.getText().equals("") ||
        jTextSubName.getText().equals("")) {
      jButtonStartSchedule.setEnabled(false);

    }
  }

  public void keyTyped(KeyEvent keyEvent) {
  }

  public void keyPressed(KeyEvent keyEvent) {
  }

  public void keyReleased(KeyEvent keyEvent) {
    jButtonStartSchedule.setEnabled(true);
    if (jTextNewRemoteRepPortNo.getText().equals("") ||
        jTextNewRemoteServerName.getText().equals("") ||
        jTextSubName.getText().equals("")) {
      jButtonStartSchedule.setEnabled(false);
    }
    else {
      if (keyEvent.getKeyCode() == KeyEvent.VK_ENTER) {
        jButtonStartSchedule_actionPerformed(null);
      }
    }

  }
}

class EditSchedule_jButtonCancle_actionAdapter
    implements java.awt.event.ActionListener {
  EditSchedule adaptee;

  EditSchedule_jButtonCancle_actionAdapter(EditSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonCancle_actionPerformed(e);
  }
}

class EditSchedule_jButtonStartSchedule_actionAdapter
    implements java.awt.event.ActionListener {
  EditSchedule adaptee;

  EditSchedule_jButtonStartSchedule_actionAdapter(EditSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonStartSchedule_actionPerformed(e);
  }

}

class EditSchedule_jButtonGetSchedule_actionAdapter
    implements java.awt.event.ActionListener {
  EditSchedule adaptee;

  EditSchedule_jButtonGetSchedule_actionAdapter(EditSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonGetSchedule_actionPerformed(e);
  }
}
