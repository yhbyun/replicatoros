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
import javax.swing.tree.*;

import com.daffodilwoods.replication.*;
import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;
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

public class RemoveSchedule
    extends JDialog
    implements FocusListener, KeyListener {
  JPanel panel1 = new JPanel();
  JEditorPane help = new JEditorPane();

  JLabel jLabelRemoveSchedule = new JLabel();
  JLabel jLabelScheduleName = new JLabel();
  JLabel jLabelSubName = new JLabel();

  JTextField jTextSubName = new JTextField();
  JComboBox jComboScheduleName = new JComboBox();

  JButton jButtonGetSchedule = new JButton();
  JButton jButtonRemoveSchedule = new JButton();
  JButton jButtonCancel = new JButton();

  _ReplicationServer repServer;
  _Subscription sub = null;
  AbstractDataBaseHandler dbHandler;

  String subName;
  boolean able = true;

  Border border1;

  public RemoveSchedule(Frame frame, String title, boolean modal) {
    super(frame, title, modal);
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

  public RemoveSchedule(_ReplicationServer repServer0,
                        DefaultMutableTreeNode subRootNode0,
                        DefaultTreeModel defaultTreeModel0, boolean able0) {
    this(StartRepServer.getMainFrame(), "Remove Schedule", true);
    repServer = repServer0;
    able = able0;
  }

  public RemoveSchedule(_ReplicationServer repServer0,
                        DefaultMutableTreeNode subRootNode0,
                        DefaultTreeModel defaultTreeModel0, String subName0,
                        boolean able0) {
    this(null, "Remove Schedule", true);
    repServer = repServer0;
    subName = subName0;
    able = able0;
    init();
  }

  private void init() {
    try {
      _Subscription sub = repServer.getSubscription(subName);
      jTextSubName.setText(subName);
      jTextSubName.setEnabled(able);
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

    jLabelRemoveSchedule.setEnabled(true);
    jLabelRemoveSchedule.setFont(new java.awt.Font("Serif", 3, 25));
    jLabelRemoveSchedule.setForeground(SystemColor.infoText);
    jLabelRemoveSchedule.setText("Remove Schedule");
    int center=jLabelRemoveSchedule.CENTER;
    jLabelRemoveSchedule.setHorizontalAlignment(center);
    jLabelRemoveSchedule.setBounds(new Rectangle(92, 5, 193, 35));

    jLabelSubName.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelSubName.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelSubName.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelSubName.setText("Subscriber's Name");
    jLabelSubName.setBounds(new Rectangle(19, 84, 174, 23));

    jLabelScheduleName.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelScheduleName.setMaximumSize(new Dimension(136, 16));
    jLabelScheduleName.setMinimumSize(new Dimension(136, 16));
    jLabelScheduleName.setPreferredSize(new Dimension(136, 16));
    jLabelScheduleName.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelScheduleName.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelScheduleName.setText("Schedule Name");
    jLabelScheduleName.setBounds(new Rectangle(19, 115, 174, 15));

    jTextSubName.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextSubName.setPreferredSize(new Dimension(6, 22));
    jTextSubName.setText("");
    jTextSubName.setBounds(new Rectangle(188, 82, 168, 23));
    jTextSubName.setEnabled(true);
    jTextSubName.addFocusListener(this);

    jComboScheduleName.setBounds(new Rectangle(188, 115, 90, 23));

    jButtonGetSchedule.setBounds(new Rectangle(286, 115, 70, 23));
    jButtonGetSchedule.setEnabled(true);
    jButtonGetSchedule.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonGetSchedule.setText("Load");
    jButtonGetSchedule.addActionListener(new
                                         RemoveSchedule_jButtonGetSchedule_actionAdapter(this));

    help.setBackground(UIManager.getColor("Button.background"));
    help.setEnabled(false);
    help.setFont(new java.awt.Font("Dialog", 2, 12));
    help.setRequestFocusEnabled(false);
    help.setToolTipText("");
    help.setDisabledTextColor(Color.black);
    help.setEditable(false);
    help.setText("Remove the selected schedule");
    help.setBounds(new Rectangle(12, 155, 341, 23));

    jButtonRemoveSchedule.setBounds(new Rectangle(113, 186, 117, 25));
    jButtonRemoveSchedule.setEnabled(false);
    jButtonRemoveSchedule.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonRemoveSchedule.setText("Remove");
    jButtonRemoveSchedule.addActionListener(new
                                            RemoveSchedule_jButtonRemoveSchedule_actionAdapter(this));

    jButtonCancel.setBounds(new Rectangle(240, 186, 117, 25));
    jButtonCancel.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonCancel.setToolTipText("");
    jButtonCancel.setText("Cancel");
    jButtonCancel.addActionListener(new
                                    RemoveSchedule_jButtonCancle_actionAdapter(this));

    jTextSubName.requestFocus();
    jTextSubName.grabFocus();
    jTextSubName.addKeyListener(this);
    jTextSubName.addFocusListener(this);

    panel1.add(help);
    panel1.add(jLabelRemoveSchedule);
    panel1.add(jLabelScheduleName);
    panel1.add(jLabelSubName);
    panel1.add(jButtonGetSchedule, null);
    panel1.add(jComboScheduleName);
    panel1.add(jTextSubName);

    panel1.add(jTextSubName);

    panel1.add(jButtonRemoveSchedule, null);
    panel1.add(jButtonCancel, null);

    //For border purpose.
    JLabel p1 = new JLabel();
    p1.setText("");
    p1.setBounds(new Rectangle(9, 66, 352, 90));
    p1.setBorder(border1);

    panel1.add(p1);
    getContentPane().add(panel1);
  }

  void jButtonCancle_actionPerformed(ActionEvent e) {
   dispose();
    hide();
  }

  void jButtonRemoveSchedule_actionPerformed(ActionEvent e) {
    try {
      String subName = jTextSubName.getText().trim();
      if (subName.equalsIgnoreCase("")) {
        throw new RepException("REP093", new Object[] {null});
      }
      sub = repServer.getSubscription(subName);
      if (sub == null) {
        throw new RepException("REP037", new Object[] {subName});
      }
      if (jComboScheduleName.getSelectedItem() == null) {
        throw new RepException("REP207", new Object[] {null});
      }
      sub.removeSchedule(jComboScheduleName.getSelectedItem().toString(),subName);
      JOptionPane.showMessageDialog(this, "Schedule dropped Successfully",
                                    "Success", JOptionPane.INFORMATION_MESSAGE);
      hide();
    }
    catch (RepException ex) {
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
    jButtonRemoveSchedule.setEnabled(true);
    if (jTextSubName.getText().equals("") ){
      jButtonRemoveSchedule.setEnabled(false);
    }
    else {
      if (keyEvent.getKeyCode() == KeyEvent.VK_ENTER) {
        jButtonRemoveSchedule_actionPerformed(null);
      }
    }
  }

  public void jButtonGetSchedule_actionPerformed(ActionEvent e) {
    ResultSet rs =null;
    Statement st=null;
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
      StringBuffer query=new StringBuffer();
      query=query.append("select * from ").append(dbHandler.getScheduleTableName())
                                     .append(" where ")
                                     .append(RepConstants.subscription_subName1)
                                     .append(" = '") .append(subName ).append("'");

       rs = st.executeQuery(query.toString());
      if (rs.next()) {
        jComboScheduleName.addItem(rs.getString(RepConstants.schedule_Name));
        jButtonRemoveSchedule.setEnabled(true);
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
      RepConstants.writeERROR_FILE(ex);
//      ex.printStackTrace();
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
    if ( ( (JTextField) fe.getSource()).equals(jTextSubName)) {
      help.setText("Enter Subscription Name in this box");
    }
    if (! (jTextSubName.getText().equals(""))) {
      jButtonRemoveSchedule.setEnabled(true);
    }
  }

  public void focusLost(FocusEvent fe) {
    jButtonRemoveSchedule.setEnabled(true);
    if (jTextSubName.getText().equals("")) {
      jButtonRemoveSchedule.setEnabled(false);

    }
  }
}

class RemoveSchedule_jButtonCancle_actionAdapter
    implements java.awt.event.ActionListener {
  RemoveSchedule adaptee;

  RemoveSchedule_jButtonCancle_actionAdapter(RemoveSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonCancle_actionPerformed(e);
  }
}

class RemoveSchedule_jButtonRemoveSchedule_actionAdapter
    implements java.awt.event.ActionListener {
  RemoveSchedule adaptee;

  RemoveSchedule_jButtonRemoveSchedule_actionAdapter(RemoveSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonRemoveSchedule_actionPerformed(e);
  }
}

class RemoveSchedule_jButtonGetSchedule_actionAdapter
    implements java.awt.event.ActionListener {
  RemoveSchedule adaptee;

  RemoveSchedule_jButtonGetSchedule_actionAdapter(RemoveSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonGetSchedule_actionPerformed(e);
  }
}
