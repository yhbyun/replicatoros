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

import java.util.*;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.tree.*;

import com.daffodilwoods.replication.*;
import java.sql.Timestamp;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class AddSchedule
    extends JDialog
    implements FocusListener, KeyListener {
  JPanel panel1 = new JPanel();
  JEditorPane help = new JEditorPane();

  JLabel jLabelAddSchedule = new JLabel();
  JLabel jLabelScheduleName = new JLabel();
  JLabel jLabelSubName = new JLabel();
  JLabel jLabelRemoteServerName = new JLabel();
  JLabel jLabelRemoteRepPortNo = new JLabel();
  JLabel jLabelStartDate = new JLabel();
  JLabel jLabelStartHour = new JLabel();
  JLabel jLabelStartMin = new JLabel();
  JLabel jLabelCountValue = new JLabel();
  JLabel jLabelRecurrenceType = new JLabel();
  JLabel jLabelRepType = new JLabel();
  JLabel jLabel1 = new JLabel();

  JComboBox startHour = new JComboBox();
  JComboBox startMin = new JComboBox();
  JComboBox RecurrenceType = new JComboBox();
  JComboBox repType = new JComboBox();
  JComboBox countValue = new JComboBox();

  JTextField jTextScheduleName = new JTextField();
  JTextField jTextSubName = new JTextField();
  JTextField jTextRemoteServerName = new JTextField();
  JTextField jTextRemoteRepPortNo = new JTextField();
  JTextField jTextStartDate = new JTextField();

  JCheckBox jCheckBoxschType = new JCheckBox();
  JPanel jPanelSchedule = new JPanel();

  JButton jButtonSchedule = new JButton();
  JButton jButtonCancel = new JButton();

  _ReplicationServer repServer;
  _Subscription sub;
  String subName;
  boolean able = true;

  Border border1;

  public AddSchedule(Frame frame, String title, boolean modal) {
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

  public AddSchedule(_ReplicationServer repServer0,
                     DefaultMutableTreeNode subRootNode0,
                     DefaultTreeModel defaultTreeModel0, boolean able0) {
    this(StartRepServer.getMainFrame(), "Add Schedule", true);
    repServer = repServer0;
    able = able0;
  }

  public AddSchedule(_ReplicationServer repServer0,
                     DefaultMutableTreeNode subRootNode0,
                     DefaultTreeModel defaultTreeModel0, String subName0,
                     boolean able0) {
    this(null, "Add Schedule", true);
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

    jLabelAddSchedule.setEnabled(true);
    jLabelAddSchedule.setFont(new java.awt.Font("Serif", 3, 25));
    jLabelAddSchedule.setForeground(SystemColor.infoText);
    jLabelAddSchedule.setText("Add Schedule");
    int center = jLabelAddSchedule.CENTER;
    jLabelAddSchedule.setHorizontalAlignment(center);
    jLabelAddSchedule.setBounds(new Rectangle(92, 5, 193, 35));

    jLabelScheduleName.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelScheduleName.setMaximumSize(new Dimension(136, 16));
    jLabelScheduleName.setMinimumSize(new Dimension(136, 16));
    jLabelScheduleName.setPreferredSize(new Dimension(136, 16));
    jLabelScheduleName.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelScheduleName.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelScheduleName.setText("Schedule Name");
    jLabelScheduleName.setBounds(new Rectangle(19, 84, 174, 23));

    jLabelSubName.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelSubName.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelSubName.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelSubName.setText("Subscription Name");
    jLabelSubName.setBounds(new Rectangle(19, 115, 174, 15));

    jLabelRemoteServerName.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelRemoteServerName.setMaximumSize(new Dimension(136, 16));
    jLabelRemoteServerName.setMinimumSize(new Dimension(136, 16));
    jLabelRemoteServerName.setPreferredSize(new Dimension(136, 16));
    jLabelRemoteServerName.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelRemoteServerName.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelRemoteServerName.setText("Remote Server Name");
    jLabelRemoteServerName.setBounds(new Rectangle(19, 142, 174, 15));

    jLabelRemoteRepPortNo.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelRemoteRepPortNo.setMaximumSize(new Dimension(136, 16));
    jLabelRemoteRepPortNo.setMinimumSize(new Dimension(136, 16));
    jLabelRemoteRepPortNo.setPreferredSize(new Dimension(136, 16));
    jLabelRemoteRepPortNo.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelRemoteRepPortNo.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelRemoteRepPortNo.setText("Remote Port No.");
    jLabelRemoteRepPortNo.setBounds(new Rectangle(19, 173, 174, 15));

    jLabelRepType.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelRepType.setMinimumSize(new Dimension(136, 16));
    jLabelRepType.setRequestFocusEnabled(true);
    jLabelRepType.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelRepType.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelRepType.setText("Replication Type");
    jLabelRepType.setBounds(new Rectangle(19, 204, 174, 15));

    jCheckBoxschType.setFont(new java.awt.Font("Dialog", 1, 13));
    jCheckBoxschType.setText("Non RealTime Scheduling");
    jCheckBoxschType.setBounds(new Rectangle(19, 235, 200, 23));

    jPanelSchedule.setEnabled(false);
    jPanelSchedule.setBorder(border1);
    jPanelSchedule.setDoubleBuffered(true);
    jPanelSchedule.setBounds(new Rectangle(15, 260, 338, 150));
    jPanelSchedule.setLayout(null);

    jLabelRecurrenceType.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelRecurrenceType.setMinimumSize(new Dimension(136, 16));
    jLabelRecurrenceType.setRequestFocusEnabled(true);
    jLabelRecurrenceType.setEnabled(false);
    jLabelRecurrenceType.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelRecurrenceType.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelRecurrenceType.setText("Recurrence Type");
    jLabelRecurrenceType.setBounds(new Rectangle(10,10, 174, 15));

    jLabelCountValue.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelCountValue.setMinimumSize(new Dimension(136, 16));
    jLabelCountValue.setRequestFocusEnabled(true);
    jLabelCountValue.setEnabled(false);
    jLabelCountValue.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelCountValue.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelCountValue.setText("Counter Value");
    jLabelCountValue.setBounds(new Rectangle(10, 45, 174, 15));

    jLabelStartDate.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelStartDate.setMinimumSize(new Dimension(136, 16));
    jLabelStartDate.setRequestFocusEnabled(true);
    jLabelStartDate.setEnabled(false);
    jLabelStartDate.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelStartDate.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelStartDate.setText("Start Date (yyyy-mm-dd)");
    jLabelStartDate.setBounds(new Rectangle(10, 80, 174, 15));

    jLabelStartHour.setFont(new java.awt.Font("Dialog", 1, 13));
    jLabelStartHour.setMinimumSize(new Dimension(136, 16));
    jLabelStartHour.setRequestFocusEnabled(true);
    jLabelStartHour.setEnabled(false);
    jLabelStartHour.setHorizontalAlignment(SwingConstants.LEFT);
    jLabelStartHour.setHorizontalTextPosition(SwingConstants.LEFT);
    jLabelStartHour.setText("Starting time (hh:mm)");
    jLabelStartHour.setBounds(new Rectangle(10, 115, 184, 15));

    jLabel1.setBounds(new Rectangle(239, 115, 10, 21));
    jLabel1.setFont(new java.awt.Font("Dialog", 0, 12));
    jLabel1.setText(" : ");
    jLabel1.setEnabled(false);

    jTextScheduleName.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextScheduleName.setPreferredSize(new Dimension(6, 22));
    jTextScheduleName.setRequestFocusEnabled(true);
    jTextScheduleName.setFocusAccelerator('1');
    jTextScheduleName.setText("");
    jTextScheduleName.setBounds(new Rectangle(188, 82, 158, 23));

    jTextSubName.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextSubName.setPreferredSize(new Dimension(6, 22));
    jTextSubName.setText("");
    jTextSubName.setBounds(new Rectangle(188, 112, 158, 23));

    jTextRemoteServerName.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextRemoteServerName.setPreferredSize(new Dimension(6, 22));
    jTextRemoteServerName.setText("");
    jTextRemoteServerName.setBounds(new Rectangle(188, 142, 158, 23));

    jTextRemoteRepPortNo.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextRemoteRepPortNo.setPreferredSize(new Dimension(6, 22));
    jTextRemoteRepPortNo.setText("");
    jTextRemoteRepPortNo.setBounds(new Rectangle(188, 172, 158, 23));
    jTextRemoteRepPortNo.setDocument(new NumericDocument());

    repType.addItem(RepConstants.replication_snapshotType);
    repType.addItem(RepConstants.replication_synchronizeType);
    repType.addItem(RepConstants.replication_pullType);
    repType.addItem(RepConstants.replication_pushType);
    repType.setBounds(new Rectangle(188, 202, 100, 23));

    RecurrenceType.addItem(RepConstants.recurrence_yearType);
    RecurrenceType.addItem(RepConstants.recurrence_monthType);
    RecurrenceType.addItem(RepConstants.recurrence_dayType);
    RecurrenceType.addItem(RepConstants.recurrence_hourType);
    RecurrenceType.addItem(RepConstants.recurrence_minuteType);
    RecurrenceType.setBounds(new Rectangle(188,10, 70, 23));
    RecurrenceType.setEnabled(false);

    countValue.setBounds(new Rectangle(188, 45, 70, 23));
    countValue.setEnabled(false);

    jTextStartDate.setFont(new java.awt.Font("Dialog", 0, 12));
    jTextStartDate.setPreferredSize(new Dimension(6, 22));
    jTextStartDate.setText("");
    jTextStartDate.setEnabled(false);
    jTextStartDate.setBounds(new Rectangle(188, 80, 100, 23));

    Vector hour = new Vector();
    for (int i = 0; i <= 23; i++) {
      hour.add(i, new Integer(i));
    }

    startHour = new JComboBox(hour);
    startHour.setEnabled(false);
    startHour.setBounds(new Rectangle(188, 115, 50, 23));

    Vector min = new Vector();
    for (int i = 0; i <= 59; i++) {
      min.add(i, new Integer(i));
    }

    startMin = new JComboBox(min);
    startMin.setEnabled(false);
    startMin.setBounds(new Rectangle(251, 115, 50, 23));

    help.setBackground(UIManager.getColor("Button.background"));
    help.setEnabled(false);
    help.setFont(new java.awt.Font("Dialog", 2, 12));
    help.setRequestFocusEnabled(false);
    help.setToolTipText("");
    help.setDisabledTextColor(Color.black);
    help.setEditable(false);
    help.setText("Add Schedule ");
    help.setBounds(new Rectangle(12, 420, 341, 23));

    jButtonSchedule.setBounds(new Rectangle(113, 454, 117, 25));
    jButtonSchedule.setEnabled(false);
    jButtonSchedule.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonSchedule.setText("Add Schedule");
    jButtonSchedule.addActionListener(new
                                      AddSchedule_jButtonSchedule_actionAdapter(this));
    jButtonCancel.setBounds(new Rectangle(240, 454, 117, 25));
    jButtonCancel.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonCancel.setToolTipText("");
    jButtonCancel.setText("Cancel");
    jButtonCancel.addActionListener(new AddSchedule_jButtonCancle_actionAdapter(this));

    jTextScheduleName.addKeyListener(this);
    jTextSubName.addKeyListener(this);
    jTextStartDate.addKeyListener(this);
    jTextScheduleName.addFocusListener(this);
    jTextSubName.addFocusListener(this);
    jTextStartDate.addFocusListener(this);
    jTextSubName.grabFocus();
    repType.addActionListener(new
                                     AddSchedule_jComboRepType_actionAdapter(this));
    RecurrenceType.addActionListener(new
                                     AddSchedule_jComboRecurrenceType_actionAdapter(this));
    jCheckBoxschType.addActionListener(new
                                       AddSchedule_jCheckBoxschType_actionAdapter(this));
    panel1.add(help);
    panel1.add(jLabelAddSchedule);
    panel1.add(jLabelScheduleName);
    panel1.add(jLabelSubName);
    panel1.add(jLabelRemoteServerName);
    panel1.add(jLabelRemoteRepPortNo);
    panel1.add(jLabelRepType);
    panel1.add(jCheckBoxschType, null);

    panel1.add(jTextScheduleName);
    panel1.add(jTextSubName);
    panel1.add(jTextRemoteServerName);
    panel1.add(jTextRemoteRepPortNo);
    panel1.add(repType);
    jPanelSchedule.add(jLabelRecurrenceType);
    jPanelSchedule.add(jLabelStartDate);
    jPanelSchedule.add(jLabelStartHour);
    jPanelSchedule.add(jLabelStartMin);
    jPanelSchedule.add(jLabelCountValue);

    jPanelSchedule.add(RecurrenceType);
    jPanelSchedule.add(jTextStartDate);
    jPanelSchedule.add(startHour);
    jPanelSchedule.add(jLabel1);
    jPanelSchedule.add(startMin);
    jPanelSchedule.add(countValue);

    panel1.add(jButtonSchedule, null);
    panel1.add(jButtonCancel, null);

    //For border purpose.
    JLabel p1 = new JLabel();
    p1.setText("");
    p1.setBounds(new Rectangle(9, 66, 350, 350));
    p1.setBorder(border1);

    panel1.add(p1);
    getContentPane().add(panel1);
    panel1.add(jPanelSchedule);
  }

  void jButtonCancle_actionPerformed(ActionEvent e) {
    dispose();
    hide();
  }

  void jButtonSchedule_actionPerformed(ActionEvent e) {
    try {
      String subName = jTextSubName.getText().trim();
      if (subName.equalsIgnoreCase("")) {
        throw new RepException("REP093", new Object[] {null});
      }
      String port = jTextRemoteRepPortNo.getText().trim();
      if (port.equalsIgnoreCase("")) {
        throw new RepException("REP094", new Object[] {null});
      }
      int remoteRepPortNo = 0;
      remoteRepPortNo = Integer.valueOf(port).intValue();

      if (remoteRepPortNo <= 0) {
        throw new RepException("REP220", new Object[] {null});
      }
      String remoteServerName = jTextRemoteServerName.getText().trim();
      if (remoteServerName.equalsIgnoreCase("")) {
        throw new RepException("REP095", new Object[] {null});
      }
      sub = repServer.getSubscription(subName);
      if (sub == null) {
        throw new RepException("REP037", new Object[] {subName});
      }

      sub.setRemoteServerPortNo(remoteRepPortNo);
      sub.setRemoteServerUrl(remoteServerName);

      if(jCheckBoxschType.isSelected())
      {
        if (countValue.getSelectedItem() == null) {
          throw new RepException("REP217", null);
        }
        String date = jTextStartDate.getText();
        String hour = startHour.getSelectedItem().toString();
        String min = startMin.getSelectedItem().toString();

        Timestamp startTimeStamp = null;
        String time = date + " " + hour + ":" + min + ":" + "00.000";
        try {
          startTimeStamp = startTimeStamp.valueOf(time);
        }
        catch (NumberFormatException ex1) {
          throw new RepException("REP211", null);
        }
        catch (IllegalArgumentException ex2) {
          throw new RepException("REP211", null);
        }
        sub.addSchedule(jTextScheduleName.getText().toString(),
                        jTextSubName.getText().toString(), "nonRealTime",
                        jTextRemoteServerName.getText(),
                        jTextRemoteRepPortNo.getText(),
                        RecurrenceType.getSelectedItem().toString(),
                        repType.getSelectedItem().toString(), startTimeStamp,
                        Integer.parseInt(countValue.getSelectedItem().toString()));
      }else{
        sub.addSchedule(jTextScheduleName.getText().toString(),
                        jTextSubName.getText().toString(), "realTime",
                        jTextRemoteServerName.getText(),
                        jTextRemoteRepPortNo.getText(),
                        "",repType.getSelectedItem().toString(),null,0);
      }
      JOptionPane.showMessageDialog(this, "Schedule added successfully",
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

  public void keyTyped(KeyEvent keyEvent) {
  }

  public void keyPressed(KeyEvent keyEvent) {
  }

  public void keyReleased(KeyEvent keyEvent) {

  }

  public void jComboRecurrenceType_actionPerformed(ActionEvent e) {
    countValue.removeAllItems();
    if (RecurrenceType.getSelectedIndex() == 0) { //Year
      for (int i = 0; i < 6; i++) {
        countValue.addItem(new Integer(i));
      }
    }
    else if (RecurrenceType.getSelectedIndex() == 1) { //Month
      for (int i = 0; i < 12; i++) {
        countValue.addItem(new Integer(i));
      }
    }
    else if (RecurrenceType.getSelectedIndex() == 2) { //Day

      for (int i = 0; i < 31; i++) {
        countValue.addItem(new Integer(i));
      }
    }
    else if (RecurrenceType.getSelectedIndex() == 3) { //Hour
      for (int i = 0; i < 24; i++) {
        countValue.addItem(new Integer(i));
      }
    }
    else if (RecurrenceType.getSelectedIndex() == 4) { //minute
       countValue.addItem(new Integer(5));
       countValue.addItem(new Integer(10));
       countValue.addItem(new Integer(15));
       countValue.addItem(new Integer(20));
       countValue.addItem(new Integer(25));
       countValue.addItem(new Integer(30));
       countValue.addItem(new Integer(35));
       countValue.addItem(new Integer(40));
       countValue.addItem(new Integer(45));

   }

  }
  void jComboRepType_actionPerformed(ActionEvent e){
  jButtonSchedule.setEnabled(true);
  }
  /**
   * will enable all the components in Schedule section if user selects the Non realTime schedule option.
   * @param e
   */
  void jCheckBoxschType_actionPerformed(ActionEvent e) {
    if (jCheckBoxschType.isSelected()) {
      for (int i = 0; i < jPanelSchedule.getComponentCount(); i++) {
        Component com = jPanelSchedule.getComponent(i);
        com.setEnabled(true);
      }
    }
    else {
      for (int i = 0; i < jPanelSchedule.getComponentCount(); i++) {
        jPanelSchedule.getComponent(i).setEnabled(false);
      }
    }
  }

  public void focusGained(FocusEvent fe) {
    if ( ( (JTextField) fe.getSource()).equals(jTextScheduleName)) {
      help.setText("Enter Schedule Name in this box");
    }
    else if ( ( (JTextField) fe.getSource()).equals(jTextSubName)) {
      help.setText("Enter Subscription Name in this box");
    }
    else if ( ( (JTextField) fe.getSource()).equals(jTextStartDate)) {
      help.setText("Enter Start Date in this box");

    }
  }

  public void focusLost(FocusEvent fe) {
    jButtonSchedule.setEnabled(true);
    if ( (jTextScheduleName.getText().trim().equalsIgnoreCase("") ||
          jTextStartDate.getText().trim().equalsIgnoreCase("") ||
          jTextSubName.getText().trim().equalsIgnoreCase(""))) {
      jButtonSchedule.setEnabled(false);
    }
  }
 /* public static void main(String[] args) {
    JFrame f= new JFrame();
     AddSchedule addSchedule = new AddSchedule(f,"",true);
         addSchedule.setBounds(new Rectangle(375,515));
         addSchedule.show();
  }*/
}

class AddSchedule_jButtonCancle_actionAdapter
    implements java.awt.event.ActionListener {
  AddSchedule adaptee;

  AddSchedule_jButtonCancle_actionAdapter(AddSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonCancle_actionPerformed(e);
  }
}

class AddSchedule_jButtonSchedule_actionAdapter
    implements java.awt.event.ActionListener {
  AddSchedule adaptee;

  AddSchedule_jButtonSchedule_actionAdapter(AddSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jButtonSchedule_actionPerformed(e);
  }
}

class AddSchedule_jComboRecurrenceType_actionAdapter
    implements java.awt.event.ActionListener {
  AddSchedule adaptee;

  AddSchedule_jComboRecurrenceType_actionAdapter(AddSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jComboRecurrenceType_actionPerformed(e);
  }
}

class AddSchedule_jCheckBoxschType_actionAdapter
    implements java.awt.event.ActionListener {
  AddSchedule adaptee;
  AddSchedule_jCheckBoxschType_actionAdapter(AddSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jCheckBoxschType_actionPerformed(e);
  }
}
class AddSchedule_jComboRepType_actionAdapter
    implements java.awt.event.ActionListener {
  AddSchedule adaptee;

  AddSchedule_jComboRepType_actionAdapter(AddSchedule adaptee) {
    this.adaptee = adaptee;
  }

  public void actionPerformed(ActionEvent e) {
    adaptee.jComboRepType_actionPerformed(e);
  }
}
