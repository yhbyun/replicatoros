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

import com.daffodilwoods.replication.DBHandler.AbstractDataBaseHandler;
import javax.swing.border.TitledBorder;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import com.daffodilwoods.replication._ReplicationServer;
import com.daffodilwoods.replication._Publication;
import javax.swing.border.Border;
import java.awt.Frame;
import javax.swing.border.EtchedBorder;
import java.awt.Color;
import java.awt.Rectangle;
import java.awt.SystemColor;
import java.util.ArrayList;
import javax.swing.*;
import java.awt.event.*;
import javax.swing.DefaultListModel;

public class SelectTableToBreakCycle
    extends JDialog
    implements FocusListener, KeyListener {
  JPanel panel1 = new JPanel();
  JButton jButton_Select = new JButton();
  JButton jButtonDeselect = new JButton();

  DefaultListModel dlmCyclicTables = new DefaultListModel();
  JList jListCyclicTables = new JList(dlmCyclicTables);

  DefaultListModel dlmSelect = new DefaultListModel();
  JList jListSelectedTables = new JList(dlmSelect);

  JLabel jLabel1 = new JLabel();
  String pubName, conflictResolver;
  _ReplicationServer repServer;
  _Publication pub;
  AbstractDataBaseHandler dbh;

  JButton jButtonOK = new JButton();
  JButton jButton3 = new JButton();
  JButton jButtonCancel = new JButton();
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
  ArrayList tablesinCycle = new ArrayList();
  ArrayList selectedBreakCycleRelation = new ArrayList();
  JScrollPane jScrollPane1 = new JScrollPane();
  DefaultListModel dlmSelectedCyclicRelation = new DefaultListModel();
  JList jList1 = new JList();

  public SelectTableToBreakCycle(Frame frame, String title,ArrayList selectedBreakCycleRelation0, boolean modal) {
    super(frame, title, modal);
    try {
      selectedBreakCycleRelation = selectedBreakCycleRelation0;
      jbInit();
      pack();
    }
    catch (Exception ex) {
      JOptionPane.showMessageDialog(this, ex, "Error Message",
                                    JOptionPane.ERROR_MESSAGE);
      return;
    }

  }

  public SelectTableToBreakCycle(DefaultTreeModel defaultTreeModel,
                                 DefaultMutableTreeNode pubRootNode,
                                 String pubName,
                                 _ReplicationServer repServer,
                                 String conflictResolver,
                                 JDialog createPublication,
                                 ArrayList tablesinCycle,
                                 ArrayList selectedBreakCycleRelation0) {

    this(StartRepServer.getMainFrame(),
         "Select Table To Break Cycle In Relation",selectedBreakCycleRelation0, true);
    this.pubName = pubName;
    this.repServer = repServer;
    this.conflictResolver = conflictResolver;
    this.createPublication = createPublication;
    this.defaultTreeModel = defaultTreeModel;
    this.pubRootNode = pubRootNode;
    this.tablesinCycle = tablesinCycle;

    initListWithDatabaseTablesForCreate();
  }

  private void jbInit() throws Exception {
    border1 = BorderFactory.createEmptyBorder();
    titledBorder1 = new TitledBorder("");
    titledBorder2 = new TitledBorder("");
    border2 = new EtchedBorder(EtchedBorder.RAISED, Color.white,
                               new Color(148, 145, 140));
    panel1.setLayout(null);

    jButton_Select.setBounds(new Rectangle(238, 99, 52, 22));
    jButton_Select.setEnabled(true);
    jButton_Select.setFont(new java.awt.Font("Dialog", 1, 15));
    jButton_Select.setText(">>");
    jButton_Select.addActionListener(new
                                     SelectTable_jButton_Select_actionAdapter(this));
    jButtonDeselect.setBounds(new Rectangle(239, 129, 52, 22));
    jButtonDeselect.setFont(new java.awt.Font("Dialog", 1, 15));
    jButtonDeselect.setText("<<");
    jButtonDeselect.addActionListener(new
                                      SelectTable_jButtonDeselect_actionAdapter(this));
    jListCyclicTables.setFont(new java.awt.Font("Dialog", 0, 12));

    jListCyclicTables.addFocusListener(this);
    jListSelectedTables.addFocusListener(this);

    jListSelectedTables.setFont(new java.awt.Font("Dialog", 0, 12));

    jLabel1.setFont(new java.awt.Font("Serif", 3, 25));
    jLabel1.setForeground(SystemColor.infoText);
    jLabel1.setText("Select relations to be suppressed to break cycle");
    jLabel1.setBounds(new Rectangle(15, 5, 494, 27));

    jButtonOK.setBounds(new Rectangle(178, 291, 81, 26));
    jButtonOK.setEnabled(true);
    jButtonOK.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonOK.setFocusPainted(true);
    jButtonOK.setText("OK");
    jButtonOK.addActionListener(new SelectTable_jButtonOK_actionAdapter(this));

    jButtonCancel.setBounds(new Rectangle(274, 291, 81, 26));
    jButtonCancel.setFont(new java.awt.Font("Dialog", 1, 12));
    jButtonCancel.setFocusPainted(true);
    jButtonCancel.setText("Cancel");
    jButtonCancel.addActionListener(new SelectTable_jButtonCancel_actionAdapter(this));

    jLabel2.setFont(new java.awt.Font("Dialog", 1, 11));
    jLabel2.setBorder(border2);
    jLabel2.setText("");
    jLabel2.setBounds(new Rectangle(15, 39, 503, 168));

    help.setBackground(UIManager.getColor("Button.background"));
    help.setForeground(SystemColor.infoText);
    help.setEnabled(true);
    help.setFont(new java.awt.Font("Dialog", 2, 12));
    help.setDoubleBuffered(false);
    help.setRequestFocusEnabled(false);
    help.setDisabledTextColor(Color.black);
    help.setEditable(false);
    help.setText("Already Selected relations to be suppressed to break cycle,if any");

    help.setBounds(new Rectangle(14, 210, 385, 22));

    jscrollpane_td = new JScrollPane(jListCyclicTables);
    jscrollpane_td.setFont(new java.awt.Font("Dialog", 1, 11));
    jscrollpane_td.setBounds(new Rectangle(19, 48, 210, 153));

    jscrollpane_st = new JScrollPane(jListSelectedTables);
    jscrollpane_st.setBounds(new Rectangle(300, 49, 210, 153));

    titledBorder2.setTitleFont(new java.awt.Font("Dialog", 0, 12));
    titledBorder1.setTitleFont(new java.awt.Font("Dialog", 0, 12));
    jScrollPane1.setBounds(new Rectangle(21, 235, 497, 35));

    jList1.setEnabled(false);
    jList1.setModel(dlmSelectedCyclicRelation);
//   System.out.println("selectedBreakCycleRelation size:"+selectedBreakCycleRelation.size());
      for (int i = 0; i < selectedBreakCycleRelation.size(); i++) {
//        System.out.println("selectedBreakCycleRelation:"+selectedBreakCycleRelation.get(i));
        dlmSelectedCyclicRelation.addElement(selectedBreakCycleRelation.get(i));
      }


    getContentPane().add(panel1);

    panel1.add(jLabel1, null);
    panel1.add(jButtonOK, null);
    panel1.add(jButtonCancel, null);
    panel1.add(jscrollpane_td);
    panel1.add(jscrollpane_st);
    panel1.add(jButton_Select, null);
    panel1.add(jLabel2, null);
    panel1.add(jButtonDeselect, null);
    panel1.add(help, null);
    panel1.add(jScrollPane1, null);
    jScrollPane1.getViewport().add(jList1, null);

    //    jListDatabaseTables.grabFocus();

    jscrollpane_st.setBorder(new TitledBorder("Selected Relations"));
    jscrollpane_td.setBorder(new TitledBorder(
        "Relations having cycle in publication"));
  }

  void initListWithDatabaseTablesForCreate() {
    jListCyclicTables.setAutoscrolls(true);
    for (int i = 0; i < tablesinCycle.size(); i++) {
      if(!selectedBreakCycleRelation.contains(tablesinCycle.get(i)))
      dlmCyclicTables.addElement(tablesinCycle.get(i));
    }
  }

  /**
   * keyPressed
   *
   * @param e KeyEvent
   */
  public void keyPressed(KeyEvent e) {
  }

  /**
   * keyReleased
   *
   * @param e KeyEvent
   */
  public void keyReleased(KeyEvent e) {
  }

  /**
   * keyTyped
   *
   * @param e KeyEvent
   */
  public void keyTyped(KeyEvent e) {
  }

  /**
   * focusGained
   *
   * @param e FocusEvent
   */
  public void focusGained(FocusEvent e) {
  }

  /**
   * focusLost
   *
   * @param e FocusEvent
   */
  public void focusLost(FocusEvent e) {
  }

  class SelectTable_jButtonCancel_actionAdapter
      implements java.awt.event.ActionListener {
    SelectTableToBreakCycle adaptee;

    SelectTable_jButtonCancel_actionAdapter(SelectTableToBreakCycle adaptee) {
      this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e) {
      adaptee.jButtonCancel_actionPerformed(e);
    }
  }

  void jButtonCancel_actionPerformed(ActionEvent e) {
    hide();
  }

  class SelectTable_jButtonOK_actionAdapter
      implements java.awt.event.ActionListener {
    SelectTableToBreakCycle adaptee;

    SelectTable_jButtonOK_actionAdapter(SelectTableToBreakCycle adaptee) {
      this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e) {
      adaptee.jButtonOK_actionPerformed(e);
    }
  }

  void jButtonOK_actionPerformed(ActionEvent e) {
    hide();
  }

  class SelectTable_jButton_Select_actionAdapter
      implements java.awt.event.ActionListener {
    SelectTableToBreakCycle adaptee;

    SelectTable_jButton_Select_actionAdapter(SelectTableToBreakCycle adaptee) {
      this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e) {
      adaptee.jButton_Select_actionPerformed(e);
    }
  }

  class SelectTable_jButtonDeselect_actionAdapter
      implements java.awt.event.ActionListener {
    SelectTableToBreakCycle adaptee;

    SelectTable_jButtonDeselect_actionAdapter(SelectTableToBreakCycle adaptee) {
      this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e) {
      adaptee.jButtonDeselect_actionPerformed(e);
    }
  }

  void jButton_Select_actionPerformed(ActionEvent e) {
    Object[] tableNames = jListCyclicTables.getSelectedValues();
    for (int i = 0; i < tableNames.length; i++) {
      String tableNameForPublishing = (String) tableNames[i];
      if (! (tableNameForPublishing == null ||
             tableNameForPublishing.equalsIgnoreCase(""))) {
        dlmCyclicTables.removeElement(tableNameForPublishing);
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
        dlmCyclicTables.addElement(deselectTableNameForPublishing);
        dlmSelect.removeElement(deselectTableNameForPublishing);
      }
    }
  }

  protected ArrayList getSelectedRelationToBreakCycle() {
    int selectedNoOfTables = dlmSelect.getSize();

    if (selectedNoOfTables == 0) {
      return new ArrayList();
    }
    ArrayList relationToBreakCycle = new ArrayList();
    for (int i = 0; i < selectedNoOfTables; i++) {
      relationToBreakCycle.add(dlmSelect.get(i));
    }
    return relationToBreakCycle;
  }

}
