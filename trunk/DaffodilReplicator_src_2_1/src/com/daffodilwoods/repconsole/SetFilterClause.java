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

import java.sql.*;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;
import javax.swing.table.*;
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

public class SetFilterClause extends JDialog
{
    JPanel panel1 = new JPanel();
    JButton jButtonBack = new JButton();
    JButton jButtonNext = new JButton();
    JButton jButtonFinish = new JButton();
    JButton jButtonCancle = new JButton();
    String[] tableNames;
    _Publication pub;
    JDialog selectTable;
    TitledBorder titledBorder1;
    TitledBorder titledBorder2;
    JScrollPane jScrollPane1 = new JScrollPane();
    RCellEditor editor = new RCellEditor(new JTextField());
    String[] columnNames = new String[]
        {
        "TableNames",
        "FilterClause"};
    DefaultTableModel dtm = new DefaultTableModel(columnNames, 0);
    JTable jTableFilter = new JTable(dtm);

    int noOFTables;
    String[] tableNameString;
    DefaultTreeModel defaultTreeModel;
    DefaultMutableTreeNode pubRootNode;
    String pubName;
    _ReplicationServer repServer;
    JLabel jLabel2 = new JLabel();
    JEditorPane help = new JEditorPane();
    String operationType=null;

    public SetFilterClause(Frame frame, String title, boolean modal)
    {
        super(frame, title, modal);
        try
        {
            jbInit();
            pack();
        }
        catch (Exception ex)
        {
            JOptionPane.showMessageDialog(this, ex, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
    }

    public SetFilterClause(_ReplicationServer repServer,
                           DefaultTreeModel defaultTreeModel,
                           DefaultMutableTreeNode pubRootNode, String pubName,
                           _Publication pub, String[] tableNames,
                           JDialog selectTable,String operationType0)
    {
        this(StartRepServer.getMainFrame(), "Set Filter Clause", true);

        this.pub = pub;
        this.tableNames = tableNames;
        this.selectTable = selectTable;
        this.defaultTreeModel = defaultTreeModel;
        this.pubRootNode = pubRootNode;
        this.pubName = pubName;
        this.repServer = repServer;
        operationType=operationType0;
//System.out.println("operationType in set filter"+operationType);
        initFilterList();
    }

    private void jbInit() throws Exception
    {

        jTableFilter.setEditingColumn(1);
        jTableFilter.setColumnSelectionAllowed(false);
        titledBorder1 = new TitledBorder("");
        titledBorder2 = new TitledBorder("");
        panel1.setLayout(null);
        jButtonBack.setBounds(new Rectangle(47, 235, 81, 26));
        jButtonBack.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonBack.setText("<Back");
        jButtonBack.addActionListener(new SetFilterClause_jButtonBack_actionAdapter(this));
        jButtonNext.setBounds(new Rectangle(136, 235, 81, 26));
        jButtonNext.setEnabled(false);
        jButtonNext.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonNext.setText("Next>");
        jButtonFinish.setBounds(new Rectangle(224, 235, 81, 26));
        jButtonFinish.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonFinish.setText("Finish");
        jButtonFinish.addActionListener(new
                                        SetFilterClause_jButtonFinish_actionAdapter(this));
        jButtonCancle.setBounds(new Rectangle(312, 235, 81, 26));
        jButtonCancle.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonCancle.setText("Cancel");
        jButtonCancle.addActionListener(new
                                        SetFilterClause_jButtonCancle_actionAdapter(this));
        jScrollPane1.setFont(new java.awt.Font("Dialog", 0, 12));
        jScrollPane1.setBounds(new Rectangle(17, 45, 375, 143));
        jLabel2.setFont(new java.awt.Font("Serif", 3, 25));
        jLabel2.setForeground(SystemColor.infoText);
        jLabel2.setPreferredSize(new Dimension(175, 33));
        jLabel2.setToolTipText("");
        jLabel2.setHorizontalAlignment(SwingConstants.CENTER);
        jLabel2.setText("Set Filter Clause");
        jLabel2.setBounds(new Rectangle(85, 2, 232, 23));
        help.setBackground(UIManager.getColor("Button.background"));
        help.setEnabled(false);
        help.setFont(new java.awt.Font("Dialog", 2, 12));
        help.setDoubleBuffered(false);
        help.setRequestFocusEnabled(false);
        help.setToolTipText("");
        help.setDisabledTextColor(Color.black);
        help.setEditable(false);
        help.setSelectionStart(106);
        help.setText(
            "Optional filter clause specified on the tables will determine the " +
            "data made available  to a Subscriber.");
        help.setBounds(new Rectangle(18, 187, 375, 42));
        jTableFilter.setFont(new java.awt.Font("Dialog", 0, 12));

        jTableFilter.addFocusListener(new FocusListener()
        {
            public void focusGained(FocusEvent fe)
            {
                help.setText("Optional filter clause specified on the tables will determine the data made available  to a Subscriber.");
            }

            public void focusLost(FocusEvent fe)
            {

            }
        });

        jTableFilter.addKeyListener(new KeyAdapter()
        {
            public void keyReleased(KeyEvent ke)
            {

            }

        });
        panel1.add(jLabel2, null);
        panel1.add(jButtonFinish, null);
        panel1.add(jButtonNext, null);
        panel1.add(jScrollPane1, null);
        panel1.add(help, null);
        panel1.add(jButtonCancle, null);
        panel1.add(jButtonBack, null);
        jScrollPane1.getViewport().add(jTableFilter, null);
        this.getContentPane().add(panel1, BorderLayout.CENTER);
    }

    void initFilterList()
    {

        noOFTables = tableNames.length;
        tableNameString = new String[noOFTables];
        for (int i = 0; i < noOFTables; i++)
        {
            tableNameString[i] = (String) tableNames[i];
            dtm.addRow(new Object[]
                       {tableNames[i], ""});
        }
        TableColumnModel tcm = jTableFilter.getColumnModel();
        tcm.getColumn(0).setCellEditor(editor);
    }

    void jButtonCancle_actionPerformed(ActionEvent e)
    {
        hide();
    }

    void jButtonBack_actionPerformed(ActionEvent e)
    {
        hide();
        selectTable.show();
    }

    void jButtonFinish_actionPerformed(ActionEvent e)
    {
      String[] filterClauseList=new String[noOFTables];
      String[] tableList=new String[noOFTables];
        try
        {
            if (jTableFilter.isEditing())
            {
                jTableFilter.getCellEditor().stopCellEditing();
            }
            for (int i = 0; i < noOFTables; i++)
            {
                String filterClause = (String) dtm.getValueAt(i, 1);
//System.out.println("filter "+ filterClause);
//System.out.println("noOFTables="+noOFTables);

                if (!filterClause.trim().equalsIgnoreCase(""))
                {
                 if(operationType.equalsIgnoreCase(RepConstants.create_Publication))
                   pub.setFilter( (String) dtm.getValueAt(i, 0), filterClause);
                 else if(operationType.equalsIgnoreCase(RepConstants.addTable_Publication)){
//System.out.println("entering filter "+filterClause+" for table "+(String) dtm.getValueAt(i, 0));
                   filterClauseList[i] = filterClause;
                   tableList[i] = (String) dtm.getValueAt(i, 0);
                 }
               }else
                 filterClauseList[i] =null ;
                   tableList[i] = (String) dtm.getValueAt(i, 0);
            }

             if(operationType.equalsIgnoreCase(RepConstants.addTable_Publication)){
              pub.addTableToPublication(tableList,filterClauseList);
              JOptionPane.showMessageDialog(this, "Publication Updated Successfully", "Success",
                                            JOptionPane.INFORMATION_MESSAGE);
              hide();
            }else if(operationType.equalsIgnoreCase(RepConstants.create_Publication)){
            pub.publish();
            createPubNode();
            JOptionPane.showMessageDialog(this, "Published Successfully", "Success",
                                          JOptionPane.INFORMATION_MESSAGE);
            hide();
          }

        }
        catch (RepException ex)
        {
            JOptionPane.showMessageDialog(this, ex, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
        catch (Exception ex1)
        {
//          ex1.printStackTrace();
            JOptionPane.showMessageDialog(this, ex1, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
    }

    private void createPubNode()
    {
        try
        {
            Connection conn = repServer.getDefaultConnection();
            DefaultMutableTreeNode node = new DefaultMutableTreeNode(pubName);
            node.setAllowsChildren(true);
            DefaultMutableTreeNode tableRootNode;
            DefaultMutableTreeNode tableNode;
            DefaultMutableTreeNode columnNode;
            tableRootNode = new DefaultMutableTreeNode("TABLES");
            for (int i = 0; i < tableNames.length; i++)
            {
                String tabName = tableNames[i];
                int lastIndex = tabName.lastIndexOf(".");
                String schemaName=null;
                if (lastIndex != -1)
                {
                   tabName.substring(0,lastIndex);
                    tabName = tabName.substring(lastIndex + 1);
                }
                tableNode = new DefaultMutableTreeNode(tabName);
                tableNode.setAllowsChildren(true);
                ResultSet rsColumns = conn.getMetaData().getColumns(null, schemaName,
                    tabName, "%");
                columnNode = new DefaultMutableTreeNode("COLUMNS");
                while (rsColumns.next())
                {
                    DefaultMutableTreeNode leafColumn = new DefaultMutableTreeNode(
                        rsColumns.getString("COLUMN_NAME"));
                    leafColumn.setAllowsChildren(false);
                    columnNode.add(leafColumn);
                }
                tableNode.add(columnNode);
                tableRootNode.add(tableNode);
            }
            node.add(tableRootNode);
            defaultTreeModel.insertNodeInto(node, pubRootNode,
                                            pubRootNode.getChildCount());

        }
        catch (SQLException ex)
        {
            JOptionPane.showMessageDialog(this, ex, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
    }
}

class SetFilterClause_jButtonCancle_actionAdapter implements java.awt.event.ActionListener
{
    SetFilterClause adaptee;

    SetFilterClause_jButtonCancle_actionAdapter(SetFilterClause adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonCancle_actionPerformed(e);
    }
}

class SetFilterClause_jButtonBack_actionAdapter implements java.awt.event.ActionListener
{
    SetFilterClause adaptee;

    SetFilterClause_jButtonBack_actionAdapter(SetFilterClause adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonBack_actionPerformed(e);
    }
}

class SetFilterClause_jButtonFinish_actionAdapter implements java.awt.event.ActionListener
{
    SetFilterClause adaptee;

    SetFilterClause_jButtonFinish_actionAdapter(SetFilterClause adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonFinish_actionPerformed(e);
    }
}
