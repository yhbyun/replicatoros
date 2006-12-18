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

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class UnSubscribe extends JDialog implements FocusListener, KeyListener
{
    JPanel panel1 = new JPanel();
    JLabel jLabel1 = new JLabel();
    JTextField jTextSubName = new JTextField();
    JButton jButtonUnsubscribe = new JButton();
    JButton jButtonCancle = new JButton();
    _ReplicationServer repServer;
    JLabel jLabel2 = new JLabel();
    DefaultMutableTreeNode subRootNode;
    DefaultTreeModel defaultTreeModel;
    JLabel jLabel3 = new JLabel();
    JLabel jLabel4 = new JLabel();
    JTextField jTextRemoteServerName = new JTextField();
    JLabel jLabel6 = new JLabel();
    JEditorPane help = new JEditorPane();
    String subName;
    boolean able = true;
    JTextField jTextRemoteRepPortNo = new JTextField();
    Border border1;

    public UnSubscribe(Frame frame, String title, boolean modal)
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

//  public UnSubscribe(_ReplicationServer repServer0,
//                     DefaultMutableTreeNode subRootNode0,
//                     DefaultTreeModel defaultTreeModel0) {
//    this(null, "Unsubscribe", true);
//    repServer = repServer0;
//    subRootNode = subRootNode0;
//    defaultTreeModel = defaultTreeModel0;
//  }
//
//  public UnSubscribe(_ReplicationServer repServer0,
//                     DefaultMutableTreeNode subRootNode0,
//                     DefaultTreeModel defaultTreeModel0, String subName0) {
//    this(null, "Unsubscribe", true);
//    repServer = repServer0;
//    subRootNode = subRootNode0;
//    defaultTreeModel = defaultTreeModel0;
//    subName = subName0;
//    init();
//  }

    public UnSubscribe(_ReplicationServer repServer0,
                       DefaultMutableTreeNode subRootNode0,
                       DefaultTreeModel defaultTreeModel0, boolean able0)
    {
        this(StartRepServer.getMainFrame(), "Unsubscribe", true);
        repServer = repServer0;
        subRootNode = subRootNode0;
        defaultTreeModel = defaultTreeModel0;
        able = able0;
    }

    public UnSubscribe(_ReplicationServer repServer0,
                       DefaultMutableTreeNode subRootNode0,
                       DefaultTreeModel defaultTreeModel0, String subName0,
                       boolean able0)
    {
        this(null, "Unsubscribe", true);
        repServer = repServer0;
        subRootNode = subRootNode0;
        defaultTreeModel = defaultTreeModel0;
        subName = subName0;
        able = able0;
        init();
    }

    private void init()
    {
        try
        {
            _Subscription sub = repServer.getSubscription(subName);
            jTextSubName.setText(subName);
            jTextSubName.setEnabled(able);
            jTextRemoteRepPortNo.setText("" + sub.getRemoteServerPortNo());
            jTextRemoteServerName.setText(sub.getRemoteServerUrl());
            if (!jTextRemoteRepPortNo.getText().trim().equalsIgnoreCase("") &&
                !jTextRemoteServerName.getText().equalsIgnoreCase(""))
            {
                jButtonUnsubscribe.setEnabled(true);
            }
        }
        catch (RepException ex)
        {
            JOptionPane.showMessageDialog(this, ex, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
    }

    private void jbInit() throws Exception
    {
        border1 = new EtchedBorder(EtchedBorder.RAISED, Color.white,
                                   new Color(148, 145, 140));
        panel1.setLayout(null);
        jLabel1.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel1.setMaximumSize(new Dimension(136, 16));
        jLabel1.setMinimumSize(new Dimension(136, 16));
        jLabel1.setPreferredSize(new Dimension(136, 16));
        jLabel1.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel1.setHorizontalTextPosition(SwingConstants.LEFT);
        jLabel1.setText("Subscription Name");
        jLabel1.setBounds(new Rectangle(19, 84, 174, 23));
        panel1.setAlignmentY( (float) 0.5);
        panel1.setDebugGraphicsOptions(0);
        jTextSubName.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextSubName.setNextFocusableComponent(jTextRemoteServerName);
        jTextSubName.setPreferredSize(new Dimension(6, 22));
        jTextSubName.setRequestFocusEnabled(true);
        jTextSubName.setFocusAccelerator('1');
        jTextSubName.setText("");
        jTextSubName.setBounds(new Rectangle(188, 82, 158, 23));
        jButtonUnsubscribe.setBounds(new Rectangle(113, 216, 117, 25));
        jButtonUnsubscribe.setEnabled(false);
        jButtonUnsubscribe.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonUnsubscribe.setText("UnSubscribe");
        jButtonUnsubscribe.addActionListener(new
                                             UnSubscribe_jButtonUnsubscribe_actionAdapter(this));
        jButtonCancle.setBounds(new Rectangle(240, 216, 117, 25));
        jButtonCancle.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonCancle.setToolTipText("");
        jButtonCancle.setText("Cancel");
        jButtonCancle.addActionListener(new UnSubscribe_jButtonCancle_actionAdapter(this));
        jLabel2.setEnabled(true);
        jLabel2.setFont(new java.awt.Font("Serif", 3, 25));
        jLabel2.setForeground(SystemColor.infoText);
        jLabel2.setText("Drop Subscription");
        jLabel2.setBounds(new Rectangle(92, 5, 193, 35));
        jLabel3.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel3.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel3.setHorizontalTextPosition(SwingConstants.LEFT);
        jLabel3.setText("Publication Port Number");
        jLabel3.setBounds(new Rectangle(19, 142, 174, 15));
        jLabel4.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel4.setMinimumSize(new Dimension(136, 16));
        jLabel4.setRequestFocusEnabled(true);
        jLabel4.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel4.setHorizontalTextPosition(SwingConstants.LEFT);
        jLabel4.setText("Publication Server Name");
        jLabel4.setBounds(new Rectangle(19, 115, 174, 15));
        jTextRemoteServerName.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextRemoteServerName.setDoubleBuffered(false);
        jTextRemoteServerName.setText("");
        jTextRemoteServerName.setBounds(new Rectangle(188, 110, 158, 23));
        jLabel6.setBorder(border1);
        jLabel6.setText("");
        jLabel6.setBounds(new Rectangle(9, 66, 350, 105));
        help.setBackground(UIManager.getColor("Button.background"));
        help.setEnabled(false);
        help.setFont(new java.awt.Font("Dialog", 2, 12));
        help.setRequestFocusEnabled(false);
        help.setToolTipText("");
        help.setDisabledTextColor(Color.black);
        help.setEditable(false);
        help.setText("Deletes specified subscription");
        help.setBounds(new Rectangle(12, 176, 341, 23));
        jTextRemoteRepPortNo.setMinimumSize(new Dimension(6, 21));
        jTextRemoteRepPortNo.setText("");
        jTextRemoteRepPortNo.setBounds(new Rectangle(188, 138, 158, 23));
        getContentPane().add(panel1);

        jTextRemoteRepPortNo.setDocument(new NumericDocument());

        jTextRemoteServerName.addKeyListener(this);
        jTextSubName.addKeyListener(this);
        jTextRemoteRepPortNo.addKeyListener(this);
        jTextRemoteRepPortNo.addFocusListener(this);
        jTextRemoteServerName.addFocusListener(this);
        jTextSubName.addFocusListener(this);

        jTextSubName.grabFocus();
        panel1.add(help, null);
        panel1.add(jTextSubName, null);
        panel1.add(jLabel4, null);
        panel1.add(jLabel3, null);
        panel1.add(jLabel1, null);
        panel1.add(jButtonCancle, null);
        panel1.add(jButtonUnsubscribe, null);
        panel1.add(jTextRemoteServerName, null);
        panel1.add(jTextRemoteRepPortNo, null);
        panel1.add(jLabel6, null);
        panel1.add(jLabel2, null);
    }

    void jButtonCancle_actionPerformed(ActionEvent e)
    {
        hide();
    }

    void jButtonUnsubscribe_actionPerformed(ActionEvent e)
    {
        try
        {
            String subName = jTextSubName.getText().trim();
            if (subName.equalsIgnoreCase(""))
            {
                throw new Exception("Subscription name is not specified.");
            }
            String port = jTextRemoteRepPortNo.getText().trim();
            if (port.equalsIgnoreCase(""))
            {
                throw new Exception("Port number is not specified.");
            }
            int remoteRepPortNo = Integer.valueOf(port).intValue();
            if (remoteRepPortNo < 0)
            {
                JOptionPane.showMessageDialog(this,
                                              "Enter only positive number in Server Port No",
                                              "Error Message",
                                              JOptionPane.ERROR_MESSAGE);
                return;
            }

            String remoteServerName = jTextRemoteServerName.getText().trim();
            if (remoteServerName.equalsIgnoreCase(""))
            {
                throw new Exception("Remote Replication Server Name can not be blank.");
            }
            _Subscription sub = repServer.getSubscription(subName);
            if (sub == null)
            {
                throw new RepException("REP044", new Object[]
                                       {subName});
            }
            sub.setRemoteServerPortNo(remoteRepPortNo);
            sub.setRemoteServerUrl(remoteServerName);
            sub.unsubscribe();
            Enumeration iterator = subRootNode.children();
            while (iterator.hasMoreElements())
            {
                DefaultMutableTreeNode treeNode = (DefaultMutableTreeNode) iterator.
                    nextElement();
                if ( ( (String) treeNode.getUserObject()).equalsIgnoreCase(subName))
                {
                    defaultTreeModel.removeNodeFromParent(treeNode);
                }
            }
            JOptionPane.showMessageDialog(this, "Unsubscribed Successfully",
                                          "Success", JOptionPane.INFORMATION_MESSAGE);
            hide();
        }
        catch (RepException ex)
        {
            JOptionPane.showMessageDialog(this, ex, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
        catch (NumberFormatException ex2)
        {
//      ex2.printStackTrace();
            JOptionPane.showMessageDialog(this,
                                          "Enter valid integer value in Port Number",
                                          "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
        catch (Exception ex1)
        {
            JOptionPane.showMessageDialog(this, ex1, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
    }

    public void keyTyped(KeyEvent keyEvent)
    {
    }

    public void keyPressed(KeyEvent keyEvent)
    {
    }

    public void keyReleased(KeyEvent keyEvent)
    {
        jButtonUnsubscribe.setEnabled(true);
        if (jTextRemoteServerName.getText().trim().equalsIgnoreCase("") ||
            jTextRemoteRepPortNo.getText().trim().equalsIgnoreCase("") ||
            jTextSubName.getText().trim().equalsIgnoreCase(""))
        {
            jButtonUnsubscribe.setEnabled(false);
        }
        else
        {
            if (keyEvent.getKeyCode() == KeyEvent.VK_ENTER)
            {
                jButtonUnsubscribe_actionPerformed(null);
            }
        }

    }

    public void focusGained(FocusEvent fe)
    {
        if ( ( (JTextField) fe.getSource()).equals(jTextRemoteRepPortNo))
        {
            help.setText("Enter Port number in this box");
        }
        else if ( ( (JTextField) fe.getSource()).equals(jTextSubName))
        {
            help.setText("Enter Subscription Name in this box");
        }
        else if ( ( (JTextField) fe.getSource()).equals(jTextRemoteServerName))
        {
            help.setText("Enter Server Name in this box");

        }
        if ( (jTextRemoteServerName.getText().trim().equalsIgnoreCase("") ||
              jTextRemoteRepPortNo.getText().trim().equalsIgnoreCase("") ||
              jTextSubName.getText().trim().equalsIgnoreCase("")))
        {
            jButtonUnsubscribe.setEnabled(false);

        }
    }

    public void focusLost(FocusEvent fe)
    {
        jButtonUnsubscribe.setEnabled(true);
        if ( (jTextRemoteServerName.getText().trim().equalsIgnoreCase("") ||
              jTextRemoteRepPortNo.getText().trim().equalsIgnoreCase("") ||
              jTextSubName.getText().trim().equalsIgnoreCase("")))
        {
            jButtonUnsubscribe.setEnabled(false);
        }
    }
}

class UnSubscribe_jButtonCancle_actionAdapter implements java.awt.event.ActionListener
{
    UnSubscribe adaptee;

    UnSubscribe_jButtonCancle_actionAdapter(UnSubscribe adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonCancle_actionPerformed(e);
    }
}

class UnSubscribe_jButtonUnsubscribe_actionAdapter implements java.awt.event.ActionListener
{
    UnSubscribe adaptee;

    UnSubscribe_jButtonUnsubscribe_actionAdapter(UnSubscribe adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonUnsubscribe_actionPerformed(e);
    }
}
