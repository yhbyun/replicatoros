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

public class UnPublish extends JDialog implements KeyListener, FocusListener
{
    JPanel panel1 = new JPanel();
    JLabel jLabel1 = new JLabel();
    JTextField jTextPubName = new JTextField();
    JButton jButtonUnpublish = new JButton();
    JButton jButtonCancle = new JButton();
    static _ReplicationServer repServer;
    JLabel jLabel2 = new JLabel();
    static DefaultMutableTreeNode pubRootNode;
    static DefaultTreeModel defaultTreeModel;
    JLabel jLabel4 = new JLabel();
    JEditorPane help = new JEditorPane();
    String pubName;
    Border border1;

    public UnPublish(Frame frame, String title, boolean modal)
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

    public UnPublish(_ReplicationServer repServer0,
                     DefaultMutableTreeNode pubRootNode0,
                     DefaultTreeModel defaultTreeModel0)
    {
        this(StartRepServer.getMainFrame(), "UnPublish", true);
        repServer = repServer0;
        pubRootNode = pubRootNode0;
        defaultTreeModel = defaultTreeModel0;
    }

    public UnPublish(_ReplicationServer repServer0,
                     DefaultMutableTreeNode pubRootNode0,
                     DefaultTreeModel defaultTreeModel0, String pubName0)
    {
        this(null, "UnPublish", true);
        repServer = repServer0;
        pubRootNode = pubRootNode0;
        defaultTreeModel = defaultTreeModel0;
        pubName = pubName0;
        init();
    }

    private void init()
    {
        try
        {
            _Publication sub = repServer.getPublication(pubName);
            jTextPubName.setText(pubName);
            jButtonUnpublish.enable(true);
        }
        catch (Exception ex)
        {
            JOptionPane.showMessageDialog(this, ex, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
//      ex.printStackTrace();
            return;
        }
    }

    public static void dropPublication(String nameOfPublication, Component com,
                                       _ReplicationServer server,
                                       DefaultMutableTreeNode pubRootNode0,
                                       DefaultTreeModel defaultTreeModel0)
    {
        _Publication pub = null;
        try
        {
            pub = server.getPublication(nameOfPublication);
            if (pub == null)
            {
                throw new RepException("REP043", new Object[]
                                       {nameOfPublication});
            }
            pub.unpublish();

            pubRootNode = pubRootNode0;
            defaultTreeModel = defaultTreeModel0;

            Enumeration iterator = pubRootNode.children();
            while (iterator.hasMoreElements())
            {
                DefaultMutableTreeNode treeNode = (DefaultMutableTreeNode) iterator.
                    nextElement();
                if ( ( (String) treeNode.getUserObject()).equalsIgnoreCase(
                    nameOfPublication))
                {
                    defaultTreeModel.removeNodeFromParent(treeNode);
                }
            }
            JOptionPane.showMessageDialog(com, "Unpublished Successfully", "Success",
                                          JOptionPane.INFORMATION_MESSAGE);
        }
        catch (Exception ex)
        {
            JOptionPane.showMessageDialog(com, ex, "Error Message",
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
        jLabel1.setText("Publication Name");
        jLabel1.setBounds(new Rectangle(22, 64, 119, 23));
        panel1.setAlignmentY( (float) 0.5);
        panel1.setDebugGraphicsOptions(0);
        jTextPubName.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextPubName.setText("");
        jTextPubName.setBounds(new Rectangle(140, 64, 153, 21));
        jButtonUnpublish.setBounds(new Rectangle(90, 141, 98, 25));
        jButtonUnpublish.setEnabled(false);
        jButtonUnpublish.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonUnpublish.setText("UnPublish");
        jButtonUnpublish.addActionListener(new
                                           UnPublish_jButtonUnpublish_actionAdapter(this));
        jButtonCancle.setBounds(new Rectangle(202, 141, 98, 25));
        jButtonCancle.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonCancle.setText("Cancel");
        jButtonCancle.addActionListener(new UnPublish_jButtonCancle_actionAdapter(this));
        jLabel2.setEnabled(true);
        jLabel2.setFont(new java.awt.Font("Serif", 3, 25));
        jLabel2.setForeground(SystemColor.infoText);
        jLabel2.setText("Drop Publication");
        jLabel2.setBounds(new Rectangle(68, 2, 202, 36));
        jLabel4.setBorder(border1);
        jLabel4.setOpaque(false);
        jLabel4.setText("");
        jLabel4.setBounds(new Rectangle(13, 48, 289, 55));
        help.setBackground(UIManager.getColor("Button.background"));
        help.setEnabled(false);
        help.setFont(new java.awt.Font("Dialog", 2, 12));
        help.setRequestFocusEnabled(false);
        help.setDisabledTextColor(Color.black);
        help.setEditable(false);
        help.setText("Deletes the specified publication");
        help.setBounds(new Rectangle(20, 105, 252, 24));
        getContentPane().add(panel1);

        jTextPubName.addFocusListener(this);
        jTextPubName.addKeyListener(this);

        panel1.add(jLabel4, null);
        panel1.add(jLabel1, null);
        panel1.add(help, null);
        panel1.add(jButtonCancle, null);
        panel1.add(jButtonUnpublish, null);
        panel1.add(jLabel2, null);
        panel1.add(jTextPubName, null);
    }

    void jButtonCancle_actionPerformed(ActionEvent e)
    {
        hide();
    }

    void jButtonUnpublish_actionPerformed(ActionEvent e)
    {
        String pubName = jTextPubName.getText().trim();
        _Publication pub = null;
        try
        {
            if (pubName.equalsIgnoreCase(""))
            {
                throw new Exception("Publication Name not specified.");
            }
            pub = repServer.getPublication(pubName);
            if (pub == null)
            {
                throw new RepException("REP043", new Object[]
                                       {pubName});
            }
            pub.unpublish();

            Enumeration iterator = pubRootNode.children();
            while (iterator.hasMoreElements())
            {
                DefaultMutableTreeNode treeNode = (DefaultMutableTreeNode) iterator.
                    nextElement();
                if ( ( (String) treeNode.getUserObject()).equalsIgnoreCase(pubName))
                {
                    defaultTreeModel.removeNodeFromParent(treeNode);

                }
            }

            JOptionPane.showMessageDialog(this, "Unpublished Successfully", "Success",
                                          JOptionPane.OK_OPTION);
            hide();
        }
        catch (Exception ex)
        {
            JOptionPane.showMessageDialog(this, ex, "Error Message",
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
        jButtonUnpublish.setEnabled(true);
        if (jTextPubName.getText().trim().equals(""))
        {
            jButtonUnpublish.setEnabled(false);
        }
        else
        {
            if (keyEvent.getKeyCode() == KeyEvent.VK_ENTER)
            {
                jButtonUnpublish_actionPerformed(null);
            }
        }

    }

    public void focusGained(FocusEvent fe)
    {
        if ( ( (JTextField) fe.getSource()).equals(jTextPubName))
        {
            help.setText("Enter Publication Name ");
        }
        if (!jTextPubName.getText().trim().equals(""))
        {
            jButtonUnpublish.setEnabled(true);
        }
    }

    public void focusLost(FocusEvent fe)
    {
        jButtonUnpublish.setEnabled(true);
        if (jTextPubName.getText().trim().equals(""))
        {
            jButtonUnpublish.setEnabled(false);
        }
    }
}

class UnPublish_jButtonCancle_actionAdapter implements java.awt.event.ActionListener
{
    UnPublish adaptee;

    UnPublish_jButtonCancle_actionAdapter(UnPublish adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonCancle_actionPerformed(e);
    }
}

class UnPublish_jButtonUnpublish_actionAdapter implements java.awt.event.ActionListener
{
    UnPublish adaptee;

    UnPublish_jButtonUnpublish_actionAdapter(UnPublish adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonUnpublish_actionPerformed(e);
    }
}
