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

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class CreateSubscription extends JDialog implements FocusListener, KeyListener
{
    JPanel panel1 = new JPanel();
    JLabel jLabel1 = new JLabel();
    JLabel jLabel2 = new JLabel();
    JTextField jTextSubName = new JTextField();
    _ReplicationServer repServer;
    JLabel jLabel4 = new JLabel();
    JTextField jTextPubName = new JTextField();
    JButton jButtonPublish = new JButton();
    JButton jButton2 = new JButton();
    JTextField jTextRemotePORT = new JTextField();
    JTextField jTextRemoteSystemName = new JTextField();
    JLabel jLabel3 = new JLabel();
    JLabel jLabel5 = new JLabel();
    String subName;
    DefaultMutableTreeNode node, subRootNode;
    DefaultTreeModel defaultTreeModel;
    JLabel jLabel7 = new JLabel();
    JEditorPane help = new JEditorPane();
    Border border1;

    public CreateSubscription(Frame frame, String title, boolean modal)
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

    public CreateSubscription(_ReplicationServer repServer0,
                              DefaultMutableTreeNode subRootNode,
                              DefaultTreeModel defaultTreeModel0)
    {
        this(StartRepServer.getMainFrame(), "Create Subscription", true);
        repServer = repServer0;
        this.subRootNode = subRootNode;
        defaultTreeModel = defaultTreeModel0;
    }

    private void jbInit() throws Exception
    {
        border1 = new EtchedBorder(EtchedBorder.RAISED, Color.white,
                                   new Color(148, 145, 140));
        panel1.setLayout(null);
        panel1.setDebugGraphicsOptions(0);
        jLabel1.setFont(new java.awt.Font("Serif", 3, 25));
        jLabel1.setForeground(SystemColor.infoText);
        jLabel1.setBorder(null);
        jLabel1.setText("Create Subscription");
        jLabel1.setBounds(new Rectangle(89, 9, 223, 32));
        jLabel2.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel2.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel2.setText("Subscription Name");
        jLabel2.setBounds(new Rectangle(14, 83, 164, 15));
        jTextSubName.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextSubName.setNextFocusableComponent(jTextPubName);
        jTextSubName.setFocusAccelerator('1');
        jTextSubName.setSelectionEnd(4);
        jTextSubName.setSelectionStart(0);
        jTextSubName.setText("");
        jTextSubName.setScrollOffset(1);
        jTextSubName.setBounds(new Rectangle(181, 77, 195, 21));
        jLabel4.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel4.setRequestFocusEnabled(true);
        jLabel4.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel4.setText("Publication Name");
        jLabel4.setBounds(new Rectangle(14, 109, 164, 15));
        jTextPubName.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextPubName.setSelectedTextColor(Color.white);
        jTextPubName.setText("");
        jTextPubName.setBounds(new Rectangle(181, 102, 195, 21));
        jButtonPublish.setBounds(new Rectangle(173, 229, 99, 25));
        jButtonPublish.setEnabled(false);
        jButtonPublish.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonPublish.setText("Subscribe");
        jButtonPublish.addActionListener(new
                                         CreateSubscription_jButtonPublish_actionAdapter(this));
        jButton2.setBounds(new Rectangle(284, 229, 99, 25));
        jButton2.setFont(new java.awt.Font("Dialog", 1, 12));
        jButton2.setText("Cancel");
        jButton2.addActionListener(new CreateSubscription_jButton2_actionAdapter(this));
        jTextRemotePORT.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextRemotePORT.setText("");
        jTextRemotePORT.setBounds(new Rectangle(181, 155, 195, 21));
        jTextRemotePORT.setDocument(new NumericDocument());
        jTextRemoteSystemName.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextRemoteSystemName.setText("");
        jTextRemoteSystemName.setBounds(new Rectangle(181, 129, 195, 21));
        jLabel3.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel3.setMinimumSize(new Dimension(270, 16));
        jLabel3.setPreferredSize(new Dimension(105, 16));
        jLabel3.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel3.setText("Publication Port Number");
        jLabel3.setBounds(new Rectangle(14, 160, 164, 15));
        jLabel5.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel5.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel5.setText("Publication System Name");
        jLabel5.setBounds(new Rectangle(14, 134, 164, 15));
        jLabel7.setBorder(border1);
        jLabel7.setBounds(new Rectangle(7, 66, 380, 124));
        help.setBackground(UIManager.getColor("Button.background"));
        help.setEnabled(false);
        help.setFont(new java.awt.Font("Dialog", 2, 12));
        help.setDoubleBuffered(false);
        help.setPreferredSize(new Dimension(373, 21));
        help.setRequestFocusEnabled(false);
        help.setCaretColor(Color.black);
        help.setDisabledTextColor(Color.black);
        help.setEditable(false);
        help.setMargin(new Insets(3, 3, 3, 3));
        help.setSelectedTextColor(Color.white);
        help.setText("Creates subscription corresponding to a publication ");
        help.setBounds(new Rectangle(5, 190, 382, 29));
        getContentPane().add(panel1, BorderLayout.CENTER);
        this.setForeground(Color.darkGray);

        jTextPubName.addFocusListener(this);
        jTextRemotePORT.addFocusListener(this);
        jTextSubName.addFocusListener(this);
        jTextRemoteSystemName.addFocusListener(this);
        jTextRemoteSystemName.addKeyListener(this);
        jTextPubName.addKeyListener(this);
        jTextRemotePORT.addKeyListener(this);
        jTextSubName.addKeyListener(this);

        jTextPubName.grabFocus();
        panel1.add(help, null);
        panel1.add(jLabel4, null);
        panel1.add(jLabel5, null);
        panel1.add(jLabel3, null);
        panel1.add(jLabel2, null);
        panel1.add(jLabel7, null);
        panel1.add(jButton2, null);
        panel1.add(jButtonPublish, null);
        panel1.add(jTextRemotePORT, null);
        panel1.add(jTextRemoteSystemName, null);
        panel1.add(jTextSubName, null);
        panel1.add(jTextPubName, null);
        panel1.add(jLabel1, null);
    }

    void jButton2_actionPerformed(ActionEvent e)
    {
        hide();
    }

    void createSubscription()
    {
        try
        {
            String subName = jTextSubName.getText().trim();
            if (subName == null || subName.equalsIgnoreCase("") ||
                subName.startsWith("\"") || subName.startsWith("\\") ||
                subName.startsWith("/") || subName.startsWith(":") ||
                subName.startsWith("<") || subName.startsWith(">") ||
                subName.startsWith("|") || subName.startsWith("?"))
            {
                throw new RepException("REP093", new Object[]
                                       {null});
            }
            String pubName = jTextPubName.getText().trim();
            if (pubName == null || subName.equalsIgnoreCase("") ||
                pubName.startsWith("\"") || pubName.startsWith("\\") ||
                pubName.startsWith("/") || pubName.startsWith(":") ||
                pubName.startsWith("<") || pubName.startsWith(">") ||
                pubName.startsWith("|") || pubName.startsWith("?"))
            {
                throw new RepException("REP092", new Object[]
                                       {null});
            }
            _Subscription sub = repServer.createSubscription(subName, pubName);
            String port = jTextRemotePORT.getText().trim();
            if (port == null || port.equalsIgnoreCase(""))
            {
                throw new RepException("REP094", new Object[]
                                       {null});
            }
            int remotePORTno = Integer.valueOf(port).intValue();
            if (remotePORTno <= 0)
            {
                throw new NumberFormatException();
            }
            String remoteSystemName = jTextRemoteSystemName.getText();
            sub.setRemoteServerPortNo(remotePORTno);
            sub.setRemoteServerUrl(remoteSystemName);
            sub.subscribe();
            JOptionPane.showMessageDialog(this, "Subscription Created Successfully",
                                          "Success", JOptionPane.INFORMATION_MESSAGE);
            setSubName(subName);
            createSubNode(pubName);
            hide();
        }
        catch (RepException ex1)
        {
            JOptionPane.showMessageDialog(this, ex1, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
        catch (NumberFormatException ex2)
        {
            JOptionPane.showMessageDialog(this,
                                          "Enter only positive integer value in Server Port No",
                                          "Error Message", JOptionPane.ERROR_MESSAGE);
            return;
        }
        catch (Exception ex)
        {
            JOptionPane.showMessageDialog(this, ex, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
    }

    void jButtonPublish_actionPerformed(ActionEvent e)
    {
        createSubscription();
    }

    public void setSubName(String subName0)
    {
        subName = subName0;
    }

    public String getSubName()
    {
        return subName;
    }

    public void createSubNode(String pubName)
    {
        node = new DefaultMutableTreeNode(subName);
        DefaultMutableTreeNode pubNode = new DefaultMutableTreeNode("PUBLICATION");
        DefaultMutableTreeNode leafPubNode = new DefaultMutableTreeNode(pubName);
        leafPubNode.setAllowsChildren(false);
        pubNode.add(leafPubNode);
        node.add(pubNode);
//    adding node to subRootNode
        defaultTreeModel.insertNodeInto(node, subRootNode,
                                        subRootNode.getChildCount());
    }

    public void focusGained(FocusEvent fe)
    {
        if ( ( (JTextField) fe.getSource()).equals(jTextRemoteSystemName))
        {
            help.setText("Enter Remote System Name in this Box");
        }
        if ( ( (JTextField) fe.getSource()).equals(jTextPubName))
        {
            help.setText("Enter Publication Name in this Box");
        }
        if ( ( (JTextField) fe.getSource()).equals(jTextSubName))
        {
            help.setText("Enter Subscription Name in this Box");
        }
        if ( ( (JTextField) fe.getSource()).equals(jTextRemotePORT))
        {
            help.setText("Enter Port Number in this Box");
        }
    }

    public void focusLost(FocusEvent focusEvent)
    {
        jButtonPublish.setEnabled(true);
        if (jTextPubName.getText().equals("") ||
            jTextRemotePORT.getText().equals("") ||
            jTextSubName.getText().equals("") ||
            jTextRemoteSystemName.getText().equals(""))
        {
            jButtonPublish.setEnabled(false);
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
        jButtonPublish.setEnabled(true);
        if (jTextPubName.getText().equals("") ||
            jTextRemotePORT.getText().equals("") ||
            jTextSubName.getText().equals("") ||
            jTextRemoteSystemName.getText().equals(""))
        {
            jButtonPublish.setEnabled(false);
        }
        else
        {
            if (keyEvent.getKeyCode() == KeyEvent.VK_ENTER)
            {
                jButtonPublish_actionPerformed(null);
            }
        }
    }
}

class CreateSubscription_jButton2_actionAdapter implements java.awt.event.ActionListener
{
    CreateSubscription adaptee;

    CreateSubscription_jButton2_actionAdapter(CreateSubscription adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButton2_actionPerformed(e);
    }
}

class CreateSubscription_jButtonPublish_actionAdapter implements java.awt.event.ActionListener
{
    CreateSubscription adaptee;

    CreateSubscription_jButtonPublish_actionAdapter(CreateSubscription adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonPublish_actionPerformed(e);
    }
}
