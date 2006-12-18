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

import com.daffodilwoods.replication.*;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: </p>
 * @author not attributable
 * @version 1.0
 */

public class UpdateSubscription extends JDialog implements FocusListener, KeyListener
{
    JPanel panel1 = new JPanel();
    JLabel jLabel1 = new JLabel();
    JTextField jTextSubName = new JTextField();
    JButton jButtonUpdate = new JButton();
    JButton jButtonCancle = new JButton();
    _ReplicationServer repServer;
    JLabel jLabel2 = new JLabel();
    JLabel jLabel4 = new JLabel();
    JTextField jTextRemoteServerName = new JTextField();
    JTextField jTextRemoteRepPortNo = new JTextField();
    JLabel jLabel3 = new JLabel();
    JLabel jLabel6 = new JLabel();
    JEditorPane help = new JEditorPane();
    _Subscription sub = null;
    String subName;
    boolean able = true;

    TitledBorder titledBorder1;
    Border border1;

    public UpdateSubscription(Frame frame, String title, boolean modal)
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

    public UpdateSubscription(Frame frame, String title, boolean modal, boolean able0)
    {
        super(frame, title, modal);
        able = able0;
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


    public UpdateSubscription(_ReplicationServer repServer0, boolean able0)
    {
        this(StartRepServer.getMainFrame(), "Update Subscription", true, able0);
        repServer = repServer0;
        able = able0;
    }

    public UpdateSubscription(_ReplicationServer repServer0, String subName0,
                       boolean able0)
    {
        this(null, "Update Subscription", true, able0);
        repServer = repServer0;
        subName = subName0;
        able = able0;
        init();
    }

    private void init()
    {
        try
        {
            sub = repServer.getSubscription(subName);
            jTextSubName.setText(subName);
            jTextSubName.setEnabled(able);
            jTextRemoteRepPortNo.setText("" + sub.getRemoteServerPortNo());
            jTextRemoteServerName.setText(sub.getRemoteServerUrl());
            if (!jTextRemoteRepPortNo.getText().trim().equalsIgnoreCase("") &&
                !jTextRemoteServerName.getText().equalsIgnoreCase(""))
            {
                jButtonUpdate.setEnabled(true);
            }
        }
        catch (RepException ex)
        {
            JOptionPane.showMessageDialog(this, ex, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
//      ex.printStackTrace();
            return;
        }
    }

    private void jbInit() throws Exception
    {
        titledBorder1 = new TitledBorder("");
        border1 = new EtchedBorder(EtchedBorder.RAISED, Color.white,
                                   new Color(148, 145, 140));
        panel1.setLayout(null);
        jLabel1.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel1.setPreferredSize(new Dimension(182, 16));
        jLabel1.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel1.setText("Subscription Name");
        jLabel1.setBounds(new Rectangle(23, 72, 157, 23));
        panel1.setAlignmentY( (float) 0.5);
        panel1.setDebugGraphicsOptions(0);
        jTextSubName.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextSubName.setMinimumSize(new Dimension(6, 22));
        jTextSubName.setFocusAccelerator('0');
        jTextSubName.setText("");
        jTextSubName.setHorizontalAlignment(SwingConstants.LEFT);
        jTextSubName.setBounds(new Rectangle(190, 73, 178, 21));
        jButtonUpdate.setBounds(new Rectangle(106, 229, 131, 25));
        jButtonUpdate.setEnabled(false);
        jButtonUpdate.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonUpdate.setHorizontalAlignment(SwingConstants.CENTER);
        jButtonUpdate.setText("Update");
        jButtonUpdate.addActionListener(new
                                             UpdateSubscription_jButtonUpdate_actionAdapter(this));
        jButtonCancle.setBounds(new Rectangle(249, 229, 131, 25));
        jButtonCancle.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonCancle.setHorizontalAlignment(SwingConstants.CENTER);
        jButtonCancle.setText("Cancel");
        jButtonCancle.addActionListener(new UpdateSubscription_jButtonCancle_actionAdapter(this));
        jLabel2.setEnabled(true);
        jLabel2.setFont(new java.awt.Font("Serif", 3, 25));
        jLabel2.setText("Update Subscription");
        jLabel2.setBounds(new Rectangle(88, 8, 218, 36));
        jLabel4.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel4.setRequestFocusEnabled(true);
        jLabel4.setToolTipText("");
        jLabel4.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel4.setText("Publication Server Name");
        jLabel4.setBounds(new Rectangle(22, 102, 163, 23));
        jTextRemoteServerName.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextRemoteServerName.setText("");
        jTextRemoteServerName.setHorizontalAlignment(SwingConstants.LEFT);
        jTextRemoteServerName.setBounds(new Rectangle(190, 102, 178, 21));
        jTextRemoteRepPortNo.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextRemoteRepPortNo.setText("");
        jTextRemoteRepPortNo.setHorizontalAlignment(SwingConstants.LEFT);
        jTextRemoteRepPortNo.setBounds(new Rectangle(190, 130, 178, 21));
        jLabel3.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel3.setRequestFocusEnabled(true);
        jLabel3.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel3.setText("Publication Port Number");
        jLabel3.setBounds(new Rectangle(22, 131, 157, 23));
        jLabel6.setBorder(border1);
        jLabel6.setDebugGraphicsOptions(0);
        jLabel6.setText("");
        jLabel6.setBounds(new Rectangle(12, 56, 369, 111));

        jTextRemoteRepPortNo.setDocument(new NumericDocument());

        jTextRemoteRepPortNo.addFocusListener(this);
        jTextRemoteServerName.addFocusListener(this);
        jTextSubName.addFocusListener(this);
        jTextRemoteRepPortNo.addKeyListener(this);
        jTextRemoteServerName.addKeyListener(this);
        jTextSubName.addKeyListener(this);

        jTextSubName.grabFocus();
        help.setBackground(UIManager.getColor("Button.background"));
        help.setEnabled(false);
        help.setFont(new java.awt.Font("Dialog", 2, 12));
        help.setRequestFocusEnabled(false);
        help.setToolTipText("");
        help.setDisabledTextColor(Color.black);
        help.setEditable(false);
        help.setText(
            "Update subscription...");
        help.setBounds(new Rectangle(10, 169, 363, 34));
        panel1.add(jLabel6, null);
        panel1.add(jTextRemoteRepPortNo, null);
        panel1.add(jTextSubName, null);
        panel1.add(jTextRemoteServerName, null);
        panel1.add(jLabel1, null);
        panel1.add(jLabel3, null);
        panel1.add(jLabel4, null);
        panel1.add(help, null);
        panel1.add(jButtonCancle, null);
        panel1.add(jButtonUpdate, null);
        panel1.add(jLabel2, null);
        this.getContentPane().add(panel1, BorderLayout.CENTER);
    }

    void jButtonCancle_actionPerformed(ActionEvent e)
    {
        hide();
    }

    void jButtonUpdate_actionPerformed(ActionEvent e)
    {
        try
        {
            String subName = jTextSubName.getText().trim();
            if (subName.equalsIgnoreCase(""))
            {
                throw new RepException("REP093", new Object[]
                                       {null});
            }
            String port = jTextRemoteRepPortNo.getText().trim();
            if (port.equalsIgnoreCase(""))
            {
                throw new RepException("REP094", new Object[]
                                       {null});
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
                throw new RepException("REP095", new Object[]
                                       {null});
            }

            sub = repServer.getSubscription(subName);
            if (sub == null)
            {
                throw new RepException("REP058", new Object[]
                                       {subName});
            }
            sub.setRemoteServerPortNo(remoteRepPortNo);
            sub.setRemoteServerUrl(remoteServerName);
            sub.updateSubscription();
            JOptionPane.showMessageDialog(this, "Subscription Updated Successfully",
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
            JOptionPane.showMessageDialog(this,
                                          "Enter valid integer value in Server Port Number",
                                          "Error Message", JOptionPane.ERROR_MESSAGE);
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
        jButtonUpdate.setEnabled(true);
        if (jTextRemoteRepPortNo.getText().equals("") ||
            jTextRemoteServerName.getText().equals("") ||
            jTextSubName.getText().equals("") || jTextSubName.getText().equals(""))
        {
            jButtonUpdate.setEnabled(false);
        }
        else
        {
            if (keyEvent.getKeyCode() == KeyEvent.VK_ENTER)
            {
                jButtonUpdate_actionPerformed(null);
            }
        }

    }

    public void focusGained(FocusEvent fe)
    {
        if ( ( (JTextField) fe.getSource()).equals(jTextRemoteServerName))
        {
            help.setText("Enter Remote Server Name in this Box");
        }
        if ( ( (JTextField) fe.getSource()).equals(jTextSubName))
        {
            help.setText("Enter Subscription Name in this Box");
        }
        if ( ( (JTextField) fe.getSource()).equals(jTextRemoteRepPortNo))
        {
            help.setText("Enter Port Number in this Box");
        }

        if (! (jTextRemoteRepPortNo.getText().equals("") ||
               jTextRemoteServerName.getText().equals("") ||
               jTextSubName.getText().equals("") ||
               jTextSubName.getText().equals("")))
        {
            jButtonUpdate.setEnabled(true);

        }
    }

    public void focusLost(FocusEvent focusEvent)
    {
        jButtonUpdate.setEnabled(true);
        if (jTextRemoteRepPortNo.getText().equals("") ||
            jTextRemoteServerName.getText().equals("") ||
            jTextSubName.getText().equals("") || jTextSubName.getText().equals(""))
        {
            jButtonUpdate.setEnabled(false);

        }
    }
}

class UpdateSubscription_jButtonCancle_actionAdapter implements java.awt.event.ActionListener
{
    UpdateSubscription adaptee;

    UpdateSubscription_jButtonCancle_actionAdapter(UpdateSubscription adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonCancle_actionPerformed(e);
    }
}

class UpdateSubscription_jButtonUpdate_actionAdapter implements java.awt.event.ActionListener
{
    UpdateSubscription adaptee;

    UpdateSubscription_jButtonUpdate_actionAdapter(UpdateSubscription adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonUpdate_actionPerformed(e);
    }
}
