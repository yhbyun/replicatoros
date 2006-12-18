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

public class CreatePublication extends JDialog implements FocusListener, KeyListener, ItemListener
{
    JPanel panel1 = new JPanel();
    JLabel jLabel1 = new JLabel();
    JLabel jLabel2 = new JLabel();
    JLabel jLabel3 = new JLabel();
    JTextField jTextPubName = new JTextField();
    JComboBox jComboBoxConflictResolver = new JComboBox();
    _ReplicationServer repServer;
    String pubName;
    DefaultMutableTreeNode pubRootNode;
    DefaultTreeModel defaultTreeModel;
    JButton jButtonNext = new JButton();
    JButton jButtonfinish = new JButton();
    JButton jButtonCancle = new JButton();
    JLabel jLabel4 = new JLabel();
    Border border1;
    JEditorPane help = new JEditorPane();

    public CreatePublication(Frame frame, String title, boolean modal)
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

    public CreatePublication(_ReplicationServer repServer0,
                             DefaultMutableTreeNode pubRootNode,
                             DefaultTreeModel defaultTreeModel0)
{
        this(StartRepServer.getMainFrame(), "Create Publication", true);
        repServer = repServer0;
        this.pubRootNode = pubRootNode;
        defaultTreeModel = defaultTreeModel0;
    }

    private void jbInit() throws Exception
    {

        this.setResizable(false);
        border1 = new EtchedBorder(EtchedBorder.RAISED, Color.white,
                                   new Color(148, 145, 140));
        panel1.setLayout(null);
        panel1.setDebugGraphicsOptions(0);
        jLabel1.setFont(new java.awt.Font("Serif", 3, 25));
        jLabel1.setForeground(SystemColor.infoText);
        jLabel1.setBorder(null);
        jLabel1.setText("      Create Publication");
        jLabel1.setBounds(new Rectangle(55, 9, 254, 33));
        jLabel2.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel2.setRequestFocusEnabled(true);
        jLabel2.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel2.setText("Publication Name");
        jLabel2.setBounds(new Rectangle(27, 82, 121, 15));
        jLabel3.setFont(new java.awt.Font("Dialog", 1, 13));
        jLabel3.setOpaque(false);
        jLabel3.setRequestFocusEnabled(true);
        jLabel3.setHorizontalAlignment(SwingConstants.LEFT);
        jLabel3.setText("Conflict Resolver");
        jLabel3.setBounds(new Rectangle(26, 108, 119, 15));
        jTextPubName.setFont(new java.awt.Font("Dialog", 0, 12));
        jTextPubName.setRequestFocusEnabled(true);
        jTextPubName.setToolTipText("");
        jTextPubName.setText("");
        jTextPubName.setScrollOffset(1);
        jTextPubName.setBounds(new Rectangle(152, 79, 202, 21));
        jComboBoxConflictResolver.setFont(new java.awt.Font("Dialog", 0, 12));
        jComboBoxConflictResolver.setDebugGraphicsOptions(0);
        jComboBoxConflictResolver.setRequestFocusEnabled(true);
        jComboBoxConflictResolver.setToolTipText("");
        jComboBoxConflictResolver.setBounds(new Rectangle(152, 104, 202, 21));
        jComboBoxConflictResolver.addItem(_Publication.publisher_wins);
        jComboBoxConflictResolver.addItem(_Publication.subscriber_wins);
        jButtonNext.setBounds(new Rectangle(106, 187, 81, 26));
        jButtonNext.setEnabled(false);
        jButtonNext.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonNext.setText("Next>");
        jButtonfinish.setBounds(new Rectangle(196, 187, 81, 26));
        jButtonfinish.setEnabled(false);
        jButtonfinish.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonfinish.setText("Finish");
        jButtonCancle.setBounds(new Rectangle(285, 187, 81, 26));
        jButtonCancle.setFont(new java.awt.Font("Dialog", 1, 12));
        jButtonCancle.setSelected(false);
        jButtonCancle.setText("Cancel");
        jButtonCancle.addActionListener(new
                                        CreatePublication_jButtonCancle_actionAdapter(this));
        this.setForeground(Color.darkGray);

        jTextPubName.addFocusListener(this);
        jComboBoxConflictResolver.addItemListener(this);
        jComboBoxConflictResolver.addFocusListener(this);

        jTextPubName.addKeyListener(this);

        jTextPubName.grabFocus();
        jLabel4.setBorder(border1);
        jLabel4.setMaximumSize(new Dimension(0, 0));
        jLabel4.setMinimumSize(new Dimension(2, 2));
        jLabel4.setOpaque(false);
        jLabel4.setRequestFocusEnabled(false);
        jLabel4.setText("");
        jLabel4.setBounds(new Rectangle(13, 59, 353, 79));
        help.setBackground(UIManager.getColor("Button.background"));
        help.setEnabled(false);
        help.setFont(new java.awt.Font("Dialog", 2, 12));
        help.setOpaque(true);
        help.setRequestFocusEnabled(false);
        help.setDisabledTextColor(Color.black);
        help.setEditable(false);
        help.setText(
            "Creates Publication with default conflict resolver set to        " +
            "          publisher_wins");
        help.setBounds(new Rectangle(21, 141, 349, 35));
        panel1.add(jButtonfinish, null);
        panel1.add(jComboBoxConflictResolver, null);
        panel1.add(jTextPubName, null);
        panel1.add(jLabel2, null);
        panel1.add(jLabel3, null);
        panel1.add(jLabel4, null);
        panel1.add(help, null);
        panel1.add(jButtonNext, null);
        panel1.add(jButtonCancle, null);
        panel1.add(jLabel1, null);
        this.getContentPane().add(panel1, BorderLayout.CENTER);
        jButtonNext.addActionListener(new
                                      CreatePublication_jButtonNext_actionAdapter(this));
    }

    void jButton2_actionPerformed(ActionEvent e)
    {
        hide();
    }

    public void setPubName(String pubName0)
    {
        pubName = pubName0;
    }

    public String getPubName()
    {
        return pubName;
    }

    void jButtonNext_actionPerformed(ActionEvent e)
    {

        String pubName = jTextPubName.getText().trim();
        try {
            if (pubName == null || pubName.equalsIgnoreCase("") ||
                pubName.startsWith("\"") || pubName.startsWith("\\") ||
                pubName.startsWith("/") || pubName.startsWith(":") ||
                pubName.startsWith("<") || pubName.startsWith(">") ||
              pubName.startsWith("|") || pubName.startsWith("?")) {
            throw new RepException("REP092", new Object[] {null});
            }
        }
        catch (Exception ex) {
          JOptionPane.showMessageDialog(this,
                                        "Publication Name is not  Specified",
                                          "Error Message",
                                          JOptionPane.INFORMATION_MESSAGE);
            return;
        }

        String conflictResolver = (String) jComboBoxConflictResolver.
            getSelectedItem();
        SelectTable st = new SelectTable(defaultTreeModel, pubRootNode, pubName,
                                         repServer, conflictResolver, this,
                                         RepConstants.create_Publication);
        Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension frameSize = new Dimension(440, 325);
        st.setBounds( (screenSize.width - frameSize.width) / 2,
                     (screenSize.height - frameSize.height) / 2, 535, 325);
        hide();
        st.show();
    }

    void jButtonCancle_actionPerformed(ActionEvent e)
    {
        hide();
    }

    public void keyTyped(KeyEvent keyEvent)
    {
    }

    public void keyPressed(KeyEvent keyEvent)
    {
    }

    public void keyReleased(KeyEvent keyEvent)
    {
        jButtonNext.setEnabled(true);
        if (jComboBoxConflictResolver.getItemCount() > 0)
        {
            if (jComboBoxConflictResolver.getSelectedItem().equals("") ||
                jTextPubName.getText().equals(""))
            {
                jButtonNext.setEnabled(false);
            }
            else
            {
                if (keyEvent.getKeyCode() == KeyEvent.VK_ENTER)
                {
                    jButtonNext_actionPerformed(null);
                }
            }
        }
    }

    public void focusGained(FocusEvent fe)
    {
        if (fe.getSource() instanceof JTextField)
        {
            help.setText("Enter Publication Name in this Box");

        }
        if (fe.getSource() instanceof JComboBox)
        {
            help.setText("Select Conflict Resolver from this Combo Box");
        }
    }

    public void focusLost(FocusEvent focusEvent)
    {
        jButtonNext.setEnabled(true);
        if (jComboBoxConflictResolver.getItemCount() > 0)
        {
            if (jComboBoxConflictResolver.getSelectedItem().equals("") ||
                jTextPubName.getText().equals(""))
            {
                jButtonNext.setEnabled(false);
            }
        }

    }

    public void itemStateChanged(ItemEvent itemEvent)
    {
        if ( ( (JComboBox) itemEvent.getSource()).equals(jComboBoxConflictResolver))
        {
            help.setText("Select Conflict Resolver from this Combo Box");
        }

    }
}

class CreatePublication_jButtonNext_actionAdapter implements java.awt.event.ActionListener
{
    CreatePublication adaptee;

    CreatePublication_jButtonNext_actionAdapter(CreatePublication adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonNext_actionPerformed(e);
    }
}

class CreatePublication_jButtonCancle_actionAdapter implements java.awt.event.ActionListener
{
    CreatePublication adaptee;

    CreatePublication_jButtonCancle_actionAdapter(CreatePublication adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButtonCancle_actionPerformed(e);
    }
}
