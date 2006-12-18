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

public class StepsForm extends JDialog implements FocusListener
{
    JPanel jPanel1 = new JPanel();
    JLabel step1 = new JLabel();
    JLabel step2 = new JLabel();
    JLabel step3 = new JLabel();
    JLabel step4 = new JLabel();
    JEditorPane description_pane = new JEditorPane();
    JLabel jLabel1 = new JLabel();
    JLabel jLabel2 = new JLabel();
    Border border1;
    public static void main(String[] args)
    {
        StepsForm stepsForm1 = new StepsForm();
    }

    public StepsForm()
    {
        try
        {
            jbInit();
        }
        catch (Exception e)
        {
            JOptionPane.showMessageDialog(this, e, "Error Message",
                                          JOptionPane.ERROR_MESSAGE);
            return;
        }
    }

    private void jbInit() throws Exception
    {
        border1 = BorderFactory.createLineBorder(SystemColor.controlText, 1);
        this.getContentPane().setLayout(null);
        jPanel1.setBounds(new Rectangle( -2, 0, 400, 300));
        jPanel1.setLayout(null);
        step1.setFont(new java.awt.Font("Dialog", 0, 12));
        step1.setDoubleBuffered(false);
        step1.setDisabledIcon(null);
        step1.setHorizontalTextPosition(SwingConstants.TRAILING);
        step1.setIcon(new ImageIcon(StepsForm.class.getResource("help.png")));
        step1.setIconTextGap(4);
        step1.setText("1. Publication Name to be created / Set conflict resolver");
        step1.setBounds(new Rectangle(38, 44, 335, 29));
        step2.setBounds(new Rectangle(36, 76, 335, 29));
        step2.setText("2. Select tables from datasource to be published");
        step2.setFont(new java.awt.Font("Dialog", 0, 12));
        step2.setDoubleBuffered(false);
        step2.setDisabledIcon(null);
        step2.setHorizontalTextPosition(SwingConstants.TRAILING);
        step2.setIcon(new ImageIcon(StepsForm.class.getResource("help.png")));
        step2.setIconTextGap(4);
        step3.setBounds(new Rectangle(36, 105, 335, 29));
        step3.setText("3. Set Filter Clause for respective tables");
        step3.setFont(new java.awt.Font("Dialog", 0, 12));
        step3.setDoubleBuffered(false);
        step3.setDisabledIcon(null);
        step3.setHorizontalTextPosition(SwingConstants.TRAILING);
        step3.setIcon(new ImageIcon(StepsForm.class.getResource("help.png")));
        step3.setIconTextGap(4);
        step4.setBounds(new Rectangle(34, 135, 335, 29));
        step4.setText("4. Published");
        step4.setFont(new java.awt.Font("Dialog", 0, 12));
        step4.setAlignmentX( (float) 0.0);
        step4.setDoubleBuffered(false);
        step4.setDisabledIcon(null);
        step4.setHorizontalTextPosition(SwingConstants.TRAILING);
        step4.setIcon(new ImageIcon(StepsForm.class.getResource("help.png")));
        step4.setIconTextGap(4);
        description_pane.setBackground(SystemColor.controlHighlight);
        description_pane.setFont(new java.awt.Font("Dialog", 0, 12));
        description_pane.setEditable(false);
        description_pane.setText("Description");
        description_pane.setBounds(new Rectangle(26, 206, 357, 79));
        jLabel1.setFont(new java.awt.Font("Serif", 3, 20));
        jLabel1.setForeground(SystemColor.infoText);
        jLabel1.setHorizontalAlignment(SwingConstants.CENTER);
        jLabel1.setIconTextGap(4);
        jLabel1.setText("Steps");
        jLabel1.setBounds(new Rectangle(49, 6, 273, 20));
        jLabel2.setBorder(border1);
        jLabel2.setText("");
        jLabel2.setBounds(new Rectangle(15, 38, 364, 137));
        this.getContentPane().add(jPanel1, null);
        jPanel1.add(step2, null);
        jPanel1.add(step1, null);
        jPanel1.add(step4, null);
        jPanel1.add(step3, null);
        jPanel1.add(description_pane, null);
        jPanel1.add(jLabel1, null);
        jPanel1.add(jLabel2, null);
    }

    public void focusGained(FocusEvent focusEvent)
    {

    }

    public void focusLost(FocusEvent focusEvent)
    {
    }
}
