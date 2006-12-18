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

public class AboutReplicator extends JDialog
{

    static boolean packFrame = false;

    public AboutReplicator(JFrame frame)
    {
        super(frame, "Daffodil Replicator Help", true);
        ImageIcon img = new ImageIcon( (getClass().getResource(
            "/icons/aboutreplicator.gif")));
        JPanel pan = new JPanel();
//      pan.setBorder(new LineBorder(Color.BLACK));
        JLabel b = new JLabel(img);
        pan.add(b, BorderLayout.CENTER);
        pan.setBackground(Color.WHITE);
        setBounds( (int) (Toolkit.getDefaultToolkit().getScreenSize().getWidth() -
                          img.getIconWidth()) / 2,
                  (int) (Toolkit.getDefaultToolkit().getScreenSize().getHeight() -
                         img.getIconHeight()) / 2, img.getIconWidth() + 2,
                  img.getIconHeight() + 70);
        getContentPane().add(pan, BorderLayout.CENTER);
        JButton jButton1 = new JButton();
        //350,250
        jButton1.setBounds( (int) (Toolkit.getDefaultToolkit().getScreenSize().
                                   getWidth() -
                                   img.getIconWidth()) / 2 + 300,
                           (int) (Toolkit.getDefaultToolkit().getScreenSize().
                                  getHeight() -
                                  img.getIconHeight()) / 2 + 200 + 10, 80, 80);
        jButton1.setDebugGraphicsOptions(0);
        jButton1.setText("Ok");
        jButton1.addActionListener(new AboutReplicator_jButton1_actionAdapter(this));
        pan.add(jButton1, null);
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

    public void Show()
    {
        super.show();
    }

    public void Dispose()
    {
        super.dispose();
    }

    void jButton1_actionPerformed(ActionEvent e)
    {
        this.dispose();
    }

    private void jbInit() throws Exception
    {
    }
}

class AboutReplicator_jButton1_actionAdapter implements java.awt.event.ActionListener
{
    AboutReplicator adaptee;

    AboutReplicator_jButton1_actionAdapter(AboutReplicator adaptee)
    {
        this.adaptee = adaptee;
    }

    public void actionPerformed(ActionEvent e)
    {
        adaptee.jButton1_actionPerformed(e);
    }
}
