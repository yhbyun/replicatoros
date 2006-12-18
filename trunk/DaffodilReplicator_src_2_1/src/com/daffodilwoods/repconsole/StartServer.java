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
import javax.swing.*;
import javax.swing.border.*;
import com.daffodilwoods.replication.RepException;

/**
 * This is the starter class for replicator. User should run this class to
 * start replication server both on publisher and subscriber end.
 *
 */
public class StartServer extends JWindow
{

    static boolean packFrame = false;

    public StartServer()
    {
        super();
        ImageIcon img = new ImageIcon( (getClass().getResource(
            "/icons/daffodilreplicator.gif")));
        JPanel pan = new JPanel();
        pan.setBorder(new LineBorder(Color.white));
        JLabel b = new JLabel(img);
        pan.add(b, BorderLayout.CENTER);
        pan.setBackground(Color.white);

        pan.setBorder(BorderFactory.createBevelBorder(BevelBorder.RAISED,
            Color.white, Color.GRAY));
        setBounds( (int) (Toolkit.getDefaultToolkit().getScreenSize().getWidth() -
                          img.getIconWidth()) / 2,
                  (int) (Toolkit.getDefaultToolkit().getScreenSize().getHeight() -
                         img.getIconHeight()) / 2, img.getIconWidth() + 2,
                  img.getIconHeight() + 10);
        getContentPane().add(pan, BorderLayout.CENTER);
    }

    public static void main(String[] args) throws RepException {
      if(args.length<1){
        throw new RepException("REP008",null);
      }
        StartServer sw = new StartServer();
        sw.Show();
        try
        {
            Thread.sleep(2000);
            UIManager.setLookAndFeel("javax.swing.plaf.metal.MetalLookAndFeel");
//            StartRepServer frame = new StartRepServer(sw);
            StartRepServer frame = new StartRepServer(sw,args[0]);
            if (packFrame)
            {
                frame.pack();
            }
            else
            {
                frame.validate();
            }
            //Center the window
            Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
            Dimension frameSize = frame.getSize();
            if (frameSize.height > screenSize.height)
            {
                frameSize.height = screenSize.height;
            }
            if (frameSize.width > screenSize.width)
            {
                frameSize.width = screenSize.width;
            }
            frame.setResizable(false);
            frame.setBounds( (screenSize.width - frameSize.width) / 2,
                            (screenSize.height - frameSize.height) / 2, 520, 370);
            frame.setVisible(true);
        }
        catch (Exception ex)
        {
//      ex.printStackTrace();

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
}
