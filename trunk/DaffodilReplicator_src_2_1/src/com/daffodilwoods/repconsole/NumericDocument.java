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

import javax.swing.text.*;

public class NumericDocument extends PlainDocument
{

    public void insertString(int off, String str, AttributeSet attr)
    {
        try
        {
            char strChar = str.charAt(0);
            String txt = getText(0, getLength());
            if ( (strChar >= '0' && strChar <= '9') ||
                ( (strChar == 'I') &&
                 ( (str.equals("Infinity")) || (str.equals("Invalid Input")))))
            {
                if (str.equals(".") && txt.indexOf(".") >= 0)
                {
                    return;
                }
                super.insertString(off, str, attr);
            }
            else
            {
                return;
            }
        }
        catch (Exception e)
        {}
    }

    public void remove(int off, int len)
    {
        try
        {
            super.remove(off, len);
            String str = getText(0, getLength());
        }
        catch (Exception e)
        {}
    }
}
