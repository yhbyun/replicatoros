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

package com.daffodilwoods.replication;

import java.text.*;
import java.util.*;

/**
 * This is the replicator's exception class, It extends Exception class.
 * When some repException occurs it's object is returned. This class
 * overrides some standard exception methods.
 *
 */

public class RepException extends Exception
{

    private String repCode;
    private Object[] params;

  public RepException(String repCode0, Object[] params0) {
        repCode = repCode0;
        params = params0;
    }

    public String getMessage()
    {
        return getMessage(null);
    }

    public String getMessage(Locale locale)
    {
        ResourceBundle manager = null;
        if (locale != null)
        {
            manager = ResourceBundle.getBundle("com.daffodilwoods.replication.RepCode", locale);
        }
        else
        {
            manager = ResourceBundle.getBundle("com.daffodilwoods.replication.RepCode");
        }
        try
        {
            MessageFormat mf = new MessageFormat(manager.getString(repCode));
            return "\n\t" + mf.format(getParametersAsString());
        }
        catch (Exception ex)
        {
            return "\n\t" + "Code \"" + repCode + "\" is not defined";
        }
    }

  private String[] getParametersAsString() {
    if (params == null) {
            return null;
        }
        int length = params.length;
        String[] str = new String[length];
    for (int i = 0; i < length; i++) {
            String temp = "" + params[i];
            temp = temp.trim();
            if (temp.startsWith("the keyword"))
            {
                String asd = temp.substring(12);
                asd = asd.trim();
                str[i] = "the keyword '" + asd + "'";
            }
            else
            {

                str[i] = "'" + temp + "'";
            }
        }
        return str;
    }

  public String getRepCode() {
        return repCode;
    }

  public void SetStackTrace(RepException ex) {
        setStackTrace(ex.getStackTrace());
    }
}
