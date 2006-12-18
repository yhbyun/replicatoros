@echo off
@REM ---------------------------------------------------------
@REM --
@REM -- Copyright (c) 2004-2005 The Daffodi Software Ltd.  All rights reserved.
@REM --
@REM -- This batch file is used to update the version of Daffodil Replicator.
@REM -- 
@REM -- REQUIREMENTS: 
@REM --	 Old version of daffodil Replicator is not compatible with latest vertsion 2.0 
@REM --  because structure of system table has been changed.If any user already working 
@REM --  with old version of Daffodil Replicator and want to get the benefit of latest 
@REM --  version then he require to update the configuration
@REM --  
@REM -- This file for use on Windows systems
@REM ---------------------------------------------------------

call setpath.bat

@if not "%JAVA_HOME%" == "" goto cont1 

echo  Warning : JAVA_HOME environment variable is not set.

pause

@goto end

:cont1

@REM ---------------------------------------------------------
@REM -- start Daffodil Replicator server
@REM ---------------------------------------------------------

title Update Version

%JAVA_HOME%\bin\java -classpath %JDBC_classpath% com.daffodilwoods.replication.UpdatePreviousVersion 


:end