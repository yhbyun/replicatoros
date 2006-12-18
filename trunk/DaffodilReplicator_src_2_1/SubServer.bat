@echo off
@REM ---------------------------------------------------------
@REM --
@REM -- Copyright (c) 2004-2005 The Daffodi Software Ltd.  All rights reserved.
@REM --
@REM -- This batch file is an example of how to start the Daffodil Replicator server 
@REM -- 
@REM -- REQUIREMENTS: 
@REM --	 To Start the Daffodil Replicator 
@REM --  You must have the data source jar files as well as the 
@REM --  Daffodil Replicator jar in your classpath.
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

title Subscription Server

%JAVA_HOME%\bin\java -classpath %JDBC_classpath% com.daffodilwoods.repconsole.StartServer subserver


:end