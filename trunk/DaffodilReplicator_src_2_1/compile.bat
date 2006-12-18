@echo off
@Rem   To Bulid the Daffodil Replicator source and make the Replcator.jar
@Rem   Copyright (c) 2004-2005 The Daffodi Software Ltd.  All rights reserved.

if not "%ANT_HOME%"=="" goto cont1
echo ERROR: Set the ANT_Home before running this script.
goto END

:cont1
if EXIST %ANT_HOME%/bin/ant.bat goto cont2
echo ERROR: ANT.bat File Not Found.
goto END

:cont2
@rem echo "%JAVA_HOME%"
if not "%JAVA_HOME%" == "" goto cont3
echo ERROR: Set the JAVA_HOME before running this script.
goto END


:cont3
%ANT_HOME%/bin/ant -buildfile .\build.xml makeJAR

:END