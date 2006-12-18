@echo off
@Rem   To update the Replicator.jar
@Rem   Copyright (c) 2004-2005 The Daffodi Software Ltd.  All rights reserved.

@rem echo "%JAVA_HOME%"

if not "%JAVA_HOME%" == "" goto cont1
echo ERROR: Set the JAVA_HOME before running this script.
goto END

:cont1
%JAVA_HOME%/bin/jar -uvf .\Replicator.jar -C . src\icons

:END