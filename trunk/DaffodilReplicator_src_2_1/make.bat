@echo off
@Rem   To make the Replicator.jar
@REM   Copyright (c) 2004-2005 The Daffodi Software Ltd.  All rights reserved.

call setPath.bat

if EXIST .\compile.bat goto compileation
echo ERROR: compile.bat File Not Found
goto end

:compileation
call compile.bat

pause

if EXIST .\update.bat goto update
echo ERROR: update.bat File Not Found
goto end

:update
call update.bat

:end