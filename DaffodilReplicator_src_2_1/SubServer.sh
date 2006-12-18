# ---------------------------------------------------------------------------------
# -- Copyright (c) 2004-2005 The Daffodi Software Ltd.  All rights reserved.
# -- This file is an example of how to start the Daffodil Replicator server 
# -- REQUIREMENTS: 
# --	 To Start the Daffodil Replicator 
# --  You must have the data source jar files as well as the 
# --  Daffodil Replicator jar in your classpath.
# --  
# -- This file for use on Linux systems

# ---------------------------------------------------------------------------------
# -- Set Java Home Environment variable
# ---------------------------------------------------------------------------------

JAVA_HOME=""
if [ -z $JAVA_HOME ] 
then 
  echo "JAVA_HOME environment variable is not set"
  exit
else 
  echo "USED JAVA = $JAVA_HOME"
fi

# ---------------------------------------------------------------------------------
# -- Set JDBC class path for data source used along with replicator.jar; 
# ---------------------------------------------------------------------------------

JDBC_CLASSPATH='./Replicator.jar'

echo "JDBC_CLASSPATH $JDBC_CLASSPATH"

exec "$JAVA_HOME/bin/java" -classpath "$JDBC_CLASSPATH"  com.daffodilwoods.repconsole.StartServer subserver  
