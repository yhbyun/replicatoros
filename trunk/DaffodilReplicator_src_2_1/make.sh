# ---------------------------------------------------------------------------------
# --  To Bulid the Daffodil Replicator source and make the Replcator.jar
# --  Copyright (c) 2004-2005 The Daffodi Software Ltd.  All rights reserved.
# ---------------------------------------------------------------------------------

#Setting  required environmental variable

export JAVA_HOME="/usr/java/j2sdk1.4.1_04"
export ANT_HOME="/opt/apache-ant-1.5.4"

if test -z "$JAVA_HOME"  ;then 
   echo " JAVA_HOME is not set ";
   exit;
else 
   echo "JAVA_HOME : $JAVA_HOME";
fi

if test -z "$ANT_HOME"  ;then 
   echo " ANT_HOME is not set ";
   exit;
else 
   echo "ANT_HOME : $ANT_HOME";
fi

#Building Replicator.jar

"$ANT_HOME/bin/ant"  makeJAR

#Updating Replicator.jar

"$JAVA_HOME/bin/jar"  -uvf ./Replicator.jar -C . src/icons
