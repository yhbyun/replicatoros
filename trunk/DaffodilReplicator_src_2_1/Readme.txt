
How to run Daffodil Replicator (b270320061742)
===============================

1 - Extract the DaffodilReplicator2_1.zip to an appropriate folder.

2 - Set JAVA_HOME path in setpath.bat / ( For Linux - PubServer.sh / SubServer.sh)

3. Edit setPath.bat / ( For Linux - PubServer.sh / SubServer.sh)

	a. Set JDBC_CLASSPATH value for data source 
	b. Add Replicator.jar and log4j.jar path to JDBC_CLASSPATH
      
(eg. JDBC_CLASSPATH="c:\DaffodilDB\DaffodilDB_Client.jar;Replicator.jar;log4j.jar"

4. Run PubServer.bat/sh to run publication server

5. SubServer.bat/sh to run subscription server


 

Special Characters Handling
===========================
Special characters with ascii value less than 32 are not parse able and create problem in replication operations. This can be fixed by using EncodeConfig.ini file. It is necessary to enter the column name (those having special characters) in EncodeConfig.ini file against the respective table name. If number of columns is more than one, should be separated by commas as listed below:

 CMSADM2.CMS_USERS=PWD1,PWD2

Here "CMSADM2.CMS_USERS" is table name and "PWD1","PWD1" are the column names that have specail characters.

Shadow Table, Sequence and Index prefix
==================================================
 
User can change the pre-fixes of Shadow Table,Sequence and indexes using config.ini 
file. The prefix values can be changed accordingly for each of the respective schema objects as given below.
 
 ShadowTablePrefix=R_S_

Here "R_S_" is a prefix for shadow table name, this can be modified according to the requirement.
                 
   
 
Set Replication Home 
=====================

All the Replicator directory files that includes error log file and transaction log file are created at Replication Home. In the current Replicator version we provide the provision to set Replication home at desired / user specific direcorty. For this edit the config.ini file and provide the required path information against Replication Home variable i.e. 'REPLICATIONHOME'.
By default, Replication Home is set to user home.


Transaction log file properties
================================

Each time the synchronization operation (Synchronization,Pull and Push) is executed a transaction log file will be created in Replication Home set both on Publisher and Subscriber data source. Transaction log file stores the statistical or detailed information regarding the data changes reported (i.e.Update, Insert or Delete) on the subscriber data source. 

To get the statistical information (operation count) along with the status of synchronization operation set transaction log file variable 'TRANSACTIONDETAIL' as 'false'. 

Altenatively, set transaction log file variable as 'true'. This will enable Replicator to provide detailed information reporting the Updation, insertion or deletion of the records on subscriber data source. 



Important Note: 
---------------

1 - Use Java(TM) 2 SDK, Standard Edition Version 1.4 or above.

2 - Backward Compatibilty :  For users who are already working with older versions of Daffodil Replicator need to update the version of daffodil Replicator using updateversion.bat. Before running the updateversion.bat file need to update the required information in config.ini like databae vendor type, driver, URL, user and password.You require to run the  updateversion.bat file two time.One time for publisher and second time for subscriber after updating database vendor name,database Name, driver, URL, user and password.


Complete information along with some special cases is also discussed in user manual for Daffodil Replicator Developers Guide.


You can send your technical queries at support@daffodildb.com or you can join our support forum at: 
http://www.daffodildb.com/forum
