Daffodil Replicator (OS) is an open source Java tool for data integration, data migration and data protection in real time. Daffodil Replicator performs data replication based on the 'Publish and Subscribe' model (in client and server architecture). A publication is a collection of one or more tables required for data replication. A subscription is a copy of tables involved in publication on client side.

Replicator supports bi-directional data replication by either capturing a data source snapshot or synchronizing the changes. It monitors  data changes in the tables involved and synchronizes all data changes made by the subscriber and the publisher on periodic basis or on-demand by the subscriber. While synchronizing with one or more target data source. Replicator uses pre-defined conflict resolution algorithms to resolve conflicts between publisher and subscriber. The publications and subscriptions are defined using GUI or APIs on existing database servers.
more>>


Features

  * Bi-directional Data Synchronization – Replicator does not put any constraint on changing data in databases. Changes can be done to any/all databases involved in replication job and data integrity will be maintained.
  * 


  * Supports replication across heterogeneous database – Replicator supports replication across any combination of supported databases. Data source on one end could be daffodil db and SQL Server could be on other end. In the same job different data sources could be used in any combination.
  * 


  * Conflict detector and resolution – Daffodil Replicator offers very strong conflict detection and resolution functionalities that allow multiple users to send in changes from the workstations simultaneously, while ensuring the integrity of the data. "paste a link to conflict resolution document".
  * 


  * Partial data (Tables, Rows and column) Replication – A user can ignore the columns that he does not want to replicate. Columns that do not allow insert, update and delete operations, create problem in data replication. Can ignore the tables he don't want to replicate or can also add filter to data source for replication of required filtered rows.
  * 


  * Large data type support – Replicator supports almost all data types of supported databases. It also supports BLOB and CLOB replication.