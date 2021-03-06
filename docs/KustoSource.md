# Kusto Source Connector

Kusto source connector allows reading data from a table in the specified Kusto cluster and database 
to a Spark DataFrame 

## Authentication

Kusto connector uses  **Azure Active Directory (AAD)** to authenticate the client application 
that is using it. Please verify the following before using Kusto connector:
 * Client application is registered in AAD
 * Client application has 'viewer' privileges or above on the target database, 
 or 'admin' privileges on the target table (either one is sufficient)
 
 For details on Kusto principal roles, please refer to [Role-based Authorization](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/role-based-authorization) 
 section in [Kusto Documentation](https://docs.microsoft.com/en-us/azure/kusto/).
 
 For managing security roles, please refer to [Security Roles Management](https://docs.microsoft.com/en-us/azure/kusto/management/security-roles) 
 section in [Kusto Documentation](https://docs.microsoft.com/en-us/azure/kusto/).
 
 ## Source: 'read' command
 
 Kusto connector implements Spark 'Datasource V1' API. 
 Kusto data source identifier is "com.microsoft.kusto.spark.datasource". 
 Kusto table schema is translated into a spark as explained in [DataTypes](Spark-Kusto DataTypes mapping.md).
 
 ### Command Syntax
 There are two command flavors for reading from Kusto:
 
 **Simplified Command Syntax**: 
  ```
 <dataframe-name> = 
 spark.read.kusto(<cluster-name>, <database-name>, <kusto-query>, <parameters map>)
  ```
 where:
 * **Kusto-query** is any valid Kusto query. For details, please refer to [Query statements documentation](https://docs.microsoft.com/en-us/azure/kusto/query/statements). To read the whole table, just provide the table name as query.
 >Note:
 Kusto commands  cannot be executed via the connector.
 * **Parameters map** is a set of key-value pairs, where the key is the parameter name. See [Supported Options](#supported-options)
 section for details
 
  
**Elaborate Command Syntax**: 
```
<dataframe-name> = 
sqlContext.read
.format("com.microsoft.kusto.spark.datasource")
.option(KustoOptions.KUSTO_CLUSTER, <cluster-name>)
.option(KustoOptions.KUSTO_DATABASE, <database-name>)
.option(KustoOptions.KUSTO_QUERY, <kusto-query>)
.load()
```
Where **parameters map** is identical for both syntax flavors.
      
 ### Supported Options
  
 **Mandatory Parameters:** 
  
 * **KUSTO_CLUSTER**:
  Kusto cluster from which the data will be read.
  Use either cluster profile name for global clusters, or <profile-name.region> for regional clusters.
  For example: if the cluster URL is 'https://testcluster.eastus.kusto.windows.net', set this property 
  as 'testcluster.eastus' 
   
  * **KUSTO_DATABASE**: 
  Kusto database from which the data will be read. The client must have 'viewer' 
  privileges on this database, unless it has 'admin' privileges on the table.
  
  * **KUSTO_AAD_CLIENT_ID**: 
  AAD application identifier of the client.
  
  * **KUSTO_AAD_AUTHORITY_ID**: 
  AAD authentication authority.
  
  * **KUSTO_AAD_CLIENT_PASSWORD**: 
  AAD application key for the client.
  

  
  * **KUSTO_QUERY**: 
 A flexible Kusto query (can simply be a table name). The schema of the resulting dataframe will match the schema of the query result. 
 
 **Optional Parameters:** 
 
    * **KUSTO_NUM_PARTITIONS**: in current release must be set to one (default).
    
 ### Examples
 
 **Using simplified syntax**
 
 Create a DataFrame based on a query accessing 'MyKustoTable' table
 ```
 val conf: Map[String, String] = Map(
       KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
       KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey
     )
     
 val df = spark.read.kusto(cluster, database, "MyKustoTable | where (ColB % 1000 == 0) | distinct ColA ", conf)
 ``` 
 
 **Using elaborate syntax**
  
  Create a DataFrame by reading all of 'MyKustoTable' table
  ```
 val conf: Map[String, String] = Map(
       KustoOptions.KUSTO_AAD_CLIENT_ID -> appId,
       KustoOptions.KUSTO_AAD_CLIENT_PASSWORD -> appKey,
       KustoOptions.KUSTO_QUERY -> "MyKustoTable"
     )
 
 val df = sqlContext.read
  .format("com.microsoft.kusto.spark.datasource")
  .option(KustoOptions.KUSTO_CLUSTER, "MyCluster")
  .option(KustoOptions.KUSTO_DATABASE, "MyDatabase")
  .options(conf)
  .load()
  ```
  
  For more reference code examples please see 
  [SimpleKustoDataSource](../src/main/scala/com/microsoft/kusto/spark/Sample/SimpleKustoDataSource.scala) and 
  [KustoConnectorDemo](../src/main/scala/com/microsoft/kusto/spark/Sample/KustoConnectorDemo.scala).