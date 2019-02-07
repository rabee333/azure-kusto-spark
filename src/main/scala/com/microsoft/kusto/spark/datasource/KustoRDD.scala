package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.Locale

import com.microsoft.azure.kusto.data.Client
import com.microsoft.kusto.spark.utils.KustoClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}

private[kusto] case class KustoPartition(whereClause: String, idx: Int) extends Partition {
  override def index: Int = idx
}

private[kusto] case class KustoPartitionInfo(num: Int, column: String)

private[kusto] case class KustoRddParameters(
                                              sqlContext: SQLContext,
                                              schema: StructType,
                                              cluster: String,
                                              database: String,
                                              query: String,
                                              appId: String,
                                              appKey: String,
                                              authorityId: String,
                                              storageAccount: String = "",
                                              storageSecret: String = "",
                                              container: String = "")

private[kusto] object KustoRDD {

  private[kusto] def leanBuildScan(params: KustoRddParameters): RDD[Row] = {
    val kustoClient = KustoClient.getAdmin(params.cluster, params.appId, params.appKey, params.authorityId)

    val kustoResult = kustoClient.execute(params.database, params.query)
    val serializer = KustoResponseDeserializer(kustoResult)
    params.sqlContext.createDataFrame(serializer.toRows, serializer.getSchema).rdd
  }

  private[kusto] def scaleBuildScan(params: KustoRddParameters, partitionInfo: KustoPartitionInfo): RDD[Row] = {

    val partitions = new Array[Partition](partitionInfo.num) // TODO calculate partitions
    new KustoRDD(partitions, params).asInstanceOf[RDD[Row]]
  }
}

private[kusto] class KustoRDD(partitions: Array[Partition], params: KustoRddParameters)
  extends RDD[InternalRow](params.sqlContext.sparkContext, Nil) {

  private def getWhereClause(part: KustoPartition): String = {
    if (part.whereClause != null) {
      "| where " + part.whereClause
    } else ""
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val kustoClient = KustoClient.getAdmin(params.cluster, params.appId, params.appKey, params.authorityId)
    readPartitionViaBlob(split.asInstanceOf[KustoPartition], context, kustoClient, params)
  }

  private[kusto] def readPartitionViaBlob(partition: KustoPartition,
                                          context: TaskContext,
                                          client: Client,
                                          params: KustoRddParameters): Iterator[InternalRow] = {
    exportPartitionToBlob(partition, context, client, params)
    importPartitionFromBlob(partition, context, client, params)
  }

  private[kusto] def exportPartitionToBlob(partition: KustoPartition,
                                           context: TaskContext,
                                           client: Client,
                                           params: KustoRddParameters): Unit = {

  }

  private[kusto] def importPartitionFromBlob(
                                              partition: KustoPartition,
                                              context: TaskContext,
                                              client: Client,
                                              params: KustoRddParameters
                                            ): Iterator[InternalRow] = {
    val directory: String = "TBD"
    params.sqlContext.read.parquet(s"wasbs://$params.container@$params.storageAccount.blob.core.windows.net/$directory")
      .queryExecution.toRdd.iterator(partition, context)
  }

  override protected def getPartitions: Array[Partition] = partitions
}