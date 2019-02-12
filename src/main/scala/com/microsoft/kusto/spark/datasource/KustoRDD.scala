package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import java.util.{Locale, UUID}

import com.microsoft.azure.kusto.data.Client
import com.microsoft.kusto.spark.utils.{CslCommandsGenerator, KustoClient, KustoQueryUtils}
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils => KDSU}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}
import scala.collection.JavaConverters._

private[kusto] case class KustoPartition(predicate: String, idx: Int) extends Partition {
  override def index: Int = idx
}

private[kusto] case class KustoPartitionInfo(num: Int, column: String, mode: String)

private[kusto] case class KustoStorageParameters(account: String,
                                                 secrete: String,
                                                 container: String,
                                                 isKeyNotSas: Boolean)

private[kusto] case class KustoRddParameters(
                                              sqlContext: SQLContext,
                                              schema: StructType,
                                              cluster: String,
                                              database: String,
                                              query: String,
                                              appId: String,
                                              appKey: String,
                                              authorityId: String)

private[kusto] object KustoRDD {

  private[kusto] def leanBuildScan(params: KustoRddParameters): RDD[Row] = {
    val kustoClient = KustoClient.getAdmin(params.cluster, params.appId, params.appKey, params.authorityId)

    val kustoResult = kustoClient.execute(params.database, params.query)
    val serializer = KustoResponseDeserializer(kustoResult)
    params.sqlContext.createDataFrame(serializer.toRows, serializer.getSchema).rdd
  }

  private[kusto] def scaleBuildScan(params: KustoRddParameters, storage: KustoStorageParameters, partitionInfo: KustoPartitionInfo): RDD[Row] = {
    val partitions = calculatePartitions(params, partitionInfo)
    new KustoRDD(partitions, params, storage).asInstanceOf[RDD[Row]]
  }

  private def calculatePartitions(params: KustoRddParameters, partitionInfo: KustoPartitionInfo): Array[Partition] = {
    partitionInfo.mode match {
      case "hash" => calculateHashPartitions(params, partitionInfo)
      case "integral" | "timestamp" | "predicate" => throw new InvalidParameterException(s"Partitioning mode '${partitionInfo.mode}' is not yet supported ")
      case _ => throw new InvalidParameterException(s"Partitioning mode '${partitionInfo.mode}' is not valid")
    }
  }

  private def calculateHashPartitions(params: KustoRddParameters, partitionInfo: KustoPartitionInfo): Array[Partition] = {
    // Single partition
    if (partitionInfo.num <= 1) return Array[Partition] (KustoPartition(null, 0))

    val partitions = new Array[Partition](partitionInfo.num)
    for(partitionId <- 0 until partitionInfo.num) {
      val partitionPredicate = s" hash(${partitionInfo.column}, ${partitionInfo.num}) == $partitionId"
      partitions(partitionId) = KustoPartition(partitionPredicate, partitionId)
    }
    partitions
  }
}

private[kusto] class KustoRDD(partitions: Array[Partition], params: KustoRddParameters, storage: KustoStorageParameters)
  extends RDD[InternalRow](params.sqlContext.sparkContext, Nil) {

  private val myName = this.getClass.getSimpleName
  override protected def getPartitions: Array[Partition] = partitions

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val kustoClient = KustoClient.getAdmin(params.cluster, params.appId, params.appKey, params.authorityId)
    readPartitionViaBlob(split.asInstanceOf[KustoPartition], context, kustoClient, params, storage)
  }

  private[kusto] def readPartitionViaBlob(partition: KustoPartition,
                                          context: TaskContext,
                                          client: Client,
                                          params: KustoRddParameters,
                                          storage: KustoStorageParameters): Iterator[InternalRow] = {

    val (blobs, directory) = exportPartitionToBlob(partition, context, client, params, storage)
    importPartitionFromBlob(partition, context, client, params, storage, blobs, directory)
  }

  // Export a single partition from Kusto to transient Blob storage.
  // Returns a list of blobs where the exported data resides, and the directory path for these blobs
  private[kusto] def exportPartitionToBlob(partition: KustoPartition,
                                           context: TaskContext,
                                           client: Client,
                                           params: KustoRddParameters,
                                           storage: KustoStorageParameters): (List[String], String) = {

    val directory = KustoQueryUtils.simplifyName(s"${params.appId}/dir${UUID.randomUUID()}/")

    val exportCommand = CslCommandsGenerator.generateExportDataCommand(
      params.appId,
      params.query,
      storage.account,
      storage.container,
      directory,
      storage.secrete,
      storage.isKeyNotSas,
      partition.idx,
      partition.predicate
    )

    (
      client.execute(params.database, exportCommand).getValues.asScala.map(row => row.get(0)).toList,
      directory
    )
  }

  private[kusto] def importPartitionFromBlob(
                                              partition: KustoPartition,
                                              context: TaskContext,
                                              client: Client,
                                              params: KustoRddParameters,
                                              storage: KustoStorageParameters,
                                              blobs: List[String],
                                              directory: String
                                            ): Iterator[InternalRow] = {

    if (blobs.isEmpty) {
      KDSU.logWarn(myName, s"Transient blob for partition ${partition.idx} is empty, skipping export...")
      return Iterator.empty
    }

    blobs.foreach(blob => KDSU.logInfo(myName, s"Exported to blob: $blob"))
    if (storage.isKeyNotSas) {
      params.sqlContext.sparkSession.conf.set(s"fs.azure.account.key.${storage.account}.blob.core.windows.net", s"${storage.secrete}")
    }
    else {
      params.sqlContext.sparkSession.conf.set(s"fs.azure.sas.${storage.container}.${storage.account}.blob.core.windows.net", s"${storage.secrete}")
    }
    params.sqlContext.sparkSession.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")

    params.sqlContext.read.parquet(s"wasbs://${storage.container}@${storage.account}.blob.core.windows.net/$directory")
      .queryExecution.toRdd.iterator(partition, context)
  }
}
