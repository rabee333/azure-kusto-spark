package com.microsoft.kusto.spark.datasource

import java.security.InvalidParameterException
import com.microsoft.kusto.spark.datasink.KustoWriter
import com.microsoft.kusto.spark.utils.{KustoDataSourceUtils, KustoQueryUtils}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val (isAsync,tableCreation) = KustoDataSourceUtils.validateSinkParameters(parameters)

    KustoWriter.write(
      None,
      data,
      parameters.getOrElse(KustoOptions.KUSTO_CLUSTER, ""),
      parameters.getOrElse(KustoOptions.KUSTO_DATABASE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_TABLE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_ID, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"),
      isAsync,
      tableCreation,
      mode,
      parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, "UTC"))

    val resultLimit = parameters.getOrElse(KustoOptions.KUSTO_WRITE_RESULT_LIMIT, "1")
    val limit = if (resultLimit.equalsIgnoreCase("none")) None else {
        try{
          Some(resultLimit.toInt)
        }
        catch {
          case _: Exception => throw new InvalidParameterException(s"KustoOptions.KUSTO_WRITE_RESULT_LIMIT is set to '$resultLimit'. Must be either 'none' or integer value")
        }
      }

    createRelation(sqlContext, adjustParametersForBaseRelation(parameters, limit))
  }

  def adjustParametersForBaseRelation(parameters: Map[String, String], limit: Option[Int]): Map[String, String] = {
    val table = parameters.get(KustoOptions.KUSTO_TABLE)

    if (table.isEmpty) throw new RuntimeException("Cannot read from Kusto: table name is missing")

    if (limit.isEmpty) {
      parameters + (KustoOptions.KUSTO_NUM_PARTITIONS -> "1")
    }
    else {
      parameters + (KustoOptions.KUSTO_TABLE -> KustoQueryUtils.limitQuery(table.get, limit.get)) + (KustoOptions.KUSTO_NUM_PARTITIONS -> "1")
    }
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val requestedPartitions = parameters.get(KustoOptions.KUSTO_NUM_PARTITIONS)
    val readMode = parameters.getOrElse(KustoOptions.KUSTO_READ_MODE, "lean")
    val numPartitions = setNumPartitionsPerMode(sqlContext, requestedPartitions, readMode)

    if(!KustoOptions.supportedReadModes.contains(readMode)) {
      throw new InvalidParameterException(s"Kusto read mode must be one of ${KustoOptions.supportedReadModes.mkString(", ")}")
    }

    if (numPartitions != 1 && readMode.equals("lean")) {
      throw new InvalidParameterException(s"Reading in lean mode cannot be done on multiple partitions. Requested number of partitions: $numPartitions")
    }

    KustoRelation(
      parameters.getOrElse(KustoOptions.KUSTO_CLUSTER, ""),
      parameters.getOrElse(KustoOptions.KUSTO_DATABASE, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_ID, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_CLIENT_PASSWORD, ""),
      parameters.getOrElse(KustoOptions.KUSTO_AAD_AUTHORITY_ID, "microsoft.com"),
      parameters.getOrElse(KustoOptions.KUSTO_QUERY, ""),
      readMode.equalsIgnoreCase("lean"),
      numPartitions,
      parameters.get(KustoOptions.KUSTO_PARTITION_COLUMN),
      parameters.get(KustoOptions.KUSTO_CUSTOM_DATAFRAME_COLUMN_TYPES))(sqlContext)
  }

  override def shortName(): String = "kusto"

  private def setNumPartitionsPerMode(sqlContext: SQLContext, requestedNumPartitions: Option[String], readMode: String): Int = {
    if (requestedNumPartitions.isDefined) requestedNumPartitions.get.toInt else {
      if (readMode.equals("lean")) 1 else {
        sqlContext.getConf("spark.sql.shuffle.partitions", "10").toInt
      }
    }
  }
}
