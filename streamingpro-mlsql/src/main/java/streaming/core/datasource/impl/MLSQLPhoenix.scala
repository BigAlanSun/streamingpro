package streaming.core.datasource.impl

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row}
import streaming.core.datasource._
import streaming.dsl.{ConnectMeta, DBMappingKey}

/**
  * @Author: Alan
  * @Time: 2019/1/3 10:52
  * @Description:
  */
class MLSQLPhoenix extends MLSQLSource with MLSQLSink with MLSQLSourceInfo with MLSQLRegistry{




  override def fullFormat: String = "org.apache.phoenix.spark"

  override def shortFormat: String = "phoenix"
  override def dbSplitter: String = ":"
  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    }else{
      Array("" ,config.path)
    }

    var schem = _dbname

    val format = config.config.getOrElse("implClass", fullFormat)
    if (_dbname != "") {
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        if(options.contains("schem")){
          schem = options.get("schem").get
        }
        reader.options(options)
      })
    }

    if (config.config.contains("schem")){
      schem = config.config.get("schem").get
    }

    val inputTableName = if (schem == "") _dbtable else s"${schem}:${_dbtable}"

    reader.option("table" ,inputTableName)

    //load configs should overwrite connect configs
    reader.options(config.config)
    reader.format(format).load()
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    }else{
      Array("" ,config.path)
    }

    var schem = _dbname

    val format = config.config.getOrElse("implClass", fullFormat)
    if (_dbname != "") {
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        if(options.contains("schem")){
          schem = options.get("schem").get
        }
        writer.options(options)
      })
    }

    if (config.config.contains("schem")){
      schem = config.config.get("schem").get
    }

    val outputTableName = if (schem == "") _dbtable else s"${schem}:${_dbtable}"

    writer.mode(config.mode)
    writer.option("table", outputTableName)
    //load configs should overwrite connect configs
    writer.options(config.config)
    config.config.get("partitionByCol").map { item =>
      writer.partitionBy(item.split(","): _*)
    }
    writer.format(config.config.getOrElse("implClass", fullFormat)).save()
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def sourceInfo(config: DataAuthConfig): SourceInfo = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    }else{
      Array("" ,config.path)
    }

    var schem = _dbname

    if (config.config.contains("schem")){
      schem = config.config.get("schem").get
    }else{
      if (_dbname != "") {
        val format = config.config.getOrElse("implClass", fullFormat)
        ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
          if(options.contains("schem")){
            schem = options.get("schem").get
          }
        })
      }
    }

    SourceInfo(shortFormat ,schem ,_dbtable)
  }
}
