package org.jgi.spark.localcluster

;

import com.typesafe.scalalogging.LazyLogging
import org.rocksdb.{CompactionStyle, Options, RocksDB}
import org.rocksdb.util.SizeUnit
import java.nio.file.{Files, Paths}

import com.google.protobuf.ByteString

/**
  * Created by Lizhen Shi .
  */
class DBManager(val dbpath: String) extends LazyLogging {

  val options = new Options().setCreateIfMissing(false)
  val db = {
    RocksDB.openReadOnly(options, dbpath)
  }

  def close(): Unit = {
    db.close()
    options.close()
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
    def run(): Unit = {
      close()
      logger.info(s"DB ${dbpath} was closed on jvm shutdown")
    }
  }))

  def get(key: Int): String = {
    val v = db.get(Utils.toByteArray(key))
    if (v==null) {
      throw  new Exception("key not found: "+key)
    }
    ByteString.copyFrom(v).toStringUtf8
  }
}

object DBManagerSingleton extends LazyLogging {

  @transient private var _instance: DBManager = null

  def instance(dbpath: String): DBManager = {
    if (_instance == null) {
      require(Files.exists(Paths.get(dbpath)))
      _instance = new DBManager(dbpath)
      logger.info(s"create singleton instance with (${_instance.dbpath}) for the first time.")
    }
    _instance
  }

}
