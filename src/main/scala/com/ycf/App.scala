package com.ycf

import java.io.{File, FileOutputStream, InputStream}
import java.util.zip.{ZipException, ZipFile}

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Hello world!
  *
  */
object App {

//  val tmpPath = "F:/project/load-data/data"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("load-data")
      .getOrCreate()

    val tmpPath = "/mnt/data"
//    val zipFile = new ZipFile("E:/BaiduNetdiskDownload/client_444_yongsheng.zip")
    val zipFile = new ZipFile("/mnt/client_446_yongsheng.zip")
    unzip(zipFile, tmpPath)

    val df = spark.read.option(
      "inferschema", "true"
    ).option(
      "header", "true"
    ).option(
      "encoding", "utf-8"
    ).csv("s3://quant-warehouse/data/*")
//    ).csv(s"file://$tmpPath/*")

//    df.printSchema()
//    df.show()

    val tableName = "history_data"

    df.write.format("orc").mode(SaveMode.Overwrite).partitionBy("","Date").option(
      "path", s"s3://quant-warehouse/test/hive/$tableName"
    ).saveAsTable(tableName)
  }

  /** 完整路径版本
    * 解压zip格式文件
    */
  //  def unzip(zipFile: ZipFile, targetPath: String): Unit = {
  //    val target = if (targetPath.endsWith("/")) targetPath else targetPath + "/"
  //    val entries = zipFile.entries()
  //    while (entries.hasMoreElements) {
  //      val element = entries.nextElement()
  //      println("Name = "+ element.getName)
  //      if (element.isDirectory) new File(target + element.getName).mkdirs()
  //      else {
  //        val fileName = element.getName.toLowerCase()
  //        val filePath = target + fileName
  //        val subPath = if (fileName.endsWith(".zip")) filePath.substring(0, filePath.indexOf(".zip")) else null
  //
  //        println(s"FileName = ${fileName} ## SubPath = $subPath  ## filePath = $filePath" )
  //        writeFile(filePath, zipFile.getInputStream(element))
  //        if (fileName.endsWith(".zip")) {
  //          new File(subPath).mkdirs()
  //          val subFile = new ZipFile(filePath)
  //          unzip(subFile, subPath)
  //          new File(filePath).deleteOnExit()
  //        }
  //      }
  //    }
  //  }

  def writeFile(path: String, inputStream: InputStream): Unit = {
    val b = new Array[Byte](1024)
    var length = inputStream.read(b)
    val output = new FileOutputStream(path, false)
    while (length > 0) {
      output.write(b, 0, length)
      length = inputStream.read(b)
    }
    output.flush()
    output.close()
  }

  /**
    * 去除多余路径
    * @param zipFile
    * @param targetPath
    */
  def unzip(zipFile: ZipFile, targetPath: String): Unit = {
    new File(targetPath).mkdirs()
    val target = if (targetPath.endsWith("/")) targetPath else targetPath + "/"
    val entries = zipFile.entries()
    while (entries.hasMoreElements) {
      val element = entries.nextElement()
      println("Name = " + element.getName)
      if (!element.isDirectory) {
//      else {
        val elementName = element.getName.toLowerCase()
        val fileName = elementName.substring(elementName.lastIndexOf("/") + 1, elementName.length)
        val filePath = target + fileName
        val subPath = if (fileName.endsWith(".zip")) filePath.substring(0, filePath.indexOf(".zip")) else null

        println(s"FileName = ${fileName} ## SubPath = $subPath  ## filePath = $filePath")
        try {
          writeFile(filePath, zipFile.getInputStream(element))
          if (fileName.endsWith(".zip")) {
            new File(subPath).mkdirs()
            val subFile = new ZipFile(filePath)
            Thread.sleep(1000)
            unzip(subFile, subPath)
            println(s"filePath=$filePath")
            new File(filePath).delete()
            new File(filePath).deleteOnExit()
          }
        } catch {
          case e: ZipException => {
            println(filePath + " has been destoryed!!")
          }
        }
      }
    }
  }
}
