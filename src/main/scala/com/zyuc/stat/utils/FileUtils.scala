package com.zyuc.stat.utils

/**
  * Created by zhoucw on 17-7-19.
  */

import java.io.{File, FileOutputStream, IOException, InputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.Logging

import scala.collection.mutable


object FileUtils extends Logging{

  /**
    *
    * 根据文件大小构建coalesce
    *
    * @param fileSystem   文件系统
    * @param filePath     文件路径
    * @param coalesceSize 收敛大小
    * @return
    *
    */

  def makeCoalesce(fileSystem: FileSystem, filePath: String, coalesceSize: Int): Int = {
    val path = new Path(filePath)
    try {
      val filesize = fileSystem.getContentSummary(path).getLength
      val msize = filesize.asInstanceOf[Double] / 1024 / 1024 / coalesceSize
      Math.ceil(msize).toInt
    } catch {
      case e: IOException => e.printStackTrace()
        1
    }

  }


  /**
    *
    * 检查文件是否上传完毕
    *
    * @param filePath    文件路径正则
    * @param fileCount   文件个数
    * @param checkPeriod 检查周期
    * @param checkTimes  检查次数
    * @param tryCount    当前检查到第几次
    * @return
    *
    */

  def checkFileUpload(fileSystem: FileSystem, filePath: String, fileCount: Int, checkPeriod: Long, checkTimes: Int, tryCount: Int): Int = {


    return 1
  }


  /**
    *
    *
    *
    * @param fileSystem 文件系统
    * @param outputPath 输出路径
    * @param loadTime   数据时间
    * @param template   路径模版
    *
    */

  def moveTempFiles(fileSystem: FileSystem, outputPath: String, loadTime: String, template: String, partitions: mutable.HashSet[String]): Unit = {

    // 删除数据目录到文件
    partitions.foreach(partition => {
      val dataPath = new Path(outputPath + "data/" + partition + "/" + loadTime + "-" + "/*.orc")
      fileSystem.delete(dataPath, false)
    })

    val tmpPath = new Path(outputPath + "temp/" + loadTime + template + "/*.orc")
    val tmpStatus = fileSystem.globStatus(tmpPath)

    var num = 0
    tmpStatus.map(tmpStat => {
      val tmpLocation = tmpStat.getPath().toString
      var dataLocation = tmpLocation.replace(outputPath + "temp/" + loadTime, outputPath + "data/")
      val index = dataLocation.lastIndexOf("/")
      dataLocation = dataLocation.substring(0, index + 1) + loadTime + "-" + num + ".orc"
      num = num + 1

      val tmpPath = new Path(tmpLocation)
      val dataPath = new Path(dataLocation)

      if (!fileSystem.exists(dataPath.getParent)) {
        fileSystem.mkdirs(dataPath.getParent)
      }
      fileSystem.rename(tmpPath, dataPath)

    })



    //获取文件列表
    // val files = fileSystem.listStatus(path)

  }

  def moveNewlogFiles(outputPath:String, outFiles:Array[FileStatus], loadTime:String) :Unit = {
    var num = 1
    outFiles.map(filestatus=>{
      val srcLocation = filestatus.getPath().toString
      val destLocation = srcLocation.replace(outputPath+"newlog", outputPath+ "data/" + loadTime)

    })

  }

  def downloadFileFromHdfs(fileSystem: FileSystem, hdfsDirLocation:String, localDirLocation:String, suffix:String) :Unit = {
    val hdfsPath = new Path(hdfsDirLocation+"*")
    val file = new File(localDirLocation)
    if(!file.exists()){
      file.mkdirs()
    }
    val hdfsStatus = fileSystem.globStatus(hdfsPath)
    hdfsStatus.map(p=>{
      val file = p.getPath
      val name = file.toString.substring(file.toString.lastIndexOf("/")+1)
      val localPath = localDirLocation+name+suffix
      val len = fileSystem.getContentSummary(p.getPath).getLength
      if(len>0){
        val in = fileSystem.open(file)
        val out= new FileOutputStream(localPath)
        IOUtils.copyBytes(in,out, 4096,true)
      }
    })
  }

  def renameHDFSDir(fileSystem:FileSystem, srcLocation:String, destLocation:String) :Boolean = {
    val srcPath = new Path(srcLocation)
    val destPath = new Path(destLocation)
    val isRename = fileSystem.rename(srcPath, destPath)
    isRename

  }


  def main(args: Array[String]): Unit = {
    val config = new Configuration
    var fileSystem: FileSystem = null
    fileSystem = FileSystem.get(config)
/*
    try {

      fileSystem = FileSystem.get(config)
      val filePath = "/hadoop/zcw/tmp/wcout"
      val coalesceSize = 5
      println(makeCoalesce(fileSystem, filePath, coalesceSize))
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (fileSystem != null) try
      fileSystem.close()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
*/
    val parentDirLocation = "/tmp/zcw"
    val srcLocation = "/tmp/zcw/t1"
    val destLocation = "/tmp/zcw/t3"
    renameHDFSDir(fileSystem, srcLocation, destLocation)

  }

}
