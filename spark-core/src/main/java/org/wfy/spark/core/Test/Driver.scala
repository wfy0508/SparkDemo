package org.wfy.spark.core.Test

import java.io.OutputStream
import java.net.Socket

/**
 * @title: Driver
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-02-20 15:04
 */
object Driver {
  def main(args: Array[String]): Unit = {
    val socket = new Socket("localhost", 9999)
    val out: OutputStream = socket.getOutputStream
    out.write(8888)
    out.flush()
    out.close()
    socket.close()
  }

}
