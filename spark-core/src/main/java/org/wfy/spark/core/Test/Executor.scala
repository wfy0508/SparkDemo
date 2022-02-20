package org.wfy.spark.core.Test

import java.io.InputStream
import java.net.{ServerSocket, Socket}

/**
 * @title: Executor
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-02-20 15:05
 */
object Executor {

  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    println("服务器启动，等待客户端发送信息：")
    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    val i: Int = in.read()
    println("接收到数据" + i)
    client.close()
    server.close()

  }
}
