package org.wfy.spark.core.Test.DistributedCompute

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @title: Executor
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-02-20 15:05
 */
object Executor1 {

  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(8888)
    println("服务器启动，等待客户端发送信息：")
    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream

    val objIn = new ObjectInputStream(in)
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = task.compute()
    println("接收到数据" + ints)
    in.close()
    client.close()
    server.close()

  }
}
