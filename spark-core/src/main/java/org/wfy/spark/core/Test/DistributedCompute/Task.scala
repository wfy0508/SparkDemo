package org.wfy.spark.core.Test.DistributedCompute

/**
 * @title: Task
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-02-20 17:18
 */
class Task extends Serializable {
  val data = List(1, 2, 3, 4)
  val logic: Int => Int = _ * 2

  // 计算
  def compute(): List[Int] = {
    data.map(logic)
  }
}
