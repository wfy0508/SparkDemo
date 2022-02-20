package org.wfy.spark.core.Test.DistributedCompute

/**
 * @title: SubTask
 * @projectName SparkDemo
 * @description: TODO
 * @author summer
 * @date 2022-02-20 17:37
 */
class SubTask extends Serializable {
  var data: List[Int] = _
  var logic: Int => Int = _

  def compute(): List[Int] = {
    data.map(logic)
  }

}
