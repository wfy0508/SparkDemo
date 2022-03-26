package org.wfy.spark.core.caseAnalysis.hotCategoryTop10

/**
 * @package: org.wfy.spark.core.caseAnalysis.hotCategoryTop10
 * @author Summer
 * @description ${description}
 * @create 2022-03-26 16:34
 * */
// 定义样例类，为累加器的输出结构
// (商品品类，点击次数，下单次数，支付次数)
case class categoryStruct(categoryID: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

