import com.alibaba.druid.pool.DruidDataSourceFactory
import org.apache.spark.sql.execution.datasources

import java.sql.Connection
import java.util.Properties
import javax.sql.DataSource

/**
 * @package:
 * @author Summer
 * @description 创建JDBC连接类
 * @create 2022-04-12 19:49
 * */
object JDBCUtil {

  var dataSource: DataSource = init()

  def init() = {
    val props = new Properties()
    props.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    props.setProperty("url", "jdbc:mysql://node1:3306/spark_test")
    props.setProperty("username", "root")
    props.setProperty("password", "1004")
    props.setProperty("maxActive", "50")
    DruidDataSourceFactory.createDataSource(props)
  }

  // 获取MySQL连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

}
