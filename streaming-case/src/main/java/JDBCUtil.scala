import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

/**
 * @package:
 * @author Summer
 * @description 创建JDBC连接类，用于连接MySQL数据库
 * @create 2022-04-12 19:49
 * */
object JDBCUtil {

  var dataSource: DataSource = init()

  def init() = {
    val props = new Properties()
    props.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    props.setProperty("url", "jdbc:mysql://node1:3306/spark_test?useUnicode=true&characterEncoding=utf8")
    props.setProperty("username", "root")
    props.setProperty("password", "1004")
    props.setProperty("maxActive", "50")
    DruidDataSourceFactory.createDataSource(props)
  }

  // 获取MySQL连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  // 插入一条记录
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]) = {
    var rtn = 0
    var ps: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      ps = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          ps.setObject(i + 1, params(i))
        }
      }
      rtn = ps.executeUpdate()
      connection.commit()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  // 批量插入数据
  def executeBatchUpdate(connection: Connection, sql: String, paramsList: Iterable[Array[Any]]) = {
    var rtn = 0
    var ps: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      ps = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- params.indices) {
            ps.setObject(i + 1, params(i))
          }
          ps.addBatch()
        }
      }
      rtn = ps.executeUpdate()
      connection.commit()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  // 判断一个数据是否存在
  def isExists(connection: Connection, sql: String, param: Array[Any]) = {
    var flag: Boolean = false
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      if (param != null && param.length > 0) {
        for (i <- param.indices) {
          ps.setObject(i + 1, param(i))
        }
        flag = ps.executeQuery().next()
        ps.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  // 获取MySQL的一条数据
  def getMysqlData(connection: Connection, sql: String, params: Array[Any]) = {
    var result: Long = 0L
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      for (i <- params.indices) {
        ps.setObject(i + 1, params(i))
      }
      val resultSet: ResultSet = ps.executeQuery()
      while (resultSet.next()) {
        result = resultSet.getLong(1)
      }
      resultSet.close()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }


}
