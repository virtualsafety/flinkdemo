//https://alvinalexander.com/source-code/scala-jdbc-sql-select-insert-statement-resultset-preparedstatement-example/
//https://alvinalexander.com/scala/scala-jdbc-connection-mysql-sql-select-example/
import java.sql.{Connection, DriverManager,PreparedStatement}


object ScalaJdbcPreparedStatement {

  def main(args: Array[String]) {
    // connect to the database  on the localhost
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://x.x.x.6x:5432/xxxxdb"
    val username = "xxxx"
    val password = "xxxx"

    // there's probably a better way to do this
    var connection: Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      val selectSql =
        """
          |select name,age from person
          |where name = ?
        """.stripMargin

      val preparedStmt: PreparedStatement = connection.prepareStatement(selectSql)
      preparedStmt.setString(1, "zhe")

      val resultSet = preparedStmt.executeQuery()
      while (resultSet.next()) {
        val name = resultSet.getString("name")
        val age = resultSet.getString("age")
        println("person: name = " + name + ", " + name)
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }
}
