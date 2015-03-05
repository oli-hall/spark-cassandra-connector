package org.apache.spark.sql.cassandra.api.java

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.cassandra.CassandraSQLContext

class JavaCassandraSQLContext(sparkContext: JavaSparkContext) extends SQLContext(sparkContext) {

    private val sqlContext = new CassandraSQLContext(sparkContext)

    /**
      * Executes a query expressed in SQL, returning the result as a DataFrame.
      */
    def cql(cqlQuery: String): DataFrame =
      DataFrame(sqlContext, sqlContext.parseSql(cqlQuery))
}
