package com.sreesh.scala
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.apache.spark.sql.functions.{col, expr,sum,concat,lit,explode,split}
import org.apache.spark.sql.SaveMode


object ReadRedshift {
  
  def main(Arags:Array[String])
  {
  /* val spark=SparkSession.builder().appName("json").master("local[*]").getOrCreate() */ 
    val spark=SparkSession.builder().appName("redshift").getOrCreate()  
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAIWRLSOV7K5NLNPNA")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    var df=spark.read.option("header",true).option("inferschema",true).csv("s3a://us-west-2.serverless-analytics/NYC-Pub/green/green_tripdata_2014*.csv")
    var df2=spark.read.option("header",true).option("inferschema",true).csv("s3a://us-west-2.serverless-analytics/NYC-Pub/green/green_tripdata_2013*.csv")
    var df3=df2.unionAll(df)
    /*  var df2=spark.read.option("header",true).option("inferschema",true).csv("s3a://us-west-2.serverless-analytics/NYC-Pub/green/green_tripdata_2014-01.csv") */
 /*   df2.groupBy(col("VendorID")).count().show() */
    df3.groupBy(col("VendorID")).count().show()
/*    var df3 =df2.select("VendorID","Total_amount","Payment_type","lpep_pickup_datetime","Lpep_dropoff_datetime","Pickup_longitude","Dropoff_longitude").join(df.select("VendorID","Total_amount","Payment_type"),df2("VendorID")===df("VendorID"))
    df3.write.format("jdbc").mode(SaveMode.Append).option("url", "jdbc:mysql://cxln2.c.thelab-240901.internal:3306/retail_db").option("driver", "com.mysql.jdbc.Driver").option(
    "dbtable", "batch6_hadoop").option("user", "sqoopuser").option("password", "NHkkP876rp").save() */
    
    
  }
  
}