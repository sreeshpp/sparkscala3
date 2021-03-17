package com.sreesh.scala
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
object CacheTest {
  
   def main(args:Array[String])
  {
   
    /* this program convert DF to RDD and RDD to DF */
     
    var spark=SparkSession.builder().appName("StopWordRenover").getOrCreate()
     
     spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAIWRLSOV7K5NLNPNA")
     spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
     spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
     spark.sparkContext.getConf.set("spark.storage.memoryfraction","0.9")
     var df = spark.read.option("header",true).option("inferschema",true).csv("s3a://sreesh-twitter/gender-classifier-DFE-791531.csv")
     var df2=df.select("gender","name","text")
     var df3=df2.crossJoin(df2)
     df3.count()
     df3.cache()
     df3.count() 
  }
   
   
  
  
}