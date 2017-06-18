package ovgu.chen.ER.teamProject;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class commonMethods {
	
	SparkConf sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	.set("*.sink.graphite.class", "org.apache.spark.metrics.sink.GraphiteSink")
	.set("*.sink.graphite.host", "127.0.0.1")
	.set("*.sink.graphite.port", "2003")
	.set("*.sink.graphite.period", "5")
	.set("*.sink.graphite.unit", "seconds")
	.set("master.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
	.set("worker.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
	.set("driver.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
	.set("executor.source.jvm.class", "org.apache.spark.metrics.source.JvmSource");
	
	SparkSession sparkSQL = SparkSession.builder()
			.master("local[*]")
			.config("spark.sql.broadcastTimeout", "3000")
//			.master("spark://192.168.144.31:7077")
//			.config("spark.executor.cores","2")
//			.config("spark.cores.max","8")
			.appName("EntityResolution")
//			.config("spark.executor.memory","6g")
			.getOrCreate();
	public SparkSession getSparkSQL() {
		return sparkSQL;
	}
	public Dataset<Row> readCSV(String str){
		Dataset<Row> inputDataset = sparkSQL.read()
				.option("header", "true")
				//.option("inferSchema", "true")
				.option("treatEmptyValuesAsNulls","true")
				.option("nullValue", " ")
				//.csv(args[0]);
				.csv(str);
				//.as(Encoders.bean(Application.class));
		return inputDataset;
	}
	
	public Dataset<Row> preprocessing(Dataset<Row> data){
		Dataset<Row> preprocessedData = data.na().fill("0").drop("age-uniform");
		preprocessedData.col("given-name").toString().replace("-", " ");
		return preprocessedData;
	}
    public Integer HammingDistance(String left, String right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("Strings must not be null");
        }

        if (left.length() != right.length()) {
            throw new IllegalArgumentException("Strings must have the same length");
        }

        int distance = 0;

        for (int i = 0; i < left.length(); i++) {
            if (left.charAt(i) != right.charAt(i)) {
                distance++;
            }
        }

        return distance;
    }

}
