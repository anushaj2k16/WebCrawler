package ovgu.chen.ER.teamProject;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.round;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import info.debatty.java.stringsimilarity.JaroWinkler;
//import com.rockymadden.stringmetric.similarity.JaroWinklerMetric;

public class ThresholdER {
	public static void main(String args[]) throws Exception {
		commonMethods methods = new commonMethods();
		
		SparkSession sparkSQL = methods.getSparkSQL();
		
		//loading data to Spark SQL
		Dataset<Row> inputData = methods.readCSV("src/main/resources/10_5_5.csv"); //Creates the first 2 Jobs. TextFile->Filter. The last one creates the RDDs via a MapPartitionRDD.

		//		inputData.cache();
		
		//Pre-processing
		Dataset<Row> preprocessedData = methods.preprocessing(inputData);
		
		preprocessedData = (preprocessedData
				.withColumnRenamed("given-name", "given_name")
				.withColumnRenamed("rec-id", "rec_id")
				.withColumnRenamed("income-normal", "income_normal")
				.withColumnRenamed("blood-pressure", "blood")
				.withColumnRenamed("credit-card-number","credit")); //This is already done in the next 3 jobs. The first stage does some caching and then a map partition internal deals with this.

		//		preprocessedData.cache();
		
		//Blocking
		//registering doubleMetaphone function
		sparkSQL.udf().register("doubleMetaphone", new UDF1<String, String>() {
	           public String call(String str) throws Exception {
	        	   DoubleMetaphone doublemetaphone = new DoubleMetaphone();
	        	   return doublemetaphone.encode(str);   
	           }
		}, DataTypes.StringType);
		
		//appending key columns
		preprocessedData.createOrReplaceTempView("preprocessedData");

		Dataset<Row> withKey = sparkSQL.sql("SELECT rec_id,gender,given_name,surname,"
				+ "							postcode,city,credit,"
				+ "                         income,income_normal,age,sex,blood,"
				+ "                         CONCAT(substring(doubleMetaphone(given_name),1,2),"
				+ "								substring(doubleMetaphone(surname),1,2),substring(postcode,1,2)) as blockingKey"
				+ "                         FROM preprocessedData"); //This is the second stage of the 3 jobs. 
		withKey.createOrReplaceTempView("blocking");
		withKey.cache();

		//do blocking by joining tuples with the same key
		Dataset<Row> block = sparkSQL.sql("SELECT r.rec_id,r.given_name,r.surname,r.gender,r.sex,"
						+ "                       r.city,r.credit,r.postcode,"
						+ "						  r.income_normal,r.income,r.age,r.blood,"
						+ "                       s.rec_id as rec_id2,s.given_name as given_name2,s.surname as surname2,"
						+ "                       s.gender as gender2,s.sex as sex2,s.city as city2,"
						+ "						  s.credit as credit2,s.income as income2,"
						+ "                       s.postcode as postcode2,s.income_normal as income_normal2,"
						+ "                       s.age as age2,s.blood as blood2"
						+ "                       FROM blocking r JOIN blocking s "
						+ "                       on (s.blockingKey=r.blockingKey)"
						+ "                       where r.rec_id > s.rec_id"); 
//		block.cache();

		//pair-wise comparison
		//jako-winkler metric
		sparkSQL.udf().register("JaroWinkler",new UDF2<String, String, Double>(){
			public Double call(String str1, String str2) throws Exception{
				JaroWinkler jaroWinkler = new JaroWinkler();
				double dou = jaroWinkler.similarity(str1, str2);
				return dou;
			}
		},DataTypes.DoubleType);
		sparkSQL.udf().register("AbsDiff",new UDF2<String, String, Double>(){
			public Double call(String str1, String str2) throws Exception{
				double d1=Double.parseDouble(str1);
				double d2=Double.parseDouble(str2);
				double dou;
				double dMax;
				if (d1>200.0){
					dMax=5000.0;
				} else dMax=10.0;
				if ((Math.abs(d1-d2)) < dMax ){
				dou = 1.0 - (Math.abs(d1-d2)/dMax);
				}
				else{ dou = 0.0;}
				return dou;
			}
		},DataTypes.DoubleType);

		Dataset<Row> totalScore = block.withColumn("gNameScore", round(callUDF("JaroWinkler", block.col("given_name"), block.col("given_name2")),2))
									   .withColumn("sNameScore", round(callUDF("JaroWinkler", block.col("surname"), block.col("surname2")),2))
									   .withColumn("genderScore", round(callUDF("JaroWinkler", block.col("gender"), block.col("gender2")),2))
									   .withColumn("sexScore", round(callUDF("JaroWinkler", block.col("sex"), block.col("sex2")),2))
									   .withColumn("cityScore", round(callUDF("JaroWinkler", block.col("city"), block.col("city2")),2))
									   .withColumn("postcodeScore", round(callUDF("JaroWinkler", block.col("postcode"), block.col("postcode2")),2))
									   .withColumn("creditScore", round(callUDF("JaroWinkler", block.col("credit"), block.col("credit2")),2))
									   .withColumn("incomeScore", round(callUDF("AbsDiff", block.col("income"), block.col("income2")),2))
									   .withColumn("incomeNormalScore", round(callUDF("AbsDiff", block.col("income_normal"), block.col("income_normal2")),2))
									   .withColumn("ageScore", round(callUDF("AbsDiff", block.col("age"), block.col("age2")),2))
									   .withColumn("bloodScore", round(callUDF("AbsDiff", block.col("blood"), block.col("blood2")),2))
									   .withColumn("tTotalScore", round(org.apache.spark.sql.functions.expr("gNameScore+sNameScore+genderScore+sexScore+cityScore"
											   + "+postcodeScore+creditScore+incomeScore+incomeNormalScore+ageScore+bloodScore"),2))
									   .select("rec_id","rec_id2","tTotalScore");
		//This is the 3rd stage.
		totalScore.cache();
		withKey.unpersist();

		//register UDF to classify each pair to match or nonmatch
		sparkSQL.udf().register("tMatchNonmatch", new UDF1<Double, String>(){
			public String call(Double dou) throws Exception {
				if(dou >=8.25){
					return "match";
				}
				else return "nonmatch";
			}
		}, DataTypes.StringType);

		//clean the record ID
		sparkSQL.udf().register("cleanRecID", new UDF1<String, String>(){
			public String call(String str) throws Exception {
				return str.toString().replace("rec-","").replace("-org", "").replace("-dup-0","").replace("-dup-1","").replace("-dup-2",""); 
			}
		},DataTypes.StringType);
		Dataset<Row> result= totalScore
									.withColumn("tMatchNonmatch", callUDF("tMatchNonmatch", totalScore.col("tTotalScore")))
									.withColumn("rec_id", callUDF("cleanRecID", totalScore.col("rec_id")))
									.withColumn("rec_id2", callUDF("cleanRecID", totalScore.col("rec_id2")));

		//Evaluation

		result.createOrReplaceTempView("result");
		
		Dataset<Row> total = sparkSQL.sql("select count(*) as total_records from result");

		Dataset<Row> realDuplicates = sparkSQL.sql("select count(*) as Duplicates from result where rec_id =rec_id2");

		Dataset<Row> tTruePositive = sparkSQL.sql("select count(tMatchNonmatch) as tTruePositive from result where (tMatchNonmatch = 'match' and "
		+ "                      rec_id = rec_id2)");

		Dataset<Row> tFalseNegative = sparkSQL.sql("select count(tMatchNonmatch) as tFalseNegative from result where (tMatchNonmatch = 'nonmatch' and"
		+ "               rec_id = rec_id2)");

		Dataset<Row> tFalsePositive = sparkSQL.sql("select count(tMatchNonmatch) as tFalsePositive from result where (tMatchNonmatch = 'match' and"
		+ "               rec_id != rec_id2)");

		Dataset<Row> tTrueNegative = sparkSQL.sql("select count(tMatchNonmatch) as tTrueNegative from result where (tMatchNonmatch = 'nonmatch' and"
		+ "               rec_id != rec_id2)");

		Dataset<Row> posNegs = tTruePositive.crossJoin(tTrueNegative).crossJoin(tFalsePositive).crossJoin(tFalseNegative);

		Dataset<Row> finalResult= posNegs.withColumn("precision", org.apache.spark.sql.functions.expr("tTruePositive/(tTruepositive + tFalsePositive)"))
						.withColumn("recall", org.apache.spark.sql.functions.expr("tTruePositive/(tTruePositive + tFalseNegative)"))
						.withColumn("F-Measure", org.apache.spark.sql.functions.expr("2*precision*recall/(precision+recall)"));
		finalResult.show();
		totalScore.unpersist();

		//Thread.sleep(1000000);
		sparkSQL.stop();
	}
}