package ovgu.chen.ER.teamProject;

//import java.util.logging.Logger;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.round;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric;

import scala.Option;

public class exampleProcess {
	public static void main(String args[]) throws Exception {
		commonMethods methods = new commonMethods();
		
		SparkSession sparkSQL = methods.getSparkSQL();
		//loading data to Spark SQL
		Dataset<Row> inputData = methods.readCSV("src/test/resources/example.csv");
		inputData.show();
		//Pre-processing
		Dataset<Row> preprocessedData = methods.preprocessing(inputData);
		preprocessedData = (preprocessedData
				.withColumnRenamed("given-name", "given_name")
				.withColumnRenamed("rec-id", "rec_id")
				.withColumnRenamed("income-normal", "income_normal")
				.withColumnRenamed("blood-pressure", "blood_pressure")
				.withColumnRenamed("credit-card-number","credit_card_number"));

		preprocessedData.show();
		//Blocking
		//registering doubleMetaphone function
		sparkSQL.udf().register("doubleMetaphone", new UDF1<String, String>() {
	           public String call(String str) throws Exception {
	        	   DoubleMetaphone doublemetaphone = new DoubleMetaphone();
	        	   return doublemetaphone.encode(str);   
	           }
		}, DataTypes.StringType);
		sparkSQL.udf().register("distance", new UDF2<String, String,Integer>() {
	           public Integer call(String left, String right) throws Exception {
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
		}, DataTypes.IntegerType);
		//appending key columns
		preprocessedData.createOrReplaceTempView("preprocessedData");
		Dataset<Row> withKey = sparkSQL.sql("SELECT rec_id,given_name,surname,"
				+ "							postcode,gender,"
				+ "                         CONCAT(substring(doubleMetaphone(given_name),1,2),"
				+ "							substring(doubleMetaphone(surname),1,2),substring(postcode,1,2)) as blockingKey"
				+ "                         FROM preprocessedData");
		withKey.createOrReplaceTempView("blocking");
		withKey.show();
		//do blocking by joining tuples with the same key
		Dataset<Row> block = sparkSQL.sql("SELECT r.rec_id,r.given_name,r.surname,"
						+ "                       r.gender,r.postcode,s.rec_id as rec_id2,s.given_name as given_name2,"
						+ "						  s.surname as surname2,s.postcode as postcode2,s.gender as gender2"
						+ "                       FROM blocking r JOIN blocking s "
					//	+ "                       on (s.blockingKey=r.blockingKey)"
						+ "                       where r.rec_id > s.rec_id AND distance(s.blockingKey, r.blockingKey)<2");
		block.show();
		//pair-wise comparison
		//jako winkler matric
		sparkSQL.udf().register("jaroSimilarity", new UDF2<String, String, String>(){
			public String call(String s1, String s2) throws Exception {
				if(s1.equals("0") || s2.equals("0")){
					return "0.0";
				}
				else{
					Option<Object> similarityScoreMid = JaroWinklerMetric.apply().compare(s1, s2, null);
				String similarityScore = similarityScoreMid.toString()
											  .replace("Some(", "")
											  .replace(")", "")
											  .replace("None", "0.0")
											  .replace(" ", "0.0");
					return similarityScore;
				}
			}
		}, DataTypes.StringType);
		Dataset<Row> similarityScores = block.withColumn("gNameScore", round(callUDF("jaroSimilarity", block.col("given_name"), block.col("given_name2")),2))
				 .withColumn("sNameScore", round(callUDF("jaroSimilarity", block.col("surname"), block.col("surname2")),2))
				 .withColumn("postcodeScore", round(callUDF("jaroSimilarity", block.col("postcode"), block.col("postcode2")),2))
				 .withColumn("genderScore", round(callUDF("jaroSimilarity", block.col("gender"), block.col("gender2")),2))
				 .select("rec_id","rec_id2","gNameScore","sNameScore","genderScore","postcodeScore");
		similarityScores.show();
		similarityScores.createOrReplaceTempView("similarityScores");
		sparkSQL.udf().register("matchNonmatchT", new UDF1<Double, String>(){
			public String call(Double dou) throws Exception {
				if(dou >=2.4){
					return "match";
				}
				else return "nonmatch";
			}
		}, DataTypes.StringType);
		sparkSQL.udf().register("matchNonmatchP", new UDF1<Double, String>(){
			public String call(Double dou) throws Exception {
				if(dou >=3.6){
					return "match";
				}
				else return "nonmatch";
			}
		}, DataTypes.StringType);
		//Threshold-Based Classification
		Dataset<Row> thresholdTotalScore = sparkSQL.sql("select rec_id, rec_id2, gNameScore+sNameScore+postcodeScore+genderScore"
				+ "								as tTotalScore from similarityScores");
		thresholdTotalScore.createOrReplaceTempView("thresholdTotalScore");
		Dataset<Row> thresholdScoreResult = sparkSQL.sql("select rec_id, rec_id2, tTotalScore, matchNonmatchT(tTotalScore) as tResult"
				+ "								from thresholdTotalScore");
		thresholdScoreResult.show();
		//Probabilistic Classification
		Dataset<Row> probabilisticTotalScore = sparkSQL.sql("select rec_id, rec_id2, 2*gNameScore+2*sNameScore+postcodeScore+genderScore"
				+ "								as pTotalScore from similarityScores");
		probabilisticTotalScore.createOrReplaceTempView("probabilisticTotalScore");
		Dataset<Row> probabilisticScoreResult = sparkSQL.sql("select rec_id, rec_id2, pTotalScore, matchNonmatchP(pTotalScore) as pResult"
				+ "								from probabilisticTotalScore");
		probabilisticScoreResult.show();
		sparkSQL.stop();
		
	}
}