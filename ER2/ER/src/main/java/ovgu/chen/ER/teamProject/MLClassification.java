package ovgu.chen.ER.teamProject;

//import java.util.logging.Logger;
import java.awt.List;
import java.lang.*;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.round;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import info.debatty.java.stringsimilarity.JaroWinkler;
//import com.rockymadden.stringmetric.similarity.JaroWinklerMetric;

import scala.Option;

import org.apache.spark.sql.functions;

public class MLClassification {
	public static void main(String args[]) throws Exception {
		commonMethods methods = new commonMethods();
		
		SparkSession sparkSQL = methods.getSparkSQL();
		//loading data to Spark SQL
		Dataset<Row> inputData = methods.readCSV("src/main/resources/10_4_5.csv");
//		Dataset<Row> duplicateTuples = inputData.filter(inputData.col("rec-id").like("%-dup-%"));
//		Dataset<Row> inputData = methods.readCSV(args[0]);
		//Pre-processing
		Dataset<Row> preprocessedData = methods.preprocessing(inputData);
		//System.out.println(preprocessedData.count());
		preprocessedData = (preprocessedData
				.withColumnRenamed("given-name", "given_name")
				.withColumnRenamed("rec-id", "rec_id")
				.withColumnRenamed("income-normal", "income_normal")
				.withColumnRenamed("blood-pressure", "blood")
				.withColumnRenamed("credit-card-number","credit"));
		//preprocessedData.show();
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
//		Dataset<Row> withKey = sparkSQL.sql("SELECT rec_id,gender,given_name,surname,"
//				+ "							postcode,city,credit,"
//				+ "                         income,income_normal,age,sex,blood,"
//				+ "                         CONCAT(substring(given_name,1,2),"
//				+ "							substring(surname,1,2), substring(postcode,1,2)) as blockingKey,"
////				+ "								substring(postcode,1,2)) as blockingKey"
//				+ "                         FROM preprocessedData");
		Dataset<Row> withKey = sparkSQL.sql("SELECT rec_id,gender,given_name,surname,"
				+ "							postcode,city,credit,"
				+ "                         income,income_normal,age,sex,blood,"
				+ "                         CONCAT(substring(doubleMetaphone(given_name),1,2),"
				+ "								substring(doubleMetaphone(surname),1,2),substring(postcode,1,2)) as blockingKey"
//				+ "								substring(postcode,1,2)) as blockingKey"
				+ "                         FROM preprocessedData");
		withKey.createOrReplaceTempView("blocking");
		//withKey.show();
//		Dataset<Row> pairs=withKey.as("r").join(withKey.as("s").withColumnRenamed("rec_id", "rec_id2")
//															   .withColumnRenamed("given_name","given_name2")
//															   .withColumnRenamed("surname","surname2")
//															   .withColumnRenamed("gender","gender2")
//															   .withColumnRenamed("sex","sex2")
//															   .withColumnRenamed("age","age2")
//															   .withColumnRenamed("blood_pressure","blood_pressure2")
//															   .withColumnRenamed("credit_card_number","credit_card_number2")
//															   .withColumnRenamed("city", "city2")
//															   .withColumnRenamed("postcode", "postcode2")
//															   .withColumnRenamed("income_normal", "income_normal2")).where("s.blockingKey=r.blockingKey");
//		pairs.show();
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
//		double reductionRation = (block.count())/Math.pow(inputData.count(),2);
		//System.out.println("Reduction Ration of 10_4_50 is: " + reductionRation + " " + block.count() + " " + inputData.count()*inputData.count());
		//calculating True Pair Completeness
//		sparkSQL.udf().register("cleanRecID", new UDF1<String, String>(){
//			public String call(String str) throws Exception {
//				return str.toString().replace("rec-","").replace("-org", "").replace("-dup-0","").replace("-dup-1","").replace("-dup-2",""); 
//			}
//		},DataTypes.StringType);
//		Dataset<Row> cleanID= block.withColumn("rec_id", callUDF("cleanRecID", block.col("rec_id")))
//								.withColumn("rec_id2", callUDF("cleanRecID", block.col("rec_id2")));
//		cleanID.createOrReplaceTempView("cleanID");
//		Dataset<Row> survivedDuplicates = sparkSQL.sql("select * from cleanID where rec_id =rec_id2");
//		double TPC = (double) survivedDuplicates.count()/ duplicateTuples.count() ;
//		System.out.println("Reduction Ration and True Pairs Completeness of 10_4_50 are: " + reductionRation +
//				" " + TPC + " " + duplicateTuples.count()+" " + survivedDuplicates.count());
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
//		block.show();		
		Dataset<Row> similarityScores = block.withColumn("gNameScore", round(callUDF("JaroWinkler", block.col("given_name"), block.col("given_name2")),3))
											.withColumn("sNameScore", round(callUDF("JaroWinkler", block.col("surname"), block.col("surname2")),3))
											.withColumn("genderScore", round(callUDF("JaroWinkler", block.col("gender"), block.col("gender2")),3))
											.withColumn("sexScore", round(callUDF("JaroWinkler", block.col("sex"), block.col("sex2")),3))
											.withColumn("cityScore", round(callUDF("JaroWinkler", block.col("city"), block.col("city2")),3))
											.withColumn("postcodeScore", round(callUDF("JaroWinkler", block.col("postcode"), block.col("postcode2")),3))
											.withColumn("creditScore", round(callUDF("JaroWinkler", block.col("credit"), block.col("credit2")),3))
											.withColumn("incomeScore", round(callUDF("AbsDiff", block.col("income"), block.col("income2")),3))
											.withColumn("incomeNormalScore", round(callUDF("AbsDiff", block.col("income_normal"), block.col("income_normal2")),3))
											.withColumn("ageScore", round(callUDF("AbsDiff", block.col("age"), block.col("age2")),3))
											.withColumn("bloodScore", round(callUDF("AbsDiff", block.col("blood"), block.col("blood2")),3))
											.select("rec_id","rec_id2","gNameScore","sNameScore","genderScore","sexScore","cityScore","postcodeScore",
													 "creditScore","incomeScore","incomeNormalScore","ageScore","bloodScore", "sumColumn");

//		similarityScores.createOrReplaceTempView("simScores");
//		Dataset<Row> totalScore = sparkSQL.sql("select rec_id, rec_id2, round(gNameScore+sNameScore+genderScore+sexScore+cityScore+postcodeScore"
//				+ "								+creditScore+incomeScore+ageScore+incomeNormalScore+bloodScore) as tTotalScore"
////				+ "								round(2*gNameScore+2*sNameScore+genderScore+sexScore+cityScore+postcodeScore"
////				+ "								+creditScore+incomeScore+ageScore+incomeNormalScore+bloodScore,2) as pTotalScore "
//				+ "								from simScores");
//		totalScore.cache();

		//ML-based classification
		//load csv training data file.
		Dataset<Row> trainingClassifier = sparkSQL.read()
											.option("header", "true")
											.option("treatEmptyValuesAsNulls","true")
											.option("inferschema", "true")
											.option("nullValue", " ")
											.csv(args[1]);
											//.csv("src/main/resources/train.csv");
		VectorAssembler assembler = new VectorAssembler()
										.setInputCols(new String[]{"GNAME", "SNAME", "STREET","ADD1",
												 "SUBURB","STATE"})
										.setOutputCol("features");
		Dataset<Row> assembledTrainingData = assembler.transform(trainingClassifier);

		VectorAssembler assemblerReal = new VectorAssembler()
										.setInputCols(new String[]{"gNameScore","sNameScore","streetNumberScore","addressScore",
												"suburbScore","stateScore"})
										.setOutputCol("features");
		Dataset<Row> assembledRealData = assemblerReal.transform(similarityScores); 
		
		//Train a decision tree model.
		DecisionTreeClassifier decisionTree = new DecisionTreeClassifier()
											.setLabelCol("label")
											.setFeaturesCol("features");
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {decisionTree});
		
		PipelineModel model = pipeline.fit(assembledTrainingData);
		
		Dataset<Row> predictions = model.transform(assembledRealData);

		Dataset<Row> classificationResult = predictions.select("rec_id","rec_id2","features","prediction");
		
		sparkSQL.udf().register("cleanRecID", new UDF1<String, String>(){
			public String call(String str) throws Exception {
				return str.toString().replace("rec-","").replace("-org", "").replace("-dup-0","").replace("-dup-1","").replace("-dup-2",""); 
			}
		},DataTypes.StringType);
		
		Dataset<Row> finalResult = classificationResult.withColumn("rec_id", callUDF("cleanRecID",classificationResult.col("rec_id")))
					.withColumn("rec_id2", callUDF("cleanRecID",classificationResult.col("rec_id2")))
					.select("rec_id","rec_id2","features","prediction").cache();
		finalResult.createOrReplaceTempView("result");
		
		Dataset<Row> total = sparkSQL.sql("select count(*) as total_records from result");
		
		Dataset<Row> duplicatesNumber = sparkSQL.sql("select count(*) as duplications from result where rec_id =rec_id2");
		
		Dataset<Row> truePositive = sparkSQL.sql("select count(prediction) as TruePositive from result where (prediction=1.0 and "
		+ "                      rec_id = rec_id2)");  
		
		Dataset<Row> falseNegative = sparkSQL.sql("select count(prediction) as FalseNegative from result where (prediction=0.0 and"
		+ "               rec_id = rec_id2)");
		
		Dataset<Row> falsePositive = sparkSQL.sql("select count(prediction) as FalsePositive from result where (prediction=1.0 and"
		+ "               rec_id != rec_id2)");

		Dataset<Row> trueNegative = sparkSQL.sql("select count(prediction) as TrueNegative from result where (prediction=0.0 and"
		+ "               rec_id != rec_id2)");

		Dataset<Row> posNegs = truePositive.crossJoin(trueNegative).crossJoin(falsePositive).crossJoin(falseNegative);

		Dataset<Row> evaluation= posNegs.withColumn("precision", org.apache.spark.sql.functions.expr("tTruePositive/(tTruepositive + tFalsePositive)"))
						.withColumn("recall", org.apache.spark.sql.functions.expr("tTruePositive/(tTruePositive + tFalseNegative)"))
						.withColumn("F-Measure", org.apache.spark.sql.functions.expr("2*precision*recall/(precision+recall)"));
		finalResult.unpersist();
		sparkSQL.stop();
	}
}