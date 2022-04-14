import org.apache.commons.lang3.Functions;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class Demo {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        String logFile = "src/main/resources/airport-codes.csv";
        /*SparkConf sparkConf = new SparkConf().setAppName("Demo").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> line = javaSparkContext.textFile(logFile);
        JavaRDD<String> word = line.flatMap( l -> Arrays.stream(l.split(" ")).iterator());
        Map<String, Long> countValues = word.countByValue();
        for(Map.Entry<String, Long> entry : countValues.entrySet()){
            System.out.println(entry.getKey() + " : " +entry.getValue());
        }*/


        SparkSession spark = SparkSession.builder().appName("Demo").master("local[*]").getOrCreate();
        Dataset<Row> airport = spark.read()
                .option("inferSchema", true)
                .option("header", true)
                .csv(logFile);
        System.out.println(airport.count());
        airport.show(4);
        airport.printSchema();
        Dataset<Row> filtered = airport.filter(airport.colRegex("type").equalTo("heliport"));
        filtered.show(4);
        System.out.println(spark.conf().getAll());
        System.out.println();




        Scanner sc = new Scanner(System.in);
        sc.next();


    }

}