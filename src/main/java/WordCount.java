import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.Arrays;
import org.apache.spark.api.java.function.FlatMapFunction;

public class WordCount {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        String logFile = "src/main/resources/DemoTextFile";
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("WordCount")
                .getOrCreate();

        Dataset<String> lines = spark.read().textFile(logFile);
        lines.show(2);
        Dataset<String> words = lines.flatMap((FlatMapFunction<String, String>) l -> Arrays.asList(l.split(" ")).iterator(), Encoders.STRING());
        words.show(2);
        Dataset<Row> counts = words.groupBy("value").count().toDF("word","count");
        counts.show(2);
        counts.createOrReplaceTempView("counts");
        spark.sql("select * from counts where count > 2").show(2);


    }
}