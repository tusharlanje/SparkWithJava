import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class MainClass {
    public static void main(String[] args) {
        //URL: http://NGL001412.persistent.co.in:4040
        System.out.println("start");
        Logger.getLogger("org").setLevel(Level.ERROR);
        String logFile = "src/main/resources/DemoTextFile"; //C:\Hadoop/README.md
        SparkSession spark = SparkSession.builder().appName("Simple Application")
                .master("local[*]")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(logFile).javaRDD();
        System.out.println(lines.getNumPartitions());
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println(words.getNumPartitions());

        Dataset<String> d1 = spark.createDataset(words.rdd(), Encoders.STRING());
        System.out.println(d1.rdd().getNumPartitions());

        spark.conf().set("spark.sql.shuffle.partitions",10);

        Dataset<Row> counts = d1.groupBy("value").count().toDF("word", "count");
        counts.show(5);
        System.out.println(counts.rdd().getNumPartitions());
        //counts.sort(functions.desc("count")).show(5);

        List<String> data = Arrays.asList("hello", "world");
        Dataset<String> ds = spark.createDataset(data, Encoders.STRING());

        Dataset<String> flatMapped = ds.flatMap((FlatMapFunction<String, String>) s -> {
            List<String> ls = new LinkedList<>();
            for (char c : s.toCharArray()) {
                ls.add(String.valueOf(c));
            }
            return ls.iterator();
        }, Encoders.STRING());


        Scanner scanner = new Scanner(System.in);
        scanner.next();

        System.out.println("End");
        //spark.stop();
    }
}
