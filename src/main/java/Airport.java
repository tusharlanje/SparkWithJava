import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Airport {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        String logFile = "src/main/resources/airport-codes.csv";
        SparkConf sparkConf = new SparkConf().setAppName("Demo").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> line = javaSparkContext.textFile(logFile);
        JavaRDD<String> heliports = line.filter(l -> l.split(",")[1].equalsIgnoreCase("heliport"));
        //JavaRDD<String> elevation = line.filter(l -> Float.valueOf(l.split(",")[3])<40);
        JavaRDD<String> elevation = heliports.map(l -> {
            String splits[] = l.split(",");
            return StringUtils.join(new String[]{
                    splits[1], splits[2]
            });
        });
        System.out.println(elevation.count());
        elevation.take(10).forEach(f -> System.out.println(f));
     }
}
