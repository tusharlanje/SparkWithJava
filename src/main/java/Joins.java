import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Joins {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf()
                .setAppName("Joins")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> username = sc.textFile("src/main/resources/username.csv");
        JavaRDD<String> password = sc.textFile("src/main/resources/username-password-recovery-code.csv");


        JavaRDD<String> usernameIdentifier = username.map(l -> l.split(",")[1]);
        JavaRDD<String> passwordIdentifier = password.map(l -> l.split(",")[1]);
        JavaRDD<String> intersectResult = usernameIdentifier.intersection(passwordIdentifier);
        JavaRDD<String> unionResult = username.union(password);
        intersectResult.collect().forEach(f -> System.out.println(f));

    }
}
