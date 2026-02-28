import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;

public class Addition {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("Addition");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("input.txt");

        int total = lines
            .flatMap(line -> Arrays.asList(line.split(",")).iterator())
            .map(num -> Integer.parseInt(num.trim()))
            .reduce((a, b) -> a + b);

        System.out.println("=================================");
        System.out.println("Sum of all numbers: " + total);
        System.out.println("=================================");

        sc.close();
    }
}
