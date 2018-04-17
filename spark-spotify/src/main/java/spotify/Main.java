package spotify;

import java.util.Iterator;
import com.google.common.collect.Lists;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

public class Main {
  static int NF = 7;
  static String IFS = ",";

  /**
   * Used to filter out bad input in data set. [position, trackname, artist, streams, url, date,
   * region]
   */
  public static boolean isValid(String[] properties) {
    if (properties.length != NF)
      return false;
    try {
      Integer.parseInt(properties[0]);
      Integer.parseInt(properties[3]);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static void main(String[] args) throws Exception {
    String inputFile = args[0];
    String outputFolder = args[1];
    SparkConf conf = new SparkConf().setAppName("spotify.Main");
    JavaSparkContext sc = new JavaSparkContext(conf);
    // Format is [position, trackname, artist, streams, url, date, region]
    JavaRDD<String> input = sc.textFile(inputFile);

    // Split csv into a rows RDD and filters out bad records
    JavaRDD<String> rows = input.flatMap(new FlatMapFunction<String, String>() {
      private static final long serialVersionUID = 1L;

      public Iterator<String> call(String x) {
        return Lists.newArrayList(x.split("\n")).iterator();
      }
    }).filter(new Function<String, Boolean>() {
      private static final long serialVersionUID = 1L;

      public Boolean call(String s) {
        return isValid(s.split(IFS));
      }
    });
    // Creates RDD pair of (artist, streams), then writes output
    JavaPairRDD<String, Long> rdd = rows.mapToPair(new PairFunction<String, String, Long>() {
      private static final long serialVersionUID = 1L;

      public Tuple2<String, Long> call(String row) {
        String[] properties = row.split(IFS);
        String artist = properties[2];
        long streams = Long.parseLong(properties[3]);
        return new Tuple2<String, Long>(artist, streams);
      }
    }).reduceByKey(new Function2<Long, Long, Long>() {
      private static final long serialVersionUID = 1L;

      public Long call(Long i1, Long i2) {
        return i1 + i2;
      }
    });
    rdd.coalesce(1).saveAsTextFile(outputFolder);
    sc.close();
  }
}
