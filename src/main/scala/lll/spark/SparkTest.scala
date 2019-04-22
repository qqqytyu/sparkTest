package lll.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api._

@Test
class SparkTest{

    var conf: SparkConf = _

    var sc: SparkContext = _

    @BeforeEach
    def before(): Unit ={
         conf = new SparkConf().setAppName("lll test").setMaster("local")
         sc = new SparkContext(conf)
    }

    @AfterEach
    def after(): Unit ={
        sc.stop()
    }

    @Test
    def joinTest(): Unit ={
        val rdd01: RDD[Int] = sc.parallelize(1 to 20, 2)
        val rdd02: RDD[Int] = sc.parallelize(20 to 30, 2)
        rdd01.union(rdd02).foreach(println)
    }

}
