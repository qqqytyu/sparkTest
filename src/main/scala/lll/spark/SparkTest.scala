package lll.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api._

import scala.collection.mutable.ListBuffer

@Test
class SparkTest{

    var conf: SparkConf = _

    var sc: SparkContext = _

    @BeforeEach
    def before(): Unit ={
        conf = new SparkConf().setAppName("lll test").setMaster("local")
        sc = new SparkContext(conf)
        sc.setLogLevel("WARN")
    }

    @AfterEach
    def after(): Unit ={
        sc.stop()
    }

    /**
      * 交集-intersection、并集-union、差集-subtract、笛卡儿积-cartesian
      * zip - 将两个单值的rdd变成tuple2的集合，两个rdd的元素个数必须一致
      * zipWithIndex -  将单值的rdd和自身索引变成tuple2的集合
      */
    @Test
    def UnionTest(): Unit ={
        val rdd01: RDD[Int] = sc.parallelize(1 to 10, 2)
        val rdd02: RDD[Int] = sc.parallelize(6 to 15, 2)
        //交集
        rdd01.intersection(rdd02).foreach(d => print(d + " "))
        println("\n=================")
        //并集
        rdd01.union(rdd02).foreach(d => print(d + " "))
        println("\n=================")
        //差集
        rdd01.subtract(rdd02).foreach(d => print(d + " "))
        println("\n=================")
        //笛卡儿积
        rdd01.cartesian(rdd02).foreach(d => print(d + " "))
        println("\n=================")
        rdd01.zip(rdd02).foreach(d => print(d + " "))
        println("\n=================")
        rdd01.zipWithIndex().foreach(d => print(d + " "))
    }

    /**
      * 去重
      * 数据是否相等通过equals判断
      */
    @Test
    def distTest(): Unit ={
        val rdd01: RDD[Int] = sc.parallelize(1 to 10, 2)
        val rdd02: RDD[Int] = sc.parallelize(5 to 15, 2)
        rdd01.union(rdd02).distinct().foreach(d => print(d + " "))
    }

    /**
      * 过滤
      */
    @Test
    def filterTest(): Unit ={
        val rdd: RDD[Int] = sc.parallelize(1 to 20, 2)
        rdd.filter(_<10).foreach(d => print(d + " "))
    }

    /**
      * 排序
      * 按key值排序，可以选择升序降序 - sortByKey
      * 按指定值排序，可以选择升序降序 - sortBy
      */
    @Test
    def sortTest(): Unit ={
        val rdd: RDD[(String, Int)] = sc.parallelize(
            List(("zhangsan", 1), ("lisi", 3), ("wangwu", 5), ("hanliu",2), ("liuqi", 4), ("dang ba", 6)))
        rdd.sortByKey().foreach(d => print(d + " "))
        println("\n=================")
        rdd.sortBy(_._2, ascending = false).foreach(d => print(d + " "))
    }

    /**
      * 抽样 - sample
      */
    @Test
    def sampleTest(): Unit ={
        val rdd: RDD[Int] = sc.parallelize(1 to 5, 2)
        //true - 放回抽样，false - 不放回抽样
        rdd.sample(withReplacement = false, 0.8).foreach(d => print(d + " "))
    }

    /**
      * 连接，只能用作KV RDD
      * cogroup - 在类型为(K,V)和(K,W)的RDD上调用，返回(K,(Iterable<V>,Iterable<W>))RDD
      * join - 内连接
      * left out join - 左外链接
      * right out join - 右外连接
      * full out join -  全外连接
      */
    @Test
    def joinTest(): Unit ={
        val rdd01: RDD[(String, Int)] = sc.parallelize(
            List(("zhangsan", 1), ("lisi", 3), ("wangwu", 5), ("hanliu",2), ("liuqi", 4), ("zhangsan", 6)))
        val rdd02: RDD[(String, Int)] = sc.parallelize(
            List(("zhangsan", 7), ("lisi", 9), ("wangwu", 11), ("hanliu",8), ("lisi", 10), ("wangba", 12)))
        rdd01.cogroup(rdd02).foreach(println)
        println("\n=================")
        rdd01.join(rdd02).foreach(println)
        println("\n=================")
        rdd01.leftOuterJoin(rdd02).foreach(println)
        println("\n=================")
        rdd01.rightOuterJoin(rdd02).foreach(println)
        println("\n=================")
        rdd01.fullOuterJoin(rdd02).foreach(println)
    }

    /**
      * partitions -  获取RDD的分区数组，可以通过数组获取分区数
      * getNumPartitions - 获取RDD的分区数
      * foreachPartition - 按分区进行foreach，action算子
      * repartition - 设置RDD的分区数，并必然产生shuffle
      * coalesce - 设置RDD的分区数，可选择是否产生shuffle
      *     当由多的分区变为少的分区的时候，可以不产生shuffle
      *     当由少的分区到多的分区的时候，建议产生shuffle
      * mapPartitions - 类似于map，但独立地在RDD的每一个分片上运行，在运行时注意迭代器链不要断掉
      * mapPartitionsWithIndex - 类似于mapPartitions，但带有一个整数参数表示分片的索引值
      */
    @Test
    def partitionTest(): Unit ={
        val rdd01: RDD[(String, Int)] = sc.parallelize(
            List(("zhangsan", 1), ("lisi", 3), ("wangwu", 5), ("hanliu",2), ("liuqi", 4), ("zhangsan", 6)),
            3)
        val rdd02: RDD[(String, Int)] = sc.parallelize(
            List(("zhangsan", 7), ("lisi", 9), ("wangwu", 11), ("hanliu",8), ("lisi", 10), ("wangba", 12)))
        println(rdd01.partitions.length)
        println("\n=================")
        println(rdd01.getNumPartitions)
        println("\n=================")
        rdd01.foreachPartition(_.foreach(println))
        println("\n=================")
        println(rdd01.repartition(5).getNumPartitions)
        println("\n=================")
        println(rdd01.coalesce(3).getNumPartitions)
        println("\n=================")
        //使用迭代器模式，防止迭代器链断掉
        rdd01.mapPartitions(tup =>new Iterator[(String, Int)]{
            println("open connect ...")
            override def hasNext: Boolean = tup.hasNext || {
                println("close connect ...")
                false
            }
            override def next(): (String, Int) = {
                val t: (String, Int) = tup.next()
                (t._1,t._2+1)
            }
        }).foreach(println)
        println("\n=================")
        //没有使用迭代器模式，打断了spark的迭代器链
        rdd01.mapPartitionsWithIndex((index, tup) =>{
            println("open connect ...")
            val tuples = new ListBuffer[(String,Int)]
            tup.foreach(t => tuples += ((t._1,t._2)))
            println("close connect ...")
            tuples.iterator
        }).foreach(println)
    }

    /**
      * map - 返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
      * flatMap - 类似于map，但是每一个输入元素可以被映射为0或多个输出元素
      * mapValues - 原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素
      */
    @Test
    def mapTest(): Unit ={
        val rdd01: RDD[Int] = sc.parallelize(1 to 10, 3)
        val rdd02: RDD[(String, Int)] = sc.parallelize(
            List(("zhangsan", 1), ("lisi", 3), ("wangwu", 5), ("lisi",1), ("liuqi", 3), ("wangwu", 5)),
            3)
        rdd01.map(_+3).foreach(d => print(d + " "))
        println("\n=================")
        rdd01.flatMap(d=>List(d,d+3)).foreach(d => print(d + " "))
        println("\n=================")
        rdd02.mapValues(_ + 3).foreach(println)
    }

    /**
      * groupBy - groupBy算子接收一个函数，这个函数返回的值作为key，然后通过这个key来对里面的元素进行分组
      * groupByKey - 在一个(K,V)的RDD上调用，返回一个(K, Iterator[V])的RDD (把相同的k合并，v放到一个iterator)
      * reduce - 将RDD中元素前两个传给输入函数，产生一个新的return值，
      *     新产生的return值与RDD中下一个元素（第三个元素）组成两个元素，再被传给输入函数，直到最后只有一个值为止
      * reduceByKey - 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起
      * aggregate - 接收两个函数，和一个初始化值。seqOp函数用于聚集每一个分区，combOp用于聚集所有分区聚集后的结果
      * aggregateByKey - 与aggregate类似,但操作的是(k,v)类型的值，且返回的是RDD
      */
    @Test
    def groupTest(): Unit ={
        val rdd: RDD[(String, Int)] = sc.parallelize(
            List(("zhangsan", 1), ("lisi", 3), ("wangwu", 5), ("lisi",1), ("liuqi", 3), ("wangwu", 5)),
            3)
        rdd.groupBy(_._2).foreach(println)
        println("\n=================")
        rdd.groupByKey().foreach(println)
        println("\n=================")
        println(rdd.reduce((v1, v2) =>{(v1._1 + v2._1, v1._2 + v2._2)}))
        println("\n=================")
        rdd.reduceByKey(_+_).foreach(println)
        println("\n=================")
        //zero为初始值("",0)，data为每个分区的值，第二次的zero为上次seqOp函数计算后的值
        //part1为seqOp函数计算完一个分区的值
        println(rdd.aggregate(("",0))(
            (zero, data) => (zero._1 + data._1, data._2 + zero._2),
            (part1, part2) => (s"${part1._1}${part2._1}", part1._2 + part2._2)
        ))
        println("\n=================")
        rdd.aggregateByKey(0)(
            (zero, data) => zero + data,
            _+_
        ).foreach(println)
    }

}
