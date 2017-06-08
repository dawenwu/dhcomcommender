package dh.com.commender
/**
  * Created by wdw on 3/9/17.
  */
//import org.apache.spark.SparkContext._
import java.util.ListIterator
import kafka.consumer.KafkaStream
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.SparkContext
import scopt.OptionParser
import java.util.ArrayList
import dh.com.mykafka.{ConsumeMsg,ProduceMsg}
import kafka.consumer.ConsumerIterator;

object moiveRecommender {

  val numRecommender = 10
  case class Params(
                 input: String = null,
                 numIterations: Int = 20,
                 lambda: Double = 1.0,
                 rank: Int = 10,
                 numUserBlocks: Int = -1,
                 numProductBlocks: Int = -1,
                 implicitPrefs: Boolean = false,
                 userDataInput: String = null)
  def main(args: Array[String]) : Unit = {
      val defaultParams = Params()
      val parser = new OptionParser[Params]("MoiveRecommender") {
      head("MoiveRecommender: an example app for ALS on MovieLens data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Int]("numUserBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numUserBlocks} (auto)")
        .action((x, c) => c.copy(numUserBlocks = x))
      opt[Int]("numProductBlocks")
        .text(s"number of product blocks, default: ${defaultParams.numProductBlocks} (auto)")
        .action((x, c) => c.copy(numProductBlocks = x))
      opt[Unit]("implicitPrefs")
        .text("use implicit preference")
        .action((_, c) => c.copy(implicitPrefs = true))
      opt[String]("userDataInput")
        .required()
        .text("use data input path")
        .action((x, c) => c.copy(userDataInput = x))
      arg[String]("<input>")
        .required()
        .text("input paths to a MovieLens dataset of ratings")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:|
          |bin/spark-submit --class com.zachary.ml.MoiveRecommender \
          |examples/target/scala-*/spark-examples-*.jar \
          |--rank 5 --numIterations 20 --lambda 1.0 \
          |data/mllib/u.data
        """.stripMargin)
    }

    val  kafkaMsg =new ConsumeMsg("ubuntu:2181","ss","test")
    val  producer =ProduceMsg("ubuntu:9092","OUTPUT")
    parser.parse(args, defaultParams).map { params =>
      onlinerecommendation(params,kafkaMsg,producer)
      //run(params)
    } getOrElse {
      System.exit(1)
    }
  }
  def readline(x:KafkaStream[Array[Byte], Array[Byte]]):Unit ={
       println(x)
  }
  def onlinerecommendation(params: Params,kafkaMsg:ConsumeMsg,outMsg:ProduceMsg): Unit = {
    //本地运行模式，读取本地的spark主目录
    var conf = new SparkConf().setAppName("Recommendation").setMaster("local[4]")
    val context = new SparkContext(conf)
    //加载数据
    val data = context.textFile(params.input)
    val ratings = data.map(_.split("\t") match {
      case Array(user, item, rate, time) => Rating(user.toInt, item.toInt, rate.toDouble)
    })
    //使用ALS建立推荐模型
    //也可以使用简单模式    val model = ALS.train(ratings, ranking, numIterations)
    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(ratings)

    while (true){
      val streamIter: ListIterator[KafkaStream[Array[Byte], Array[Byte]]] = kafkaMsg.getKafkaStream()

      while (streamIter.hasNext){
         val stream :KafkaStream[Array[Byte], Array[Byte]] =streamIter.next()
         while (stream.iterator().hasNext){
             val streamIterator : ConsumerIterator[Array[Byte], Array[Byte]]  = stream.iterator()
             val svalues:String = new String(streamIterator.next().message())

             val  id =svalues.split("\\|") match {
               case Array(id, age, sex, job, x) => (id)
             }
            var rs = model.recommendProducts(id.toInt, numRecommender)

            var value = ""
            var key = "0"
            //保存推荐数据到hbase中
            rs.foreach(r => {
             key = r.user.toString()
             value = value + r.product + ":" + r.rating + ","
            })
            outMsg.sendMsg(key+"-"+value)
         }
        Thread.sleep(10)
      }
    }
  }

  def run(params: Params) {
    //本地运行模式，读取本地的spark主目录
    var conf = new SparkConf().setAppName("Recommendation")
     // .setSparkHome("D:\\work\\hadoop_lib\\spark-1.1.0-bin-hadoop2.4\\spark-1.1.0-bin-hadoop2.4")
    .setMaster("local[4]")
    //集群运行模式，读取spark集群的环境变量
    //var conf = new SparkConf().setAppName("Moive Recommendation")
    val context = new SparkContext(conf)
    //加载数据
    val data = context.textFile(params.input)
    //val data = context.textFile("hdfs://ubuntu:8020/user/ml-20m/ml-20m/ratings.csv")
    /**
      * *MovieLens ratings are on a scale of 1-5:
      * 5: Must see
      * 4: Will enjoy
      * 3: It's okay
      * 2: Fairly bad
      * 1: Awful
      */
    val ratings = data.map(_.split("\t") match {
      case Array(user, item, rate, time) => Rating(user.toInt, item.toInt, rate.toDouble)
    })

    //使用ALS建立推荐模型
    //也可以使用简单模式    val model = ALS.train(ratings, ranking, numIterations)
    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(ratings)
    predictMoive(params, context, model)
    evaluateMode(ratings, model)
    //clean up
    context.stop()
  }
  /**
    * 模型评估
    */
  private def evaluateMode(ratings: RDD[Rating], model: MatrixFactorizationModel) {
    //使用训练数据训练模型
    val usersProducets = ratings.map(r => r match {
      case Rating(user, product, rate) => (user, product)
    })

    //预测数据
    val predictions = model.predict(usersProducets).map(u => u match {
      case Rating(user, product, rate) => ((user, product), rate)
    })

    //将真实分数与预测分数进行合并
    val ratesAndPreds = ratings.map(r => r match {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }).join(predictions)

    //计算均方差
    val MSE = ratesAndPreds.map(r => r match {
      case ((user, product), (r1, r2)) =>
        var err = (r1 - r2)
        err * err
    }).mean()

    //打印出均方差值
    println("Mean Squared Error = " + MSE)
  }

  /**
    * 预测数据并保存到hdfs中
    */
  private def predictMoive(params: Params, context: SparkContext, model: MatrixFactorizationModel) {
    var recommenders = new ArrayList[java.util.Map[String, String]]()
    val recommendershdfs =new ArrayList[String]()
    //读取需要进行电影推荐的用户数据
    val userData = context.textFile(params.userDataInput)
    userData.map(_.split("\\|") match {
      case Array(id, age, sex, job, x) => (id)
    }).collect().foreach(id => {
      //为用户推荐电影
      var rs = model.recommendProducts(id.toInt, numRecommender)
      var value = ""
      var key = 0
      var line =""
      //保存推荐数据到hbase中
      rs.foreach(r => {
        key = r.user
        value = value + r.product + ":" + r.rating + ","
      })

      //成功,则封装put对象，等待插入到Hbase中
      if (!value.equals("")) {
        var put = new java.util.HashMap[String, String]()
        put.put("rowKey", key.toString)
        put.put("t:info", value)
        line=key.toString()+":" + value
        recommenders.add(put)
        recommendershdfs.add(line)
      }
    })
    //new ConsumeMsg()
    val rdd = context.makeRDD(recommendershdfs.toArray())
    rdd.saveAsObjectFile("hdfs://ubuntu:8020/user/result")
    //保存到到HBase的[recommender]表中
    //recommenders是返回的java的ArrayList，可以自己用Java或者Scala写HBase的操作工具类，这里我就不给出具体的代码了，应该可以很快的写出
    //HbaseUtil.saveListMap("recommender", recommenders)
  }
}
