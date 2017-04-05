package dh.com.mykafka

/**
  * Created by wdw on 2017/4/1.
  */
import java.util.Properties
import kafka.consumer.{ConsumerIterator, KafkaStream, ConsumerConfig, Consumer}
import kafka.message.MessageAndMetadata
import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{Await, Future, future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by tao on 6/25/15.
  */
object MultiThreadConsumer {
  def ZK_CONN     = "ubuntu:2181"
  def GROUP_ID    = "ss"
  def TOPIC       = "test"

  def main(args: Array[String]): Unit = {
    println(" 开始了 ")

    val connector = Consumer.create(createConfig())
    val topicCountMap = new HashMap[String, Int]()
    topicCountMap.put(TOPIC, 1) // TOPIC在创建时就指定了它有2个partition

    val msgStreams: Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]]
    = connector.createMessageStreams(topicCountMap)

    println("# of streams is " + msgStreams.get(TOPIC).get.size)
    // 变量futureIndex用来输出Future的序号
    var futureIndex = 0
    for (stream <- msgStreams.get(TOPIC).get) {
      processSingleStream(futureIndex, stream)
      futureIndex = futureIndex+1
    }
    // 主线程阻塞30秒
    Thread.sleep(30000)
    /* 注意，这里虽然主线程退出了，但是已经创建的各个Future任务仍在运行（一直在等待接收消息）
     * 怎样在主线程里结束各个Future任务呢？
     */
    println(" 结束了 ")
  }

  /**
    * 一个Future处理一个stream
    * TODO:  还需要一个可以控制Future结束的机制
    * @param futureIndex
    * @param stream
    * @return
    */
  def processSingleStream(futureIndex:Int, stream: KafkaStream[Array[Byte], Array[Byte]]): Future[Unit] = future {
    val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
    while (it.hasNext) {
      val data: MessageAndMetadata[Array[Byte], Array[Byte]] = it.next()
      println("futureNumer->[" + futureIndex + "],  key->[" + new String(data.key) + "],  message->[" + new String(data.message) + "],  partition->[" +
        data.partition + "],  offset->[" + data.offset + "]")
    }
  }

  def createConfig(): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", ZK_CONN)
    props.put("group.id", GROUP_ID)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    new ConsumerConfig(props)
  }
}
