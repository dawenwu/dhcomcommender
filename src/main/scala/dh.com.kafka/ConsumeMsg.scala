package dh.com.mykafka
/**
  * Created by wdw on 2017/3/22.
*/
import java.util.{HashMap, ListIterator, Properties}
import kafka.consumer.{ConsumerIterator, KafkaStream, ConsumerConfig, Consumer}

class ConsumeMsg(val zkConnect:String,val groupID: String,val Topic:String)  {
  def createConsumerConfig():ConsumerConfig={
    val props = new Properties()
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupID)
    //props.put("topic",Topic)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    new ConsumerConfig(props)
  }
  val consumerConfig = createConsumerConfig()
  val connector = Consumer.createJavaConsumerConnector(consumerConfig)

  //def  consumeStream():Unit ={
  def  getKafkaStream(): ListIterator[KafkaStream[Array[Byte], Array[Byte]]] ={
    val topicCountMap = new HashMap[String, Integer]()
    // 设置每个topic开几个线程
    topicCountMap.put(Topic,1)
    // 获取stream  Map[String, List[KafkaStream[byte[], byte[]]]]
    val streams = connector.createMessageStreams(topicCountMap)
    val stream = streams.get(Topic)
    return stream.listIterator()
  }

}


