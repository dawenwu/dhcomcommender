package dh.com.kafka

/**
  * Created by wdw on 2017/4/1.
  */
import java.util.Properties
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
//import scala.concurrent.{Await, Future, future}
object ProduceKeyedMsg {
  def BROKER_LIST = "ubuntu:9092,ecs2:9092"
  def TOPIC = "my-2nd-topic"
  def main(args: Array[String]): Unit = {
    println("开始产生消息！")
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)
    for (i <- 0 to 10) {
      val ret= producer.send(new ProducerRecord(TOPIC,"key-" + i,"msg-" + i))
      val metadata = ret.get //打印出 metadata
      println("i="+i+",offset=" + metadata.offset()+",partition="+metadata.partition())
    }
    producer.close
  }
}