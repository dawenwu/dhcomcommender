package dh.com.mykafka

/**
  * Created by wdw on 2017/4/5.
  */
import java.util.Properties
//import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
//import org.apache.kafka.common.serialization.StringSerializer

case class ProduceMsg(val BROKER_LIST:String,val TOPIC:String) {
  //object ProduceMsg {
  // 设置配置属性

  private val props = new Properties()
  props.put("metadata.broker.list",this.BROKER_LIST)
  props.put("serializer.class","kafka.serializer.StringEncoder")
  props.put("request.required.acks","1")
  props.put("producer.type","async")

  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[String, String](this.config)

  def sendMsg(msg:String) {
     val message = new KeyedMessage[String, String](TOPIC, msg)
     producer.send(message)
  }

  def sendMsg(topid :String,key:String,msg:String) : Unit ={
      //producer.send(new ProducerRecord(topid, key,msg))
      val message = new KeyedMessage[String, String](topid, msg)
      producer.send(message)
  }
}
