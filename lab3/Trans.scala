import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.streams.processor.{
  Cancellable,
  ProcessorContext,
  PunctuationType,
  Punctuator,
  StateStore
}
import org.apache.kafka.streams.state.{StoreBuilder, Stores}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.state.KeyValueIterator

/**
    * This is our transformer application
    */

object Trans extends App {

  /**
    * Local variables
    */
  val kvStore = createKVStore("brewery")
  // Here we can specify our window size, the default value is 20 seconds
  var windowSize: Long = 20000

  /**
    * Scala case class: store decoded stream data
    */
  sealed trait Foo

  //Our event structure for deserialization and serialization.
  case class Event(
      timestamp: Long,
      city_id: Long,
      city_name: String,
      style: String
  ) extends Foo

  case class Counter(city_id: Long, count: Long) extends Foo

  /**
    * Util functions
    */
  def createKVStore(name: String): KeyValueStore[Long, Long] = {
    val kvStoreBuilder: StoreBuilder[KeyValueStore[Long, Long]] =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(name),
        Serdes.Long,
        Serdes.Long
      )
    val kvStore: KeyValueStore[Long, Long] = kvStoreBuilder.build();
    kvStore
  }

  def createKVStoreBuilder(
      name: String
  ): StoreBuilder[KeyValueStore[Long, Long]] = {
    val kvStoreBuilder: StoreBuilder[KeyValueStore[Long, Long]] =
      Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(name),
        Serdes.Long,
        Serdes.Long
      )
    kvStoreBuilder
  }

  // Update the value in KV Dtore
  def updateCounter(kvStore: KeyValueStore[Long, Long], city_id: Long): Unit = {
    kvStore.get(city_id) match {
      // if there is no record, add into the kvstore, or update the record
      case 0 => kvStore.put(city_id, 1)
      case _ => kvStore.put(city_id, kvStore.get(city_id) + 1)
    }
  }

  // Check the value in KV Dtore and delete 0
  def deleteCounter(kvStore: KeyValueStore[Long, Long], city_id: Long): Unit = {
    kvStore.get(city_id) match {
      case 0 => {
        kvStore.delete(city_id)
      }
      case num if num >= 1 => kvStore.put(city_id, kvStore.get(city_id) - 1)
    }
  }

  // Overwrite default punctuator, this punctuator can be cancelled by schedule, we will cancel this Punctuator after the first run
  class myPunctuation() extends Punctuator {
    var schedule: Cancellable = _
    var myKVStore: KeyValueStore[Long, Long] = _
    var city_id: Long = _
    var myContext: ProcessorContext = _

    // Set the args for punctuate operations
    def setArgs(
        kvStoreA: KeyValueStore[Long, Long],
        city_idA: Long,
        myContextA: ProcessorContext
    ): Unit = {
      myKVStore = kvStoreA
      city_id = city_idA
      myContext = myContextA
    }

    override def punctuate(timestamp: Long): Unit = {
      deleteCounter(myKVStore, city_id)
      // Here we format our delete stream as JSON style
      val counterJson = Counter(city_id, myKVStore.get(city_id)).asJson.noSpaces
      myContext.forward(city_id.toString, counterJson)
      myContext.commit()
      // Cancel the schedule to make sure it is executed only once
      schedule.cancel()
    }
  }

  /**
    * Transformer API
    */
  class MyTransformer
      extends Transformer[String, String, KeyValue[String, String]] {
    var myContext: ProcessorContext = _;
    var myKVStore: KeyValueStore[Long, Long] = _;

    // Acquire default configs
    // The name of KVStore is hard written
    override def init(context: ProcessorContext): Unit = {
      this.myContext = context;
      this.myKVStore = myContext
        .getStateStore("brewery")
        .asInstanceOf[KeyValueStore[Long, Long]];

    }

    override def transform(
        key: String,
        value: String
    ): KeyValue[String, String] = {
      // Decode the input stream as case class Event
      val event = decode[Event](value).toSeq(0)
      val style = event.style
      val city_id = event.city_id
      // Every input record should be updated
      updateCounter(myKVStore, city_id)

      var cancelable: Cancellable = () => {};
      val punctuation = new myPunctuation()
      punctuation.setArgs(myKVStore, city_id, myContext)
      // Here we define a schedule to update records
      val scheduled: Cancellable = myContext.schedule(
        Duration.ofMillis(windowSize),
        // Use system time
        PunctuationType.WALL_CLOCK_TIME,
        punctuation
      )
      punctuation.schedule = scheduled

      // This return value would be printed in the update stream
      new KeyValue[String, String](
        city_id.toString,
        Counter(city_id, myKVStore.get(city_id)).asJson.noSpaces.toString
      )
    }

    override def close(): Unit = {
      val iter: KeyValueIterator[Long, Long] = myKVStore.all()
      while (iter.hasNext) {
        myKVStore.delete(iter.next().key)
      }
      iter.close()
    }
  }

  /**
    * TransformerSupplier: added into topology
    */
  class MyTransformerSupplier
      extends TransformerSupplier[String, String, KeyValue[String, String]] {
    override def get()
        : Transformer[String, String, KeyValue[String, String]] = {
      new MyTransformer()
    }
  }

  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "transformer")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-server:9092")
    p
  }

  var f: Array[String] = _;

  import scala.io.Source

  // Change the default window size
  if (args.length > 0){
    windowSize = args(0).toInt * 1000
  }
  // Here we read all the beer types
  val source = Source.fromFile("./beer.styles")
  f = source.getLines().toArray

  val builder: StreamsBuilder = new StreamsBuilder
  //  val another: StreamsBuilder = new StreamsBuilder
  var events: KStream[String, String] = builder.stream[String, String]("events")
  var updates: KStream[String, String] =
    builder.stream[String, String]("updates")

  /**
    * Events
    */

  builder.addStateStore(createKVStoreBuilder("brewery"))
  val trans = new MyTransformerSupplier()
    .asInstanceOf[TransformerSupplier[String, String, KeyValue[String, String]]]

  // filter beer types
  events = events.filter((key, value) =>
    f.contains(decode[Event](value).toSeq(0).style)
  )
  updates = events.transform(trans, "brewery")
  updates.foreach((key, value) => {
    println(value)
  })

  // specity the output stream
  updates.to("updates")

  val topology = builder.build()
  val streams: KafkaStreams = new KafkaStreams(topology, props)

  streams.start()

  // print topology info
  println(topology.describe())

  sys.ShutdownHookThread {
    val timeout = 10;
    streams.close(Duration.ofSeconds(timeout))
  }
}
