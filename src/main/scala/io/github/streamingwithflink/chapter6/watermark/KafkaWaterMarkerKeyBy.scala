package io.github.streamingwithflink.chapter6.watermark

import java.util.Properties

import io.github.streamingwithflink.util.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @author  gl
  *          写此类的目的是为了测试几个一直以来的疑惑：
  *          1.当kafka两个分区的时候，如果我只往kafka的0分区写入数据，那么此时flink的时间时钟是否还能推进
  *          2.当往kafka两个分区都写入数据，但是给flink解析的key却都是a, 这样是否会导致水位线无法推动，a相关的key都到了某一个分区，其他分区task无数据消费，导致时间时钟无法推进？
  *          3.当往kafka某个分区，一直写入不同key的数据时，是否可以将时间时钟推进？
  *          4.当kafka topic是2个分区，但是flink程序并行度设置的是10个并行度，10个map -> 10 个watermaker，这样水位线能推动吗
  *          带着这三个疑惑，来测试一下
  *
  *          核心理论：上游算子的水位线会广播到所有下游！ 是所有下游(而不是基于键值发送，这里可以看书27页)，即使是多流合并，但对于算子而言，只会取最小的水位线！
  *          只有上游水位线广播到所有下游，才可以实现分布式中各个机器的水位线保持一致！另外，shuffle过程拉取数据归拉取数据
  *          和水位线没关系，水位线是从数据入口处直接开始定义的! 所以一条流最开始的水位线是每个source中统一的，然后广播到所有下游算子中
  *          比如：单流中三个source -> 三个map -> 三个watermark -> keyby -> 三个window 那么三个watermark各自维护自己上游的一个source的水位线
  *          虽然三个水位线可能不一样，但是到了keyby阶段，会根据设置时间(例如十秒)广播一次到下游三个windows时，每个windows会获得三个上游广播来的水位线，此时取最小的水位线即可！
  *          至于多个流也没关系，在下游只要有合并的地方，例如keyby -> windows, 那么合并后的算子水位线是多个流中最小的水位线即可！
  *
  *          这里可以看网址图片：https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/event_timestamps_watermarks.html#watermark-strategies-and-the-kafka-connector
  *          看最下面的图片和多流的思路是一样的
  *
  *          结论：
  *          1.不能，当只往kafka的一个分区里写入数据的时候，flink的时间时钟不会推进，这是因为水位线设置的入口在kafka消费者的正后方，所以入口处就是kafka的两个分区，所以只要有一个分区不进入数据，那么水位线就不会推动！
  *          2.不会，只要数据从kafka两个入口处有数据进入，是可以推动时间时钟，这是因为水位线是广播发送的，不过如果flink keyby的key都是a，那么结果会是只在某个taskManager中打印，会造成数据倾斜罢了，而且水位线是上游算子广播到下游，所以其他taskManager也会收到同一时刻的水位线！！
  *          3.不行，即使是往一个分区中写多个不同key数据，也不可以将时间时钟推进，除非你通过设置source并行度为1,后面有解释
  *          4.可以推动，虽然flink 连接kafka消费者是按照topic分区号 % flink并行度 来确定source的，但是项目启动的时候，如果发现多余的source，此时会自动过滤多余source的watermaker
  *          只采用正确的，可以获得数据的watermaker来作为判断水位线的标准
  *
  *          还有一个结论：水位线一般我们是使用周期分配的，例如10s生成一次水位线:.setAutoWatermarkInterval(1000L * 10) 那么水位线生成后会直接广播到所有下游！是所有下游！
  *          那么也就是说，即使三台物理机器，分布式管理一个窗口函数，那么三台物理机都会获得同样的水位线，所以就一定可以同一时间触发窗口函数！！
  *          这样就实现了分布式窗口结束时间的统一！十分的方便！！
  *
  *          注意：watermarker的设置位置也很重要，下面的设置是正确的设置，在kafka消费者后面map后直接设置水位线，如果source，map的并行度都是2，那么后面就会有两个并行度的watermarke
  *          此时如果kafka只有一个分区有数据，那么流向就会是一个source -> 一个map -> watermarke， 那另一个watermarker就永远不会推进，这就导致后面的 windows窗口只有一个输入分区有数据
  *          另一个输入分区一直没有输入，这样就会导致水位线无法推动！
  *          那么如果source设置成1个并行度，后面map，watermakr 即使是两个并行度, kafka只有一个分区有数据，水位线还是会继续推动，这是因为一个source 到两个map 的分发是rebound 轮询发送消息，所以后面
  *          两个map和watermaker都会有数据，所以下游的windows的两个分区都有数据，水位线就会继续推进
  *          但是如果是两个source -> 两个map -> keyby()函数后再设置两个watermarke水位线，kafka只有一个分区有数据，水位线不会推动，
  *          甚至kafka两个分区都有数据，也不保证能推动水位线，必须要两个分区的数据都是不同的key，才可以推动
  *          这是因为map后的keyby 是hash分发数据到两个watermaker，但是每个watermarke都需要两个分区才可以推动，所以如果key数据倾斜
  *          即使kafka两个分区都有数据，也会造成某个 watermarke一个分区有数据，另一个分区没数据导致无法推动水位线！
  *          所以强烈建议在数据入口处或者souce阶段就注册水位线
  *
  *          另外flink 新版本中集成了连接kafka source处直接设置水位线的方法，十分方便，并且也实现了如果kafka某个分区失效
  *          可以根据设置将某个分区设置为无效，从而不会影响水位线的推动！
  *
  *          还有一点就是 ，窗口会根据你的数据的实际事件时间来规划放在哪个窗口里面 ，并不会按照水位线来规划放在哪个窗口里面
  *          水位线只不过是用来触发窗口操作的标准！！！！
  *
  *
  *          心得：通过这次实践，对于flink的水位线，窗口，和各个taskManager的理解更上一层，总结一下
  *          关于水位线其实就是根据代码编写的位置为起点，开始记录各个入口数据中的最小值作为水位线发送给下流程序
  *          如果有一个入口没有数据写入，则会一直停止水位线的推动，但是数据还是会进入下游继续逻辑操作，只不过需要水位线触发的算子则永远不会触发
  *
  *          延伸心得：其实窗口操作很简单，底层就是MapState<时间, listState>状态管理罢了，数据属于哪个窗口的时间段，就放在哪个key里面的list即可！
  *          当上游的水位线发送过来后，onTimer回调函数中中对比一下窗口的结尾是否到达触发点即可！！！ 到达了就直接触发呗~
  *
  */
object KafkaWaterMarkerKeyBy {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //这里和kafka的分区数量保持一致
    env.setParallelism(4)
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //下面是每10秒生成一次水位线
    env.getConfig.setAutoWatermarkInterval(1000L * 10)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val checkpointPath = "hdfs://169.254.106.100:9000/flink/checkpoints"
    val config = new Configuration()
    config.setBoolean("state.backend.async", true)
    env.setStateBackend(new FsStateBackend(checkpointPath).configure(config))

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "169.254.106.100:9092")
    properties.setProperty("group.id", "test-wartermark-keyby")
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getCanonicalName);
    properties.setProperty("value.deserializer", classOf[StringDeserializer].getCanonicalName);
    properties.setProperty("auto.offset.reset", "earliest")

    val myConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String](
      "test-wartermark-bykey"
      , new SimpleStringSchema() //反序列化工具转成string 这个类是flink给我们实现的
      , properties
    )
    //myConsumer.setStartFromEarliest()
    val stream: DataStream[String] = env.addSource(myConsumer) //.setParallelism(1)//这里设置并行度会让水位线永远推进，因为下面的map会轮询获得数据

    val value: DataStream[SensorReading] = stream.map((x: String) => SensorReading(x.split(",")(0), x.split(",")(1).toLong, x.split(",")(2).toDouble))
      //.keyBy((_: SensorReading).id) //不建议用，没有软用还费劲
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(element: SensorReading): Long = {
            element.timestamp
          }
        }) //设置水位线
      .keyBy((_: SensorReading).id)
      .timeWindow(Time.seconds(5)) //窗口五秒
      .reduce((r1: SensorReading, r2: SensorReading) => {
        SensorReading(r1.id, r2.timestamp, r1.temperature + r2.temperature) //窗口函数，单词计数
      })

    value.print("windows:::")

    env.execute()
  }

}
