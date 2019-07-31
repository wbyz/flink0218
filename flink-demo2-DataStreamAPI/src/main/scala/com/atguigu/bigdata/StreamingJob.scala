/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.atguigu.bigdata


import java.util.Calendar

import com.atguigu.bigdata.api.{MyFilterFunction, MyMapFunction, SensorReading, SensorSource}
import org.apache.flink.api.common.functions.{RichFilterFunction, RuntimeContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.collection.immutable
import scala.util.Random

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
/**
  * @author Witzel
  * @since 2019/7/30 20:09
  *
  */
object StreamingJob {
    def main(args: Array[String]) {
        // TODO 一、environment
        // set up the streaming execution environment 获取执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 设置动态并行度为1， 如果不设置， 那么默认为当前机器的cpu的数量
        env.setParallelism(1)
        // 设置流的时间为Event Time
        //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 多个数据来源
        /*    // 数据来源一：直接做推送一个列表，模拟一个流
          val sensorReadings = env.fromCollection(List(
            SensorReading("sensor_1", 1547718199, 35.80018327300259),
            SensorReading("sensor_6", 1547718199, 15.402984393403084),
            SensorReading("sensor_7", 1547718199, 6.720945201171228),
            SensorReading("sensor_10", 1547718199, 38.101067604893444)
          ))

          val stream = sensorReadings
                          .keyBy("id")
                          .timeWindow()

          // 数据来源二：从文件获取数据
          var stream = env.readTextFile(filePath)

          // 数据来源三：以kafka的消息队列的数据作为数据来源
          val properties = new Properties()
          properties.setProperty("bootstrap.servers",
                                 "localhost:9092")
          properties.setProperty("group.id",
                                 "consumer -group")
          properties.setProperty("key.deserializer",
                                 "org.apache.kafka.common.serialization.StringDeserializer")
          properties.setProperty("value.deserializer",
                                 "org.apache.kafka.common.serialization.StringDeserializer")
          properties.setProperty("auto.offset.reset",
                                 "latest")

          val env = StreamExecutionEnvironment.getExecutionEnvironment
          env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
          env.setParallelism(1)
          val stream = env
          // source为 来 自Kafka的 数 据， 这 里 我 们 实 例 化 一 个 消 费 者， topic为hotitems
              .addSource(new FlinkKafkaConsumer[String]("hotitems",
                                                         new SimpleStringSchema(),
                                                         properties))

     */
        // TODO 二、数据来源四： 自定义Source
        /*    env
                .addSource(new SensorSource)
                .print()*/

        // TODO 三、Basic Transformations(基本转换算子)
        /*
        env
            .addSource(new SensorSource)
    //      .map(new MyMapFunction)   // TODO 1、自定义mapFunction
            .filter(r => r.temperature >= 25) // TODO 2、filter 算子
            .print()*/
        // TODO 3、sum/max/min...
        // 又因为是流处理，所以每有一个元素就会进行一次累加和打印
        /* 输出结果
    (1,2,2)
    (2,3,1)
    (2,5,1)
    (1,7,2)
     */
        /*    val stream = env
                    .fromElements((1,2,2),(2,3,1),(2,2,4),(1,5,3))
                    .keyBy(0)
                    .sum(1)
                    .print()*/

        // TODO 4、reduce 算子
        /*  输出结果
    (en,List(tea))
    (fr,List(vin))
    (en,List(tea, cake))
     */
        /*    val inputStream = env
                .fromElements( ("en", List("tea")), ("fr", List("vin")), ("en", List("cake")) )
                .keyBy(0)
                .reduce((x,y)=> (x._1, x._2 ::: y._2))
                .print()*/

        // TODO 三、KeyedStream Transformations(键控流转换算子)
        // TODO 1、keyBy 算子 ：第0号元素分组聚合， 第1号元素累加， 第2号元素不操作，同组内只保留第一个集合的元素  （已经在之前几个算子中用到了）

        // TODO 四、Multistream Transformations(多流转换算子)
        // TODO 1、Union
        //将事件类型相同的多条 DataStream 合并到一起，在进入到合流时，使用 FIFO 先进先出的原则。Union 算子不会对事件的顺序做处理。
        /*val parisStream: DataStream[SensorReading] = ...
    val tokyoStream: DataStream[SensorReading] = ...
    val rioStream: DataStream[SensorReading] = ...
    val allCities: DataStream[SensorRreading] = parisStream.union(tokyoStream , rioStream)*/

        // TODO 2、Connect, Comap and Coflatmap
        // 为了获得确定性的结果，connect 必须和 keyBy 或者 broadcast 一起使用。
        // TODO 2-1 ConnectedStreams
        /* ConnectedStream 提供了 map 和 flatMap 方法。
     map: 需要 CoMapFunction 作为参数
     flatMap: 需要 CoFlatMapFunction 作为参数*/
        /*// first stream
    val first: DataStream[Int] = ...
    // second stream
    val second: DataStream[String] = ...
    // connect streams
    val connected: ConnectedStreams[Int, String] =
    first.connect(second)*/

        // TODO 2-2 Comap
        // TODO 2-3 Coflatmap
        /* CoMapFunction 和 CoFlatMapFunction
    都需要两条输入流的类型，
    还需要输出流的类型，
    还需要定义两个方法，一个方法对应一条流。
    map1()和 flatMap1() 处理第一条流，
    map2() 和 flatMap2() 处理第二条流。*/
        /*  // IN1: 第一条流的事件类型
        // IN2: 第二条流的事件类型
        // OUT: 输出流的事件类型
        CoMapFunction[IN1, IN2, OUT]
        > map1(IN1): OUT
        > map2(IN2): OUT

        CoFlatMapFunction[IN1, IN2, OUT]
        > flatMap1(IN1, Collector[OUT]): Unit
        > flatMap2(IN2, Collector[OUT]): Unit*/

        // with keyBy
        /*
        val one: DataStream[(Int, Long)] = ...
        val two: DataStream[(Int, String)] = ...

        // keyBy two connected streams
        val keyedConnect1: ConnectedStreams[(Int, Long), (Int, String)] = one
                .connect(two)
                // key both input streams on first attribute 在第一个属性上键入两个输入流
                .keyBy(0, 0)

        // alternative: connect two keyed streams
        val keyedConnect2: ConnectedStreams[(Int, Long), (Int, String)] = one
                .keyBy(0)
                .connect(two.keyBy(0))
    */
        // with broadcast
        /*
    val first: DataStream[(Int, Long)] = ...
    val second: DataStream[(Int, String)] = ...

    // connect streams with broadcast
    val keyedConnect: ConnectedStreams[(Int, Long), (Int, String)] = first
            // broadcast second input stream
            .connect(second.broadcast())
*/

        /*
          TODO Connect 与 Union 区别：
          • Union 之前两个流的类型必须是一样， Connect 可以不一样，在之后
          的 CoMapFunction 中再去调整成为一样的。
          • Connect 只能操作两个流， Union 可以操作多个。
       */

        // TODO 2-4 Split and Select
        /*
        //Split 是 Union 的反函数。
        //1 // IN: the type of the split elements
        //2 OutputSelector[IN]
        //3 > select(IN): Iterable[String]*/
        /*
        val inputStream: DataStream[(Int, String)] = ...

        val splitted: SplitStream[(Int, String)] = inputStream
        .split(t => if (t._1 > 1000) Seq("large") else Seq("small"))

        val large: DataStream[(Int, String)] = splitted.select("large")
        val small: DataStream[(Int, String)] = splitted.select("small")
        val all: DataStream[(Int, String)] = splitted.select("small", "large")*/

        // TODO 五、实现 UDF 函数
        // TODO 1 Function Classes
        // 例如 MapFunction, FilterFunction, ProcessFunction
        /*env.addSource(new SensorSource)
//            .filter(new MyFilterFunction)
            // 匿名类
            .filter(new RichFilterFunction[SensorReading] {
              override def filter(value: SensorReading): Boolean = {
                value.temperature > 25
              }
            })
            .print()*/

        // TODO 六、Sink  --> Elasticsearch
        /*
        val stream = env.fromCollection(List(
            "a",
            "b"
        ))

        val httpHosts = new java.util.ArrayList[HttpHost]
        httpHosts.add(new HttpHost("hadoop102", 9200, "http"))

        val esSinkBuilder = new ElasticsearchSink.Builder[String](
            httpHosts,
            new ElasticsearchSinkFunction[String] {
                def createIndexRequest(element: String): IndexRequest = {
                    val json = new java.util.HashMap[String, String]
                    json.put("data", element)

                    Requests.indexRequest()
                            .index("my-index")
                            .`type`("my-type")
                            .source(json)
                }

                override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
                    indexer.add(createIndexRequest(element))
                }
            }
        )

        // finally, build and add the sink to the job's pipeline
        stream.addSink(esSinkBuilder.build)
*/

        // TODO 七、Distribution Transformations(分布式转换算子)
        // Random
        // 随机数据交换由DataStream.shuffle()方法实现。shuffle方法将数据随机的分配到并行的任务中去。

        // Round-Robin
        // Round-Robin是一种负载均衡算法。可以将数据平均分配到并行的任务中去。

        // Rescale
        // rescale方法使用的也是round-robin算法，但只会将数据发送到接下来的task slots中的一部分task slots中。

        // Broadcast
        // broadcast方法将数据复制并发送到所有的并行任务中去。

        // Global
        // global方法将所有的数据都发送到下游算子的第一个并行任务中去。这个操作需要很谨慎，因为将所有数据发送到同一个task，将会对应用造成很大的压力。

        // Custom
        // 自定义数据分配策略。

        //

        env.execute("Flink Streaming Scala API Skeleton")
    }



}
