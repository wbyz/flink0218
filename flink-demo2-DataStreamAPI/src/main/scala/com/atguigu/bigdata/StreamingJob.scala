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

import com.atguigu.bigdata.api.{MyMapFunction, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

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
object StreamingJob {
  def main(args: Array[String]) {
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

    // TODO 1、数据来源四： 自定义Source
    /*
    env
            .addSource(new SensorSource)
//            .map(new MyMapFunction)   // TODO 2、自定义mapFunction
            .filter(r => r.temperature >= 25) // TODO 3、filter
            .print()*/

    // TODO 4、keyBy 操作：第0号元素分组聚合， 第1号元素累加， 第2号元素不操作，同组内只保留第一个集合的元素
    // 又因为是流处理，所以每有一个元素就会进行一次累加和打印
    /* 输出结果
    (1,2,2)
    (2,3,1)
    (2,5,1)
    (1,7,2)
     */
    val stream = env
                    .fromElements((1,2,2),(2,3,1),(2,2,4),(1,5,3))
                    .keyBy(0)
                    .sum(1)
                    .print()

    env.execute("Flink Streaming Scala API Skeleton")
  }



}
