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

import com.atguigu.bigdata.api._
import com.atguigu.bigdata.bean.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

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
        // set up the streaming execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // TODO 一、Configure Time Characteristic
        // TODO 1、Event Time
        // 一般只在Event Time无法使用时，才会被迫使用Processing Time或者Ingestion Time。
        // 只有在使用EventTime下，才会有乱序的情况
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // TODO 2、Watermark
        /*
        * Watermark是一种衡量Event Time进展的机制，它是数据本身的一个隐藏属性，数据本身携带着对应的Watermark。
        * Watermark是用于处理乱序事件的，而正确的处理乱序事件，通常用Watermark机制结合Window来实现。
        * 数据流中的Watermark用于表示timestamp小于Watermark的数据，都已经到达了，因此，Window的执行也是由Watermark触发的。
        * Watermark可以理解成一个延迟触发机制，我们可以设置Watermark的延时时长t，每次系统会校验已经到达的数据中最大的maxEventTime，
         然后认定Event Time小于maxEventTime - t的所有数据都已经到达，如果有窗口的停止时间等于maxEventTime – t，那么这个窗口被触发执行。
         */
        // TODO 3、TimestampAssigner
        /*
        Flink暴露了TimestampAssigner接口供我们实现，使我们可以自定义如何从事件数据中抽取时间戳。
        自定义MyAssigner有两种类型
            AssignerWithPeriodicWatermarks      // 周期性水印
            AssignerWithPunctuatedWatermarks    // 不时打断得水印
        */
        /*  TODO  注意：水印插入注意点
            插入水印可以有延迟，收到的数据，在过200ms延迟后才插入水印，
            此时数据和水印之间可能会有其他数据插入，若此水印没过水位线，就会闭合水位线以下所有窗口
            例如1, 4, 6, w(4), 2, 9, [w(7)], 8, 5, 7, w(7), 10, 3
            最大延迟时间是2s
            滑动窗口长度是5s，滑动距离2s

            当flink程序碰到 1 时，把 1 事件分配到[0:5)
            当flink程序碰到 4 时，把 4 事件分配到[0:5), [2:7), [4:9)

            在6时，插入水印w(4) 但小于水位线w(5) 不关闭窗口
            在9时，插入水印w(7)，但有水印插入延迟周期，在过7后才插入水印，此时没过第二个窗口水位线，直接关闭[0:5), [2:7)两个窗口
            // 同理只要没过一个窗口线，那么该窗口及之前的所有窗口都会被关闭，若设置合理不会存在多个窗口未关闭

            对于批处理来讲，数据结构是(k, v)
            对于流处理来讲，数据结构是(k, v, EventTime, WindowEndTime)
            所以在flink遇到 4 时，4的数据结构应该为如下，且在三个窗口中
            ("sensor_1", "10', 4)
            ("sensor_1", "10", 4, 5)
            ("sensor_1", "10", 4, 7)
            ("sensor_1", "10", 4, 9)

            之后讨论超时数据处理，有三种情况：抛弃、放回对应窗口、在下一个窗口接收
         */
        val stream = env
                        .addSource(new SensorSource)
                // TODO 3-1、Assigner with periodic watermarks分配时间戳和水印（用于无序，可自定义AssignerWithPeriodicWatermarks）
//                        .assignTimestampsAndWatermarks(new PeriodicAssigner )
                // TODO 3-2、Assigner with Ascending watermarks 分配递增水印 （用于有序，直接使用数据的时间戳生成水印）
//                        .assignAscendingTimestamps(e => e.timestamp)
                // TODO 3-3、能大致估算出数据流中的事件的最大延迟时间 自定义BoundedOutOfOrdernessTimestampExtractor
//                        .assignTimestampsAndWatermarks(new MaxTardinessAssigner)
                // TODO 3-4、Assigner with punctuated watermarks 指定数据流插入水印 (没啥应用场景。。。。)
                        .assignTimestampsAndWatermarks(new PunctuatedAssigner )

        // execute program
        env.execute("Flink Streaming Scala API Skeleton")
    }
}











