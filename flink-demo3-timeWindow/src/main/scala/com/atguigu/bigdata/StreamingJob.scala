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

import com.atguigu.bigdata.api.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

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
        // 设置并行度
        env.setParallelism(1)
        // 设置流的时间为Eventime
        //    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // TODO 一、TimeWindow
        // TimeWindow是将指定时间范围内的所有数据组成一个Window，一次对一个Window里面的所有数据进行计算。
        // TODO 1、滚动窗口
        /*
        // Flink默认的时间窗口根据Processing Time进行窗口的划分，将Flink获取到的数据根据进入Flink的时间划分到不同的窗口中。
        val stream = env
                        .addSource(new SensorSource)
                        .map(r => (r.id, r.temperature))
                        // 按照传感器id分流
                        .keyBy(_._1)
                        // timeWindow(窗口大小，[滑动距离]) 只有第一个参数就是滚动窗口，有两个参数就是滑动窗口
                        //时间间隔可以通过Time.milliseconds(x)，Time.seconds(x)，Time.minutes(x)等其中的一个来指定。
                        .timeWindow(Time.seconds(15))   //,Time.seconds(5))
                        .reduce((r1,r2)=>(r1._1,r1._2.min(r2._2)))
                        .print()
        */

        // TODO 2、滑动窗口
        // 滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是window_size，一个是sliding_size。
        val minTempPerWindow: DataStream[(String, Double)] = env
                .addSource(new SensorSource)
                .map(r => (r.id, r.temperature))
                // 按照传感器id分流
                .keyBy(_._1)
                .timeWindow(Time.seconds(15), Time.seconds(5))
                .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))

        minTempPerWindow.print()

        // execute program
        env.execute("Flink Streaming Scala API Skeleton")
    }
}
