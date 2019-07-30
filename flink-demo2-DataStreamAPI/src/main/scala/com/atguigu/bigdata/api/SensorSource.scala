package com.atguigu.bigdata.api

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.collection.immutable
import scala.util.Random

/**
  * @author Witzel
  * @since 2019/7/30 18:20
  */
// 需要extends RichParallelSourceFunction , 泛型为SensorReading
class SensorSource extends RichParallelSourceFunction[SensorReading] {

    var running = true

    override def run(srcCtx: SourceContext[SensorReading]): Unit = {
        // 初始化随机数发生器
        val rand = new Random()

        // 初始化10个(温度传感器的id，温度值)元组
        var curFTemp: immutable.IndexedSeq[(String, Double)] = (1 to 10).map {
            // 产生高斯随机数
            i => ("sensor_" + i, 65 + (rand.nextGaussian() * 20))
        }

        // 无限循环，产生数据流
        while(running){
            // 更新温度
            curFTemp = curFTemp.map( t => (t._1, t._2 + (rand.nextGaussian() * 0.5)) )
            val curTime = Calendar.getInstance.getTimeInMillis

            // 发射新的传感器数据，注意这里srcCtx.collect
            curFTemp.foreach( t=> srcCtx.collect(SensorReading(t._1,curTime,t._2)))

            // wait for 100ms
            Thread.sleep(100)
        }
    }

    override def cancel(): Unit = {
        running = false
    }

}

// 传感器id，时间戳，温度
case class SensorReading(
                                id: String,
                                timestamp: Long,
                                temperature: Double
                        )