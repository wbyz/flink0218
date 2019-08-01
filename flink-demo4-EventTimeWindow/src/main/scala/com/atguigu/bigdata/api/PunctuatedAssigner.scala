package com.atguigu.bigdata.api

import com.atguigu.bigdata.bean.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @author Witzel
  * @since 2019/7/31 20:35
  */
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading]{
    val bound: Long = 60 * 1000

    override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTS: Long): Watermark = {
        if (lastElement.id == "sensor_1") {
            new Watermark(extractedTS - bound)
        } else {
            null
        }
    }

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        element.timestamp
    }
}
