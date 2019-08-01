package com.atguigu.bigdata.api

import com.atguigu.bigdata.bean.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  * @author Witzel
  * @since 2019/7/31 18:44
  */
// 周期性水印

class MaxTardinessAssigner  extends AssignerWithPeriodicWatermarks[SensorReading]{
    val bound: Long = 60 * 1000    // 延迟为1分钟
    var maxTs: Long = Long.MinValue // 观察到的最大时间戳  // 初始值为Long类型的最小值

    override def getCurrentWatermark: Watermark = {
        new Watermark(maxTs - bound)
    }

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
        maxTs = maxTs.max(element.timestamp)
        element.timestamp
    }
}
