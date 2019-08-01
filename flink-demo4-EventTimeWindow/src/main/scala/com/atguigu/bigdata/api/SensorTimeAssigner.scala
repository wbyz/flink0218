package com.atguigu.bigdata.api

import com.atguigu.bigdata.bean.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author Witzel
  * @since 2019/7/31 19:57
  */
class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
    // 抽取时间戳
    override def extractTimestamp(element: SensorReading): Long = element.timestamp
}
