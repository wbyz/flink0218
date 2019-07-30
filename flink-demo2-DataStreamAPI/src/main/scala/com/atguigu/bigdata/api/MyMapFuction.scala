package com.atguigu.bigdata.api

import org.apache.flink.api.common.functions.MapFunction

/**
  * @author Witzel
  * @since 2019/7/30 18:26
  */
class MyMapFunction extends MapFunction[SensorReading , String] {
    override def map(r: SensorReading): String = r.id
}

