package com.atguigu.bigdata.api

import org.apache.flink.api.common.functions.FilterFunction

/**
  * @author Witzel
  * @since 2019/7/31 9:14
  */
class MyFilterFunction extends FilterFunction[SensorReading]{
    override def filter(value: SensorReading): Boolean = {
        value.temperature > 25
    }
}
