package com.ymatou.op.collector.client

import com.typesafe.config.ConfigFactory
import com.ymatou.op.collector.client.processors.StatsProcessor
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * Created by xuemingli on 16/8/23.
 */
object Application {
    @JvmStatic fun main(args: Array<String>) {
        val profile = System.getProperty("profile") ?: "prod"
        val config = ConfigFactory.load("application-$profile.conf")
        val kafkaProps = Properties()
        kafkaProps.load(javaClass.getResourceAsStream(config.getString("consumer.config")))
        val consumer = Consumer(config, kafkaProps)
        val influx = InfluxDBFactory.connect(config.getString("influx.url"), "root", "root")
        //influx.setLogLevel(InfluxDB.LogLevel.BASIC)
        influx.enableBatch(config.getInt("influx.batch.size"),
                config.getInt("influx.batch.interval"),
                TimeUnit.SECONDS)
        val stats = StatsProcessor(config, influx)
        consumer.register("stats", stats)
        consumer.start()
    }
}