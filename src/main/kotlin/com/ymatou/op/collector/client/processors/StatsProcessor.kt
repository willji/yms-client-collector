package com.ymatou.op.collector.client.processors

import com.typesafe.config.Config
import com.ymatou.op.collector.client.Location
import com.ymatou.op.collector.client.LocationEngine
import com.ymatou.op.collector.client.Processor
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.influxdb.InfluxDB
import org.influxdb.dto.Point
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.URL
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong


/**
 * Created by xuemingli on 16/8/23.
 */

fun Double.toFixed(scale: Int) = BigDecimal.valueOf(this).setScale(scale, RoundingMode.HALF_UP).toDouble()


class StatsProcessor(override val config: Config, val influx: InfluxDB): Processor {
    val logger = LoggerFactory.getLogger(javaClass)!!
    val queue = LinkedBlockingQueue<Map<String, String>>()
    val executor = Executors.newSingleThreadExecutor()!!
    val latch = CountDownLatch(1)
    val statistics = mutableMapOf<String, MutableMap<Location, DescriptiveStatistics>>()
    val counter = mutableMapOf<String, MutableMap<String, AtomicLong>>()
    val locationEngine = LocationEngine()
    val db = config.getString("influx.db")!!
    val prefix = config.getString("influx.prefix")!!

    init {
        locationEngine.load(javaClass.getResourceAsStream("/location.csv"))
    }


    override fun put(item: Map<String, String>) {
        logger.debug("[stats][r]")
        queue.offer(item, config.getLong("processor.stats.timeout"), TimeUnit.MILLISECONDS)
    }

    override fun start() {
        var start = System.currentTimeMillis()
        executor.execute {
            while (latch.count > 0) {
                try {
                    val current = System.currentTimeMillis()
                    if (current - start >= TimeUnit.SECONDS.toMillis(config.getLong("processor.stats.interval"))) {
                        start = current
                        statsSendAndClear(current)
                        countSendAndClear(current)
                    }
                    val item = queue.poll(100, TimeUnit.MILLISECONDS)
                    item ?: continue
                    val rs = item["resp_time"]!!.toDouble()
                    if (rs < 0 || rs > 60 * 2 * 1000) {
                        logger.warn("response time must be less than ${60 * 2 * 1000} and must be positive number, but it is $rs")
                        continue
                    }
                    val target = normalizeTarget((item["target"] ?: "").split('?')[0])
                    val location = locationEngine.lookup(item["ip"]!!)
                    if (item["mobile_operator"] != null) {
                        location.agent = item["mobile_operator"]!!
                    }

                    location.agent = normalizeAgent(location.agent)

                    stats(target, location, rs)

                    val status = try {
                        (item["status"] ?: "0").toInt()
                    } catch (e: Exception) {
                        logger.error("get status code error ${e.message}")
                        -1
                    }

                    count(target, status)

                } catch (e: Exception) {
                    logger.error("stats process error", e)
                }

            }
        }
    }

    override fun fields(): Set<String> {
        return setOf("target", "ip", "resp_time", "mobile_operator", "status")
    }

    override fun shutdown() {
        executor.shutdown()
        latch.countDown()
    }

    private fun statsSendAndClear(timestamp: Long) {
        statistics.forEach {
            val target = it.key
            it.value.forEach {
                val location = it.key.location
                val agent = it.key.agent
                val point = Point.measurement("$prefix.stats.resp_time")
                        .time(timestamp, TimeUnit.MILLISECONDS)
                        .addField("mean", it.value.mean.toFixed(3))
                        .addField("std_dev", it.value.standardDeviation.toFixed(3))
                        .addField("upper_90", it.value.getPercentile(90.0).toFixed(3))
                        .addField("median", it.value.getPercentile(50.0).toFixed(3))
                        .addField("geometric_mean", it.value.geometricMean.toFixed(3))
                        .tag("app", target)
                        .tag("location", location)
                        .tag("agent", agent)
                        .build()
                influx.write(db, "default", point)
            }
        }
        statistics.clear()
    }

    private fun normalizeTarget(src: String): String {
        val url = URL(src)
        if (!url.host.endsWith("ymatou.com")
                || url.host.contains("img.ymatou")
                || url.host.contains("s1.ymatou")
                || url.host.contains("static")
                || url.host.contains("evt.ymatou")) {
            return "${url.protocol}://${url.host}"
        }
        return src
    }

    private fun normalizeAgent(src: String): String {
        if (src.contains("中国移动") || src.contains("CMCC", true) || src.contains("中國移動") || src.contains("ChinaMobile", true)) {
            return "中国移动"
        }
        if (src.contains("中国联通") || src.contains("CUCC", true) || src.contains("中國聯通") || src.contains("ChinaUnicom", true)) {
            return "中国联通"
        }
        if (src.contains("中国电信") || src.contains("CTCC", true) || src.contains("中國電信") || src.contains("ChinaTelecom", true)) {
            return "中国电信"
        }
        return "其他"
    }

    private fun countSendAndClear(timestamp: Long) {
        counter.forEach {
            val target = it.key
            val builder = Point.measurement("$prefix.count.code").time(timestamp, TimeUnit.MILLISECONDS)
            it.value.forEach {
                builder.addField(it.key, it.value.get())
            }
            val point = builder.tag("app", target).build()
            influx.write(db, "default", point)
        }
        counter.clear()
    }

    private fun stats(target: String, location: Location, rs: Double) {
        if (! statistics.containsKey(target)) {
            statistics[target] = mutableMapOf()
        }
        val stat = statistics[target]!!
        if (!stat.containsKey(location)) {
            stat[location] = DescriptiveStatistics()
        }
        stat[location]!!.addValue(rs)
    }

    private fun count(target: String, status: Int) {
        if (! counter.containsKey(target)) {
            counter[target] = mutableMapOf("total" to AtomicLong())
        }
        val catalog = when (true) {
            status >= 100 && status < 200 -> "1xx"
            status >= 200 && status < 300 -> "2xx"
            status >= 300 && status < 400 -> "3xx"
            status >= 400 && status < 500 -> "4xx"
            status >= 500 && status < 600 -> "5xx"
            else -> "unknown"
        }

        val c = counter[target]!!
        if (c[status.toString()] == null) {
            c[status.toString()] = AtomicLong()
        }
        c[status.toString()]?.incrementAndGet()
        if (c[catalog] == null){
            c[catalog] = AtomicLong()
        }
        c[catalog]?.incrementAndGet()
        c["total"]?.incrementAndGet()
    }
}