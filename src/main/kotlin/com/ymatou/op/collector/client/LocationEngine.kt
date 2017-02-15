package com.ymatou.op.collector.client

import org.apache.commons.csv.CSVFormat
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.io.InputStreamReader
import java.util.*

/**
 * Created by xuemingli on 16/8/23.
 */

data class Location(val location: String, var agent: String)

data class Row(val country: String, val province: String, val city: String, val agent: String, val begin: Long, val end: Long)

class LocationEngine {
    val logger = LoggerFactory.getLogger(javaClass)

    val store = TreeMap<Long, Row>()

    fun load(input: InputStream) {
        val reader = InputStreamReader(input)
        val records = CSVFormat.EXCEL.parse(reader)
        records.forEach {
            store[it[4].toLong()] = Row(it[0], it[1], it[2],
                    it[3], it[4].toLong(), it[5].toLong())
        }
    }

    private fun ipToLong(ip: String): Long {
        val address = ip.split(".")
        var ret: Long = 0
        for (i in 0..3) {
            ret += address[i].toLong() shl 24 - 8 * i
        }
        return ret
    }

    private fun _lookup(ip: String): Row {
        val defaultRow = Row("unknown", "unknown", "unknown", "unknown", 0L, 0L)
        try {
            val key = ipToLong(ip)
            val entry = store.floorEntry(key) ?: return defaultRow
            return entry.value
        } catch (e: Exception) {
            logger.error("lookup $ip error, ${e.message}")
        }
        return defaultRow
    }

    fun lookup(ip: String): Location {
        val row = _lookup(ip)
        var location = row.province

        if (location == "*") {
            location = "unknown"
        }

        var agent = row.agent
        if (agent == "*") {
            agent = "unknown"
        }
        return Location(location, agent)
    }

}
