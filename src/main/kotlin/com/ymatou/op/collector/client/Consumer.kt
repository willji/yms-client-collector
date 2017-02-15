package com.ymatou.op.collector.client

import com.typesafe.config.Config
import kafka.consumer.ConsumerConfig
import kafka.message.MessageAndMetadata
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Created by xuemingli on 16/8/23.
 */
class Consumer(val config: Config, kafkaProps: Properties) {
    val logger = LoggerFactory.getLogger(javaClass)!!
    val consumer = kafka.consumer.Consumer.createJavaConsumerConnector(ConsumerConfig(kafkaProps))!!
    val latch = CountDownLatch(1)
    val processors = ConcurrentHashMap<String, Processor>()
    var executor: ExecutorService? = null

    private fun parse(record: MessageAndMetadata<String, String>): Map<String, String> {
        logger.debug("[r] message: ${record.message().replace("\u0001", "\n")}")
        val seq = record.message().split("\u0001")
                .map { it.split('=', limit = 2).map { it.trim() } }
                .map { it.component1() to it.component2() }
        val ret = mutableMapOf(*seq.toList().toTypedArray())
        val action_param = ret["action_param"]?.split(';')
                ?.map { it.split(':') }
                ?.map { it.component1() to it.component2() }
                ?.toMap() ?: mapOf("status" to "-1", "resp_time" to "-1")
        ret["status"] = action_param["status"]!!
        ret["resp_time"] = action_param["resp_time"]!!
        return ret
    }

    fun register(name: String, processor: Processor) {
        processors[name] = processor
        processor.start()
    }

    fun dispatch(item: Map<String, String>) {
        for (processor in processors.values) {
            processor.put(item.filter { processor.fields().contains(it.key) })
        }
    }

    fun start() {
        logger.info("consumer start")
        val topicMap = config.getStringList("consumer.topic").map { it to Runtime.getRuntime().availableProcessors() }.toMap()
        val consumerMap = consumer.createMessageStreams(topicMap, { String(it) }, { String(it) })
        executor = Executors.newFixedThreadPool(topicMap.size * Runtime.getRuntime().availableProcessors())
        consumerMap.values.forEach {
            it.forEach {
                executor?.execute {
                    for(stream in it) {
                        try {
                            dispatch(parse(stream))
                        } catch (e: Exception) {
                            logger.error("dispatch or parse error", e)
                        }

                    }
                }
            }
        }
        latch.await()
    }

}