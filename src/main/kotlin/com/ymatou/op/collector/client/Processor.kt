package com.ymatou.op.collector.client

import com.typesafe.config.Config

/**
 * Created by xuemingli on 16/8/23.
 */
interface Processor {
    val config: Config

    fun put(item: Map<String, String>)
    fun start()
    fun fields(): Set<String>
    fun shutdown()
}