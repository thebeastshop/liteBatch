package com.litesalt.batch.handler;

import java.io.File;

import com.litesalt.batch.entity.RedisRowBatchQueue;

/**
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description redis批插处理器
 */
public class RedisFileRowBatchHandler<T> extends FileRowBatchHandler<T> {

	public RedisFileRowBatchHandler(File file, long submitCapacity, Class<T> clazz, String host, int port, String auth) {
		super(file, submitCapacity, clazz);
		this.queue = new RedisRowBatchQueue<T>(clazz, host, port, auth);
	}
}
