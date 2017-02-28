package com.litesalt.batch.handler;

import org.springframework.jdbc.core.JdbcTemplate;

import com.litesalt.batch.entity.MemoryRowBatchQueue;

/**
 * @author Paul-xiong
 * @date 2017年2月27日
 * @description 内存批插处理器
 */
public class MemoryRowBatchHandler<T> extends RowBatchHandler<T> {

	public MemoryRowBatchHandler(JdbcTemplate jdbcTemplate, long submitCapacity, Class<T> clazz) {
		super(jdbcTemplate, submitCapacity, clazz);
		this.queue = new MemoryRowBatchQueue<T>();
	}
}
