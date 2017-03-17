package com.litesalt.batch.context;

import com.litesalt.batch.enums.TargetType;

/**
 * @author Paul-xiong
 * @date 2017年3月17日
 * @description 队列上下文
 */
public class QueueContext {
	/**
	 * 
	 */
	private TargetType type;
	/**
	 * redis key扩展
	 */
	private String redisKeyExt;

	public QueueContext() {
		this(TargetType.DB);
	}

	public QueueContext(TargetType type) {
		this(type, null);
	}

	public QueueContext(TargetType type, String redisKeyExt) {
		super();
		this.type = type;
		this.redisKeyExt = redisKeyExt;
	}

	public TargetType getType() {
		return type;
	}

	public void setType(TargetType type) {
		this.type = type;
	}

	public String getRedisKeyExt() {
		return redisKeyExt;
	}

	public void setRedisKeyExt(String redisKeyExt) {
		this.redisKeyExt = redisKeyExt;
	}

}
