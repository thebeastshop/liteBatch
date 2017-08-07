package com.thebeastshop.batch.context;

import com.thebeastshop.batch.enums.TargetType;

/**
 * @author Paul-xiong
 * @date 2017年3月17日
 * @description 队列上下文
 */
public class QueueContext<T> {
	/**
	 * 
	 */
	private TargetType type;
	/**
	 * 
	 */
	private Class<T> clazz;

	public QueueContext() {
		this(TargetType.DB);
	}

	public QueueContext(TargetType type) {
		this(type, null);
	}

	public QueueContext(TargetType type, Class<T> clazz) {
		super();
		this.type = type;
		this.clazz = clazz;
	}

	public TargetType getType() {
		return type;
	}

	public void setType(TargetType type) {
		this.type = type;
	}

	public Class<T> getClazz() {
		return clazz;
	}

	public void setClazz(Class<T> clazz) {
		this.clazz = clazz;
	}

}
