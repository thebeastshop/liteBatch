/**
 * <p>Title: liteBatch</p>
 * <p>Description: 一个轻量级，高性能的快速批插工具</p>
 * <p>Copyright: Copyright (c) 2016</p>
 * @author Bryan.Zhang
 * @email 47483522@qq.com
 * @Date 2016-11-10
 * @version 1.0
 */
package com.litesalt.batch.entity;

/**
 * PO包裹类
 */
public class WrapItem<T> {
	
	private T t;
	
	private boolean shutdownSignature;

	public WrapItem() {
	}
	
	public WrapItem(T t) {
		this.t = t;
	}

	public T getT() {
		return t;
	}

	public void setT(T t) {
		this.t = t;
	}

	public boolean isShutdownSignature() {
		return shutdownSignature;
	}

	public void setShutdownSignature(boolean shutdownSignature) {
		this.shutdownSignature = shutdownSignature;
	}
}
