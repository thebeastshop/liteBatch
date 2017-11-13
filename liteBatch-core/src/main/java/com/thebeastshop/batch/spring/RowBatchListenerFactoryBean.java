package com.thebeastshop.batch.spring;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.jdbc.core.JdbcTemplate;

import com.thebeastshop.batch.DBRowBatchListenerBuilder;
import com.thebeastshop.batch.listener.RowBatchListener;

/**
 * @author gongjun[jun.gong@thebeastshop.com]
 * @since 2017-04-27 14:18
 */
public class RowBatchListenerFactoryBean<T> implements FactoryBean<RowBatchListener<T>> {

	private JdbcTemplate jdbcTemplate;

	private Class<T> beanClass;

	private Integer submitCapacity;

	private Boolean syn;

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public Class<T> getBeanClass() {
		return beanClass;
	}

	public void setBeanClass(Class<T> beanClass) {
		this.beanClass = beanClass;
	}

	public Integer getSubmitCapacity() {
		return submitCapacity;
	}

	public void setSubmitCapacity(Integer submitCapacity) {
		this.submitCapacity = submitCapacity;
	}

	public Boolean getSyn() {
		return syn;
	}

	public void setSyn(Boolean syn) {
		this.syn = syn;
	}

	@Override
	public RowBatchListener<T> getObject() throws Exception {
		if (syn == null) {
			return DBRowBatchListenerBuilder.buildMemoryRowBatchListener(jdbcTemplate, submitCapacity, beanClass);
		}
		return DBRowBatchListenerBuilder.buildMemoryRowBatchListener(jdbcTemplate, submitCapacity, beanClass, syn);
	}

	@Override
	public Class<?> getObjectType() {
		return RowBatchListener.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}
}
