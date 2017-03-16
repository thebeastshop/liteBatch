/*
 * Copyright (C), 上海布鲁爱电子商务有限公司
 */
package com.litesalt.batch.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Paul-xiong
 * @date 2017年3月16日
 * @description
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AliasField {
	public String value();
}
