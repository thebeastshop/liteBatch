/*
 * Copyright (C), 上海布鲁爱电子商务有限公司
 */
package com.thebeastshop.batch.callback;

import java.util.List;

/**
 * @author Paul-xiong
 * @date 2017年7月14日
 * @description 异常处理回调
 */
public interface ExceptionCallback<T> {
	void handle(List<T> batchList);
}
