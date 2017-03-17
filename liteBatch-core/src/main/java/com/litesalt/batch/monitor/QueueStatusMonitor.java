package com.litesalt.batch.monitor;

import java.util.Date;
import java.util.Observable;
import java.util.Observer;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.litesalt.batch.handler.RowBatchHandler;

/**
 * @author Paul-xiong
 * @date 2017年3月2日
 * @description 队列状态监控器
 */
public class QueueStatusMonitor<T> implements Observer {

	private final Logger log = Logger.getLogger(QueueStatusMonitor.class);

	private final long time = 600 * 1000; // 定时时间

	private Date lastBatchTime = new Date();

	private Timer timer = new Timer();;

	public QueueStatusMonitor() {
		this(null);
	}

	public QueueStatusMonitor(final RowBatchHandler<T> handler) {
		super();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					if (handler != null) {
						Date now = new Date();
						if (now.getTime() - lastBatchTime.getTime() > time) {
							log.info("队列健康状态监视器开始工作");
							handler.rowBatch(handler.takeAll());
						}
					}
				} catch (Exception e) {
					log.error("队列健康状态监视器工作异常", e);
				}
			}
		}, 0, time);
	}

	@Override
	public void update(Observable o, Object arg) {
		lastBatchTime = new Date();
	}

}
