package com.thebeastshop.batch.monitor;

import java.util.Date;
import java.util.Observable;
import java.util.Observer;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.thebeastshop.batch.handler.RowBatchHandler;

/**
 * @author Paul-xiong
 * @date 2017年3月2日
 * @description 队列状态监控器
 */
public class QueueStatusMonitor<T> implements Observer {

	private final Logger log = Logger.getLogger(QueueStatusMonitor.class);

	private Long time = 600 * 1000L; // 定时时间

	private Date lastBatchTime = new Date();

	private Timer timer = new Timer();;

	public QueueStatusMonitor() {
		this(null);
	}

	public QueueStatusMonitor(final RowBatchHandler<T> handler) {
		this(handler, 600 * 1000L);
	}

	public QueueStatusMonitor(final RowBatchHandler<T> handler, Long monitorTime) {
		super();
		if (monitorTime != null) {
			this.time = monitorTime;
		}
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					if (handler != null) {
						Date now = new Date();
						if (now.getTime() - lastBatchTime.getTime() > time) {
							log.info("队列健康状态监视器开始工作");
							handler.flush();
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
