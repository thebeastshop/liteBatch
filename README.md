
##liteBatch
liteBatch是一个轻量级，高性能，高通用的批插框架。

* 能够像普通insert一样在循环中插入PO
* 异步执行，无阻塞
* 维护一个缓冲的队列，到达配置的阀值之后批次提交
* 可以和各种ORM结合使用
* 兼容各种数据库
* 适应所有的VO，自动生成脚本
* 性能高效，测试机上测试大概4w+/秒
* 自动处理各种基础类型的数据
* 支持自定义的映射和过滤字段

更新记录
1.0.1
优化了队列控制的实现机制
增加了redis队列的实现
选择使用redis队列时候，如果重启或者宕机，数据不会丢失，100%数据完整性
增加了队列健康度监控机制，减少了线程开销

##快速使用
也可以参考test工程的testUnit

```java
		RowBatchListener<Person> rowBatchListener = RowBatchListenerBuilder.buildRedisRowBatchListener(jdbcTemplate, 5000, Person.class, "localhost",6379);
		try {
			Random random = new Random();
			Person person = null;
			for (int i = 0; i < 66000; i++) {
				person = new Person();
				person.setAge(random.nextInt(100));
				person.setAddress("XX马路1号");
				person.setCompany("天天 向上科技有限公司");
				person.setName("张三");
				person.setCreateTime(new Date());
				rowBatchListener.insertOneWithBatch(person);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
```

##注意
在mysql数据库下，需要注意以下几点

* 驱动包一定得5.1.13版本以上（含）
* 在jdbc连接url里得加上rewriteBatchedStatements=true参数