
##liteBatch
liteBatch是一个轻量级，高性能，高通用的批插工具。

* 能够像普通insert一样在循环中插入PO
* 维护一个缓冲的队列，到达配置的阀值之后批次提交
* 可以和各种ORM结合使用
* 适应所有的VO，自动生成脚本
* 性能高效，测试机上测试大概4w+/秒
* 自动处理各种基础类型的数据
* 支持自定义的映射和过滤字段

##快速使用
也可以参考test工程的testUnit

```java
    RowBatchListener<Person> rowBatchListener = new RowBatchListener<>(jdbcTemplate, 5000, Person.class);//申明一个监听器
	try{
		Random random = new Random();
		Person person = null;
		for(int i=0;i<500500;i++){
			person = new Person();
			person.setAge(random.nextInt(100));
			person.setAddress("XX马路1号");
			person.setCompany("XX科技有限公司");
			person.setName("张三");
			person.setCreateTime(new Date());
			rowBatchListener.insertOneWithBatch(person);//可以在循环中进行插入
		}
	}catch(Exception e){
		e.printStackTrace();
	}finally{
		rowBatchListener.closeListener();//一定要关闭监听，建议在finally里关闭
	}
```

##注意
在mysql数据库下，需要注意以下几点

* 驱动包一定得5.1.13版本以上（含）
* 在jdbc连接url里得加上rewriteBatchedStatements=true参数