# Spark用户行为日志分析

### 目录结构
/sql: 数据库初始化的表

### 用户session分析
每一次执行用户访问session分析模块，要抽取100个session。

session随机抽取：按照每个的每个小时的session数量，占当天session总数的比例，乘以每天要抽取的session数量，计算出每个小时要抽取的session数量；
然后每天每小时的session中，随机抽取出之前计算出来的数量的session。

