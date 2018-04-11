# 获取工具
`git clone git@git.intra.im:qixuan.zhu/job_helper.git`
# 脚本批量解析程序
## 命令行执行
`cd ~/job_helper/bin`  

`python job_analysis.py run "e:/workspace/bigdata-dwh/code/dwd/user_profile/job" "sql"`
## 参数说明
filepath:'e:/workspace/bigdata-dwh/code/dwd/user_profile/job'  

脚本路径,程序将处理路径 filepath 下的所有脚本  

suffix:'sql'  

脚本的后缀,程序只处理后缀为 suffix 的脚本  

解析行为:  

将按照 Job注解规范 自动解析脚本中的  @source,@target  并将解析结果写入脚本末尾  

例如:  
`--/**#2018-04-11 14:48:42.980433
--   #source|dwd.dwd_user
--   #source|dwd.dwd_rack_order
--   #target|dwd.user_profile_rack_history_consume_data
--**/
`
## 注意:
*脚本依赖自动解析的结果 主要供脚本开发者参考*  

*脚本被提交到git的dev分支后,以--@ 开头的注解将被保存到 元数据管理系统 中*  

*所以将 --   # 批量替换为 --   @ 可以批量进行上述进程*  

*目前调度系统没有使用该依赖,所以并不会自动加入调度系统*
# 脚本批量处理程序
## 命令行执行
`cd ~/job_helper/bin`  

`python job_handle.py run "e:/workspace/bigdata-dwh/code/dwd/user_profile/job" "sql" "table"`  
## 参数说明
filepath:'e:/workspace/bigdata-dwh/code/dwd/user_profile/job'  

脚本路径,程序将处理路径 filepath 下的所有脚本  

suffix:'sql'  

脚本的后缀,程序只处理后缀为 suffix 的脚本  

conf:'table'  

选择配置文件
## 处理时使用的配置文件
1.conf:'func'  

配置文件位置:./func.conf  

配置文件说明:  

column1|column2  

将本次处理涉及的脚本代码中的 column1 替换为 column2  

例如:  

'${bdp.system.bizdate}'|from_unixtime(unix_timestamp(),'yyyy-MM-dd')  

将脚本中所有的 '${bdp.system.bizdate}' 替换为 from_unixtime(unix_timestamp(),'yyyy-MM-dd')  

这样做的逻辑是 阿里云特有函数 '${bdp.system.bizdate}' 返回当前日期   
hive函数 from_unixtime(unix_timestamp(),'yyyy-MM-dd') 经测试 与阿里云函数有等效的返回值  

2.conf:'table'  

配置文件位置:./table.conf  

配置文件说明:  

column1|column2  

将本次处理涉及的脚本代码中的 column1 替换为 column2  

例如:  

owo_user_profile.|dwd.  

将脚本中所有的 owo_user_profile. 替换为 dwd.  

这样做的逻辑是 原owo_user_profile库的表都迁移到了dwd库  

## 注意:
一般应该优先替换长文本,因为可能A替换项是B替换项的子句,比如:  

A:  

unix_timestamp(datetrunc(getdate(),'DD'))|unix_timestamp(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),'yyyy-MM-dd')  

B:  

getdate()|from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')  

这里如果先替换B,  A不会被替换  

所以在配置文件中A需要比B的行数小
