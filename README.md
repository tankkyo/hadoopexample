# 环境搭建
1. 下载Jar包
2. 配置环境
    - yarn-env.sh
    - yarn-site.xml
    - mapred-site.xml
    - capacity-scheduler.xml (optional)
3. 启动Yarn
在Hadoop安装目录下面有个sbin目录
```shell
cd $HADOOP_HOME # Hadoop的安装目录
sbin/start-yarn.sh
sbin/mr-jobhistory-daemon.sh start historyserver
```
# Shell使用
1. 阅读官方文档
    - jar
    - application
    - logs
2. 使用Shell命令
```shell
cd $HADOOP_HOME
bin/yarn jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar pi
bin/yarn application -list
bin/yarn application -kill <appid>
bin/yarn logs -applicationId <appid>
```
# Web UI使用
1. 查看Yarn的Web 
    - 总资源数
    - 所有的节点状况
    - 所有的applications
    - 调度器
2. 查看History Web UI
    - Job的基本信息
    - 详细信息
        - overview
        - counters
        - configuration
        - map tasks
        - reduce tasks

# 程序设计
1. 在IDE中新建工程
    - 编写Mapper
    - 编写Reducer
    - 编写Driver
2. 编写WordCount程序
3. 本地运行 & 调试
4. 编译为二进制包
5. 提交到集群执行

# 课后作业
1. 实现倒排索引
    - 读取data/docs目录下的所有文件
    - 输出每个单词所在的文件以及个数
    - 输出如下结果:
    > is      t0.txt:2        t1.txt:1        t2.txt:1

（提示）可以通过context.getInputSplit获取输入的文件，进而获取输入文件名

