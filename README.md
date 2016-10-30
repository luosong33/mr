一：运行本地mr，操作远程hadoop
1、插件
2、dll文件
3、windows版hadoop
4、新建包并修改源码
resources中
hadoop-eclipse-plugin-2.6.0.jar为插件
hadoop_dll2.6.0为windows中需要运行的dll文件
hadoop-2.6.1.zip为编译好的win版hadoop
/mr/src/main/java/org/apache/hadoop/io/nativeio/NativeIO.java为windows需要修改源码


二：简单javaAPI操作hadoop
/mr/src/main/resources/core-site.xml、hdfs-site.xml为java代码直接操作hadoop所需的配置文件，与远程hadoop配置一样