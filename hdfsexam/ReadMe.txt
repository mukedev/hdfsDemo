老师，您好：
    项目分为三个mapreduce阶段
    1.merge阶段
        功能：合并同ip同用户名的登录信息，将登出时间合并在同一行。
    2.join阶段
        功能：将1阶段输出与visit.log数据做join操作。
    3.top阶段
        功能：统计量与排序


    准备：
        1.创建hdfs://node01:8020/exam/login/ 目录，并上传login.log文件到该目录下。
        2.将visit.log文件上传至运行服务器/home/hadoop/file/。

    启动：
        hadoop jar hdfsexam.jar cn.bxg.exam.merge.HdfsExamApp
        hadoop jar hdfsexam.jar cn.bxg.exam.join.JoinExamApp
        hadoop jar hdfsexam.jar cn.bxg.exam.top.TopExamApp

    最后，希望老师给予宝贵意见及分享一些优秀的解决方案。谢谢


