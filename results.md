打包文件主要包含：hl-TPCDI（hanlin的文档）、tools（生成TPC-DI数据的工具）、sf_3（里面有数据，23个sql task脚本和1个python task脚本以及定义他们的dag.py，其他sf代码相似不放了）、create_image.py（airflow统计数据画图的），还有本pdf。

上届GitHub：https://github.com/risg99/tpc-di-benchmark，可参考报告和readme

各工具版本：MySQL 8.0.29(就是mariadb)、Docker Desktop 4.34.2

这里提一嘴，hanlin和这个组最大不同就是hanlin区分了静态表和非静态，前者是数据从头到尾不改变的，所以她用一个sql全打包生成了，所以我们的DAG dependency略微不同，具体详见hanlin文档，dependency详见dag.py



sf_9数据生成，替换hanlin文档sf24

![image-20241216184139787](C:\Users\戴尔\AppData\Roaming\Typora\typora-user-images\image-20241216184139787.png)



DAG graph

![image-20241216062345469](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\DAG_graph.png)

完成示意图

![image-20241216102139522](C:\Users\戴尔\AppData\Roaming\Typora\typora-user-images\image-20241216102139522.png)

sf_3  run duration & gantt & task duration graphs

![image-20241216223659045](C:\Users\戴尔\AppData\Roaming\Typora\typora-user-images\image-20241216223659045.png)

![image-20241216223240172](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\sf3_gantt.png)

跑了两遍

![image-20241216223431044](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\sf3_run_time_by_task.png)

sf_6

![image-20241216223809453](C:\Users\戴尔\AppData\Roaming\Typora\typora-user-images\image-20241216223809453.png)

![image-20241216101945689](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\sf6_run_time_by_task.png)



![image-20241216102107425](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\sf6_gantt.png)



sf_9

![image-20241216224001045](C:\Users\戴尔\AppData\Roaming\Typora\typora-user-images\image-20241216224001045.png)



![image-20241216222806407](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\sf9_run_time_by_task.png)



![image-20241216232815879](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\sf9_gantt.png)



sf_12

![image-20241216224737756](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\sf12_run_time_by_task.png)

![image-20241216125509322](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\sf12_run_time_by_task.png)



![image-20241216125402389](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\sf12_gantt.png)



total_run_time

![image-20241216225714873](C:\Users\戴尔\Desktop\TPC-DI\Results_graphs\total_run_time.png)

run_time_by_task

![image-20241216232216079](C:\Users\戴尔\AppData\Roaming\Typora\typora-user-images\image-20241216232216079.png)

run_time_by_task_exclude_top_5

![image-20241216232458967](C:\Users\戴尔\AppData\Roaming\Typora\typora-user-images\image-20241216232458967.png)

我贴心的为你画了表格，里面的数据详见create_image.py

| TASK ID                          | SF 3 | SF 6 | SF 9 | SF 12 |
| -------------------------------- | ---- | ---- | ---- | ----- |
| create_staging_schema            | 1    | 1    | 1    | 1     |
| load_txt_csv_to_staging          | 60   | 197  | 187  | 304   |
| load_finwire_to_staging          | 7    | 17   | 18   | 25    |
| parse_finwire                    | 7    | 22   | 16   | 37    |
| convert_load_customermgmt        | 146  | 811  | 1030 | 1762  |
| create_master_schema             | 0    | 0    | 1    | 1     |
| load_master_static               | 2    | 2    | 2    | 2     |
| transform_load_dimcompany        | 0    | 0    | 0    | 1     |
| load_dimessages_dimcompany       | 3    | 0    | 0    | 0     |
| transform_load_dimbroker         | 1    | 0    | 1    | 1     |
| transform_load_prospect          | 2    | 1    | 2    | 3     |
| transform_load_dimcustomer       | 1    | 1    | 1    | 2     |
| load_dimessages_dimcustomer      | 1    | 0    | 1    | 1     |
| update_prospect                  | 3    | 6    | 12   | 18    |
| transform_load_dimaccount        | 1    | 2    | 2    | 3     |
| transform_load_dimsecurity       | 7    | 13   | 28   | 46    |
| transform_load_dimtrade          | 68   | 372  | 1573 | 1043  |
| load_dimessages_dimtrade         | 5    | 4    | 8    | 16    |
| transform_load_financial         | 132  | 506  | 1422 | 2528  |
| transform_load_factcashbalance   | 400  | 883  | 1765 | 2267  |
| transform_load_factholdings      | 106  | 239  | 433  | 1005  |
| transform_load_factwatches       | 587  | 1573 | 2021 | 3120  |
| transform_load_factmarkethistory | 287  | 394  | 191  | 7     |
| load_dimessages_dimtrade         | 0    | 0    | 0    | 0     |

