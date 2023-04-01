# 声明 
秋招结束后会将仓库设置为private
# gradescope通过截图
Project1 :
![img](/Pics/CMU15-445P1_PASS_PIC.png)

![img](/Pics/CMU15-445P1_LeaderBord.png)

Project2
![img](/Pics/CMU15-445P2_PASS_PIC.png)

![img](/Pics/CMU15-445P2_LeaderBord.png)


Project3
![img](/Pics/CMU15-445P3_PASS_PIC.png)

![img](/Pics/CMU15-445P3_LeaderBord.png)


Project4
![img](/Pics/CMU15-445P4_PASS_PIC.png)

project4无leaderboard
# 测试代码运行
~~~shell
cd build
make clean
cmake ..
make test_name
./test/test_name
~~~
# 自我评价
写得很烂，尤其是lab2和lab4，前者是耗时长，可能算法逻辑并不是最优；后者则是代码结构很烂， 对条件变量以及锁的处理看着就非常业余。lab的思路几乎没有一个能自己想出来，都是参考别人博客磕磕绊绊写的。

这是本人尝试写完的第一个lab，感觉很有难度。但收获颇多，尤其是C++的使用以及关系型数据库的原理。
# 后续计划
- 试着在C++语法层面对代码进行优化，尤其是在C++11的使用方面
- 调整代码结构，使其更精简
- 调整算法逻辑，使其更快速

# 关于lab2
个人感觉lab2是最难的一个实验。

得理解动态哈希表的概念，推荐资料如下：
- [geeksforgeeks](https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/)
- [youtube视频](https://www.youtube.com/watch?v=cmneVtDrhAA&ab_channel=ChrisMarriott-ComputerScience)
- [我自己画的示意图](https://github.com/SixPlusAndTimes/bustub-private/blob/main/dynamic_hashing--insert%E5%92%8Cbucketsplit%E6%BC%94%E7%A4%BA.pdf)

# 优化lab1

shell脚本,执行测试程序10次，计算其平均执行时间，。提一嘴，我完全不懂shell编程，是让ChatGPT生成的，还真不错 :)

初始：

| lru_replacer_test| buffer_pool_manager_instance_test|parallel_buffer_pool_manager_test|
|---|---|---|
| 0.227| 2.999|3.116 |

主要关于unordered_map的使用方面的优化,以及引用传值(for循环内)，迭代器使用(使用前置++)
| lru_replacer_test| buffer_pool_manager_instance_test|parallel_buffer_pool_manager_test|
|---|---|---|
| 0.229| 2.929|3.026|
              
感觉没啥优化的样子

# 优化lab3
这次就一次性测试取全部的测试代码吧, 运行15次取平均值
~~~shell
./testTime.sh ./TestAll
....
平均运行时间为：4.580
~~~
优化措施：
- 把中间变量消除，特别是调用值传递时的变量， 可能会额外多调用一次拷贝构造函数
- 能用引用就用引用
- 为vector添加元素前先reserve腾出空间
- push_back(std::move(左值)) 或者 push_back(临时变量) 或者 emplaceXX
  - 注意Value这个类没有移动构造函数，所以有些优化是做不了的，即使使用了std::move
- 同样还是对unordered_map的关于find和count的使用。
- 迭代器的前置与后置自增


优化后的平均执行时间为： 4.418秒。

感觉也不是很明显阿 ：(



# 优化lab4
可算把各个lock方法的while等待条件给想清楚了，顺眼了不少

把一些重复代码封装了起来

本来想把三个lock方法的woundwait算法封装在一个方法里的，但是越改越来丑了:)，改了这里，这个测试代码没过，改了那里，那个测试代码没过。。。