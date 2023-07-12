# 声明 
秋招结束后会将仓库设置为private
# 整个实验的大致架构图
其中蓝色方块是学生补充完成的部分。

![img](/Pics/CMU15-445整体架构图.png)

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

# 优化lab2
刚写完时，leadboard排名比较靠后，但是经过长时间debug过后，当时再也不想去碰这个实验了。

现在回过头来进行优化。刚好有两次面试问到我关于项目优化的问题，所以就专门准备给lab2优化下。

优化方法也更科学了一些，使用perf + 火焰图的方式寻找性能瓶颈，然后再做针对性的优化措施。

参考博客： [perf对多线程Profile简单流程_perf ](https://blog.csdn.net/banfushen007/article/details/122913803)

~~~c
sudo perf record -g -F 99 <要检测的程序名>   // -p 后跟进程id
perf script | FlameGraph/stackcollapse-perf.pl |  FlameGraph/flamegraph.pl > output.svg  // 输出的svg文件就是火焰图
~~~
打开图片：
![img](/Pics/FlameFigure.png)
可以看到主要有两个操作占用CPU的运行时间：
- IO写操作
- 对锁的一些操作
其中IO的耗时比锁更多一些，因此主要对io进行优化？

主要优化思路是，**减少IO操作**， 具体做法是： 在对buffer pool 的页进行unpin时，又一个bool类型变量，表示这个page是否为脏，如果为脏，则buffer pool后续必须将页写入磁盘，这是非常慢的，如果不为脏，buffer pool不会刷盘，减少了IO操作。

因此，我们需要识别那些操作是无论如何都要将page设置为脏，而哪些操作根本不需要设置为脏，尽量减少IO操作。比如，SplitInsert方法中，如果仅仅split桶而没有对目录页进行增长，那么目录页可以不被设置为脏。
*
除了IO优化，也可以进行一些锁的优化，这主要是对**锁粒度的细化**，比较简单而且效果也不是很明显。

优化效果： grading_hash_table_scale_test 的执行时间从原来的33s，减少为28s左右，优化了大约 **21%** 左右

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