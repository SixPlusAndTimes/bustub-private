这个project的bug大部分是并发条件， 由于之前没有正经的并发编程经验，所以debug起来依然很痛苦。但是好在测试代码的并发度不高(测试代码中会使用sleep函数故意使线程睡眠几秒)， 可以直接使用cout\LOG_DEBUG调试。

我觉得project4的难度比project3大些， 但是projet3的代码量大，所以反而project3的完成时间要多些。

课上的理论听得好好的，但是一到编程时间就蒙了。比如隔离级别的实现，本project不要求实现serailizable隔离级别，但尽管如此，我刚开始还是不知道要怎样具体地实现它们.

看了一些博客以及看了测试用例后渐渐摸清了, 推荐这个博客 https://blog.csdn.net/twentyonepilots/article/details/120868216, 写得比较清楚

总结以下 :

- isolation = READ_UNCOMMITTED : 不需要读锁， 只需要写锁
- isolation = READ_COMMITED : 读锁，写锁都需要，但是读锁 在读完就释放， 写时上写锁，在commit时才释放写锁 (`不需要2pl,看测试代码就懂了`)
- isolation = REPEATABLE_READ ： 使用二阶段锁，在growing阶段只能获取锁，在shring阶段只能释放锁，而且所有的锁都在commit时才释放(2pl 就成了 strong 2pl)
- isolation =SERIALIZABLE ： 本lab不要求，理论上需要 上一级别的所有要求 + `index locks`

> // 注意： 普通2PL只能解决不可重复读的问题，不能解决脏读问题； 

读写锁立即释放还是在commit时统一全部释放，不由lock_manager决定，而是由各executor决定
- seq_scan : 只涉及读锁, 
  - 在Next方法开头判断隔离级别是否为 READ_UNCOMMITTED, 如果是就不用加读锁, 其他两个隔离条件都需要加读锁. 
  - 在Next方法返回前, 判断隔离级别是否为REPEATABLE_READ, 如果不是则立刻释放读锁,如果是则不释放,由上层调用者在事务commit时一起释放
- delete: 涉及写锁和锁升级, 首先应该意识到delete从它的子执行器(也就是scan executor)中获得tuple, 视隔离级别不同, 或得的tuple有 没有上锁或者已经上读锁的 两种可能
  - 获得子执行器传出的tuple后, 判断当前事务的隔离级别, 如果是REPEATABLE_READ, 那么对该tuple执行锁升级
  - 否则对该tuple上写锁
  - 不需要解锁, 会在事务commit后自动释放全部的锁
- update: 同上
- insert : 似乎没有要求 ?
  
> 虽然官网提示我们要更新索引的writeset,但是测试代码中没有对索引进行检查.

> 花费时间最长的是关于Reapeatable Read相关的测试

测试代码会这样测试你是否正确实现了Reapeatable Read隔离级别：
- 开启两个thread，记为thread1，thread2
- thread1 执行两次Selcet操作，两次查询的数据的RID相同
- thread2 执行一次Update操作，修改的数据与thread1的数据的RID相同
- 测试代码使用sleep操作保证调度顺序一定为：
  - thread1第一次select -> thread2 update相同数据 -> thread1第二次select
- 如果正确实现了Repeatable隔离级别，那么thread1的第二次select结果与第一次的结果相同
- 这里的关键就是使得thread2在执行update时被挂起。
- UpdateExecutor有一个seqscan子Executor，子执行器已经对数据上了读锁，且由于隔离级别为Reapeatable Read，因此子执行其上的读锁不会归还直到事务结束。这时主执行器，在对记录进行update时就要使用LockUpdate方法了
- 因此LockUpdate方法一定要确保在这种调度顺序下，thread2会在条件变量上等待，等待改RID的读锁和写锁数量为0。

>具体如何调试？
说来惭愧，本人不是很会用GDB，且这个实验的并发读不高，所以一直使用控制台在调试 ：）。主要观察thread2 的update方法是否在thread1 commit事务前结束执行。如果是的话，就打印更加详细的LOG，把等待队列的has_writer、readcount、是否陷入等待、是否执行唤醒操作、wound-wait算法是否正确abort了新事务等等，都打印出来，一个一个排查。

易错： 当一个事务被wakeup时，并不代表它获取了锁，有可能它作为一个老事务被新事物abort了，因此一定要检查这种情况！