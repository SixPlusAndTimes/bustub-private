这个project的bug大部分是并发条件， 由于之前没有正经的并发编程经验，所以debug起来依然很痛苦。

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