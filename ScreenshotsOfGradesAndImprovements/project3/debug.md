# InserExecutor不会改变结果集

所以在execution_engine.h 添加判断tuple是否真的被分配的逻辑

~~~cpp
    Tuple tuple;  // 这里已经调用了一次默认构造函数，但是分配的空间在栈上，因此不需要delete之类的操作
    RID rid;
    while (executor->Next(&tuple, &rid)) {
    // 注意 ： 这里要判断tuple是否真的被分配
    // insert是不会将tuple赋值的， 所以应该判断元祖是否分配再将它加入结果集中。 insert操作不会改变结果集！
    if (result_set != nullptr && tuple.IsAllocated()) {
        result_set->push_back(tuple);
    }
    }
~~~

` && tuple.IsAllocated()` 是要增加的

# 内存管理
栈上分配的内存不用管理，因为它们的生存周期是受限的，会被自动释放。而堆上分配的对象必须手动释放或者使用智能指针管理。

说起来很容易，踩坑同样容易。

比如在seq_scan_executor.cpp的Next()方法中， 要将元组拷贝到tuple指针指向的内存中，而不是new 一个对象将指针拷贝给tuple指针。可以看execution_engine.h中，在栈上分配了一个Tuple， 在调用Next方法时，它传入的是栈上这个Tuple的指针，意思是让我们将Tuble构建出来然后复制到栈上相应的位置。
~~~cpp
    // 下面的new 操作会在堆内存中申请空间，但是没有对应的析构函数
    // tuple = new Tuple(values, output_shema_); // 这一行有两个错误，1是没有析构Tuple ，2是没有在给定的地址创建，请见execution.h中的执行代码。 
    // new(tuple)Tuple(values, output_shema_); // 这一行也错，错在在堆上分配对象
    *tuple = Tuple(values, output_shema_); 
~~~

尽量用智能指针管理智能指针，比如 insert_executor.cpp 的初始化方法中，传入的child_executor是智能指针，所以我们也用智能指针管理它，否则会有内存泄漏