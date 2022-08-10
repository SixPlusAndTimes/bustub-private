# Build error
很可能是代码风格的问题，我为了这个问题提交了不下15次;
`make format` `make check-lint` 在本地通过后，很玄学的，gradescope竟然没通过。。。只能一点一点比对

还有 `make format` 竟然会对注释掉了的代码进行改动。。。 所以注释掉大片后，一定要再次make format一次

# 超时
这个错误是最烦人的，因为什么输出都没有。最坑人的是，代码风格也可能会造成超时错误

project1 我是大概写了以下 split 的逻辑后进行了第一次提交， 果不其然的是超时。。。

我把锁去掉了，但还是超时。没有办法，我把extendible_hash_table重置成“出场状态”，先测试前一个Task1， 但是buid error了，修改几次后，发现还是代码风格问题

。。。真的很玄学阿， 感觉在代码风格上浪费了太多时间了


另外, 数组越界也有可能会超时错误
~~~cpp
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int readable_size = sizeof(readable_);
  // assert(readable_size == (BUCKET_ARRAY_SIZE - 1) / 8 + 1);
  for (int array_index = 0; array_index < readable_size; array_index++) {
    char reading_bytes = readable_[array_index];
    for (int byte_idx = 0; byte_idx <= 7; byte_idx++) {
      if (static_cast<bool>(reading_bytes & (0B10000000 >> byte_idx))) {
        // 某一位已经被占用，判断是否重复,使用cmp比较key，使用 == 号比较value
        if ((cmp(array_[array_index * 8 + byte_idx].first, key) == 0) &&
            array_[array_index * 8 + byte_idx].second == value) {
          return false;
        }
        // 不重复则继续寻找下一个空位置
        continue;
      }
      // 找到空位，插入即可
      // LOG_DEBUG("Found free space, array_index = %d, byte_index = %d",array_index, byte_idx);
      array_[array_index * 8 + byte_idx] = std::pair<KeyType, ValueType>(key, value);
      readable_[array_index] = readable_[array_index] | (0B10000000 >> byte_idx);
      // 还要修改 occupied数组,将对应的位置1即可
      occupied_[array_index] = occupied_[array_index] | (0B10000000 >> byte_idx);
      return true;
    }
  }
  return false;
}
~~~
上面的代码, 外层for循环中的结束条件 不小心把等号加上去了, 然后就超时了...

这个BUG也改了很久,不下20次提交

# 一小部分以下部分地去完成和测试
本地的测试完成一个就提交一次， 否则最后一次性提交可能会很难再排查出问题。

两个本地测试其实不用加锁也可以通过，所以可以先把功能实现完成，去gradescope上检测以下(应该能拿30分)，然后再去实现线程安全

# GetSplitImageIndex()
将 bucktindex 的最高位至反即可

这个没想出来，参考别人的才了解的

# readable数组是不连续的
在没有 split之前， 某一个bucket中的readable数组确实是连续的，但是在分裂后将bucket的元素rehash后，bucket中的元素就是不连续的了

所以在`hash_table_bucket_page.cpp`中的 insert 逻辑不能查找空位以及判重同时进行

必须先完整扫描bucket中的元素判重， 然后再插入

这个BUG卡了我很久， 线上测试又看不出什么，最后还是自己仿照本地测试进行 splitinsert 后才发现错了

不得不说本地测试是真的有点鸡肋。。。
