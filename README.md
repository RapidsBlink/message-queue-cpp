# message-queue

1-million message-queue in c++ (使用intrinsics编写向量化代码) --- Created By Blink`团队 ([Pisces](https://github.com/WANG-lp) and [yche](https://github.com/CheYulin))。


## 本地构建代码

```zsh
mkdir -p build && cd build
cmake .. -Dtest=ON -DenableLOG=ON -DenableWall=ON -DCMAKE_INSTALL_PREFIX=/alidata1/race2018/work   
make -j && make install
```

## 本仓库的文件

文件夹/文件 | 说明
--- | ---
[include](include) | 动态链接库export出来时候的头文件
[src](src) | 源代码文件
[cmake](cmake), [CMakeLists.txt](CMakeLists.txt) | 项目构建文件

## 最高跑分统计数据

```
[2018-07-11 23:49:55,890] Send: 478408 ms Num: 2000000009
[2018-07-11 23:49:55,890] Put message per second: 4180532.12
[2018-07-11 23:51:55,199] Index Check: 107483 ms Num:1000010
[2018-07-11 23:55:46,916] phase3 start
[2018-07-11 23:55:46,916] Check: 231732 ms Num: 200000000
[2018-07-11 23:55:46,916] Tps: 2691946.44
```