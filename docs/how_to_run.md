## 启动之前

编译的`dir`全部统一为`cmake-build-debug`

sql语句编译模块如果重新修改，需要额外编译，其地址为`./src/observer/sql/parser`

```
flex lex_sql.l
bison -d -b yacc_sql yacc_sql.y
```

flex 使用 2.5.35 版本测试通过，bison使用**3.7**版本测试通过

## 启动命令

observer
可以直接用`observer.sh`启动
```
./cmake-build-debug/bin/observer -f ./etc/observer.ini
```

obclient
可以直接用`obclient.sh`启动
```
./cmake-build-debug/bin/obclient [ip] [port]
help
```

## SQL测试

写了一个小脚本`./run_test.sh`，用来执行与数据库交互的SQL测试。首先需要在`./test/test_case`里写想要测试的SQL语句，在`./test/test_ans`写预期的返回结果，注意两者的文件名要相同。然后在根目录运行脚本，即可得到结果。运行的输出值会输出在`./test/test_out`里。

具体可以参考已经写好的测试case


