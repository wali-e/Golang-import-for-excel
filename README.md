# excel 导入

1.基本思路是将大excel 拆分多个小excel

2.再启动携程同时读取多个小excel并导入

---

## 使用

创建目录并整理打包

> $  mkdir output
>
> $  go mod tidy
>
> $  go build import.go

使用参数说明

> Usage of import:
> -chr string
> 数据库编码类型 (default "utf8")
> -db string
> 数据库名 (default "test")
> -exl string
> 操作的Excel文件路径 (default "./import.xlsx")
> -fls value
> 写入表字段 (default "name,project,amount,date,info")
> -h string
> 数据库连接host (default "127.0.0.1")
> -pa string
> 数据库连接密码 (default "123456")
> -pr string
> 数据库连接密码 (default "3306")
> -size int
> 拆分长度 (default 1000)
> -tb string
> 导入的目标表名 (default "test")
> -u string
> 数据库连接用户 (default "root")
