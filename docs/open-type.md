# 三方数据源接入 idaas 类型系统文档
## 前言
## 类型描述格式
类型描述以 json 格式提供, 左侧 key 为数据库类型描述, 包括固定部分与参数部分

固定部分为类型描述中, 一个类型不可变的部分, 由开发者自行填写

参数部分一般为对这个类型的限制描述, 开发者需要在参数列表中找到对应的参数名, 并填写到恰当的位置

以 mysql varchar(100) 举例, 其固定部分为 varchar, 参数部分为 (100), 寻找对长度 100 的参数描述名称, 为 length, 即最终其描述 key 为: `varchar($length)

如果类型中存在部分可能出现的字段, 比如 unsigned, zerofill, 请以 [](多选一, 可选), {}(多选一, 必选) 描述

json 右侧为对这个类型的能力边界描述, 与数据库类型预计转换到的中间类型映射, 其结构如下:
{
"to": "中间类型1:中间类型2...", // 转换优先级, 靠前的优先级高
"bit": 1, // 整数时, 以位数描述
"unsigned": "unsigned", // 如果支持无符号, 无符号的描述字符串, 如不支持, 不需要填写此值
"to": "typeNumber" // 转换到的中间类型, 如果多个类型, 以 : 分隔, 表示转换优先级

}

比如以 mysql tinyint 为例子, 其完整描述结构如下:
{
"tinyint[($m)][unsigned][zerofill]": {"bit": 1, "unsigned": true, "to": "typeNumber"}
}

## 中间类型列表
中间类型分为基础类型(对绝大多数数据库均可生效的类型), 与扩展类型(仅部分数据库可用的类型)
### 基础类型
### typeNumer
数字类型, 以 Double 存储其值, 针对不同的数据库类型, 取不同的属性描述其能力
1. 针对整数, 以 bit(位数) 描述能力, 以 unsigned 为辅助描述
2. 针对单精度浮点数, 以 fbit(浮点位数) 描述能力
3. 针对双精度, 以 precision(长度) 与 scale(精度) 描述能力

### typeString
字符串类型, 以 String 存储其值, length(长度) 描述能力

### typeBinary
二进制类型, 以 byte[] 存储其值, length(长度) 描述能力

### typeDate
日期类型, 其描述参数有:
1. timeZone: 字符串, 支持 "+8/-8" 描述, 或者 "Shanghai", "Asia/Shanghai" 等字符串类型的描述
2. format: 字符串, 描述将日期序列化为字符串时, 其结构, 关键字有: "YYYY/YY, MM, DD"

### typeDateTime
时间类型, 其描述参数有:
1. timeZone: 字符串, 支持 "+8/-8" 描述, 或者 "Shanghai", "Asia/Shanghai" 等字符串类型的描述
2. format: 字符串, 描述将日期序列化为字符串时, 其结构, 关键字有: "YYYY/YY, MM, DD"


### 扩展属性
1. typeYear: 年, 支持以 ($m) 描述, 表示宽度, 类型为 "YYYY/YY"
2. typeInterval: 时间间隔


## 示例
以 Mysql 数据类型示例, 描述如下:
```json
{
    "tinyint[($m)][unsigned][zerofill]": {"bit": 1, "unsigned": "unsigned", "to": "typeNumber"},
    "smallint[($m)][unsigned][zerofill]": {"bit": 4, "unsigned": "unsigned", "to": "typeNumber"},
    "mediumint[($m)][unsigned][zerofill]": {"bit": 8, "unsigned": "unsigned", "to": "typeNumber"},
    "int[($m)][unsigned][zerofill]": {"bit": 32, "unsigned": "unsigned", "to": "typeNumber"},
    "bigint[($m)][unsigned][zerofill]": {"bit": 256, "unsigned": "unsigned", "to": "typeNumber"},
    "float[($float)][unsigned][zerofill]": {"fbit": 16, "unsigned": "unsigned", "to": "typeNumber"},
    "double[($float)][unsigned][zerofill]": {"float": 256, "unsigned": "unsigned", "to": "typeNumber"},
    "decimal($precision, $scale)[unsigned][zerofill]": {"precision":[1, 65], "scale": [0, 30], "unsigned": "unsigned", "to": "typeNumber"},
    "date": {range": ["1000-01-01", "9999-12-31"], "to": "typeDate"},
    "time": {range": ["-838:59:59","838:59:59"], "to": "typeInterval:typeNumber"},
    "year[($m)]": {range": [1901, 2155], "to": "typeYear:typeNumber"},
    "datetime": {range": ["1000-01-01 00:00:00", "9999-12-31 23:59:59"], "to": "typeDateTime"},
    "timestamp": {to": "typeDateTime"},
    "char[($width)]": {"byte": 255, "to": "typeString"},
    "varchar[($width)]": {"byte": "64k", "fixed": false, "to": "typeString"},
    "tinyblob": {"byte": 255, "to": "typeBinary"},
    "tinytext": {"byte": 255, "to": "typeString"},
    "blob": {"byte": "64k", "to": "typeBinary"},
    "text": {"byte": "64k", "to": "typeString"},
    "mediumblob": {"byte": "16m", "to": "typeBinary"},
    "mediumtext": {"byte": "16m", "to": "typeString"},
    "longblob": {"byte": "4g", "to": "typeBinary"},
    "longtext": {"byte": "4g", "to": "typeString"},
    "bit($width)": {"byte": 8, "to": "typeBinary"},
    "binary($width)": {"byte": 255, "to": "typeBinary"},
    "varbinary($width)": {"byte": 255, fixed": false, "to": "typeBinary"}
}
