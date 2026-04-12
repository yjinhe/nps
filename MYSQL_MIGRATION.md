# NPS MySQL 存储改造操作文档

## 概述

NPS 原版使用 JSON 文件存储数据（clients.json / tasks.json / hosts.json），每次增删改操作都会全量序列化写入文件，数据量大时性能较差。本次改造新增 MySQL 存储后端，支持增量写入，大幅提升性能。

### 性能对比

| 操作 | JSON 文件存储 | MySQL 存储 |
|------|-------------|-----------|
| 新增客户端 | 全量序列化所有客户端写入文件 | 单条 INSERT |
| 删除隧道 | 全量序列化所有隧道写入文件 | 单条 DELETE |
| 更新配置 | 全量序列化写入文件 | 单条 UPDATE |
| 流量统计 | 每次操作都写入 | 每30秒异步批量同步 |
| 查询 | 内存 sync.Map 遍历 | 内存 sync.Map 遍历（相同） |

---

## 一、从旧版本升级迁移

### 前提条件

- MySQL 5.7+ 或 MySQL 8.0+
- 已编译新版本 NPS（包含 MySQL 存储支持）
- 旧版本 NPS 正在运行的数据目录（包含 conf/ 目录）

### 步骤 1：创建 MySQL 数据库

```bash
mysql -u root -p -e "CREATE DATABASE nps DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_general_ci;"
```

如需创建专用账号：

```bash
mysql -u root -p -e "CREATE USER 'nps'@'%' IDENTIFIED BY 'your_password'; GRANT ALL PRIVILEGES ON nps.* TO 'nps'@'%'; FLUSH PRIVILEGES;"
```

### 步骤 2：执行数据迁移

迁移是**非破坏性**操作，不会修改原有 JSON 文件，可随时回退。

```bash
./nps migrate \
  -mysql_dsn=nps:your_password@tcp(127.0.0.1:3306)/nps?charset=utf8mb4&parseTime=true&loc=Local \
  -conf_path=/你的旧版nps安装路径
```

参数说明：

| 参数 | 说明 | 示例 |
|------|------|------|
| `-mysql_dsn` | MySQL 连接字符串（必填） | `nps:pwd@tcp(127.0.0.1:3306)/nps?charset=utf8mb4&parseTime=true&loc=Local` |
| `-conf_path` | NPS 安装目录（包含 conf/ 子目录） | `/etc/nps` |

迁移输出示例：

```
Migrating data from JSON files to MySQL...
Config path: /etc/nps
MySQL DSN: nps:****@tcp(127.0.0.1:3306)/nps?charset=utf8mb4&parseTime=true&loc=Local
Migration completed successfully!

Next steps:
1. Verify data in MySQL
2. Add mysql_dsn to nps.conf:
   mysql_dsn=nps:your_password@tcp(127.0.0.1:3306)/nps?charset=utf8mb4&parseTime=true&loc=Local
3. Restart NPS service
```

### 步骤 3：验证迁移数据

```bash
mysql -u nps -p nps -e "
  SELECT 'clients' AS table_name, COUNT(*) AS count FROM nps_client
  UNION ALL
  SELECT 'tunnels', COUNT(*) FROM nps_tunnel
  UNION ALL
  SELECT 'hosts', COUNT(*) FROM nps_host;
"
```

对比 JSON 文件中的记录数：

```bash
# 旧版数据文件路径
wc -l /你的旧版nps安装路径/conf/clients.json
wc -l /你的旧版nps安装路径/conf/tasks.json
wc -l /你的旧版nps安装路径/conf/hosts.json
```

### 步骤 4：修改配置启用 MySQL

编辑 `conf/nps.conf`，添加 MySQL 连接字符串：

```ini
# MySQL storage (leave empty to use JSON file storage)
mysql_dsn=nps:your_password@tcp(127.0.0.1:3306)/nps?charset=utf8mb4&parseTime=true&loc=Local
```

> 如果 `mysql_dsn` 留空或注释掉，NPS 将继续使用 JSON 文件存储。

### 步骤 5：重启 NPS 服务

```bash
# 停止旧服务
killall nps

# 启动新版本
./nps -conf_path=/你的nps安装路径
```

启动日志中会看到：

```
[I] mysql storage enabled, connecting to mysql...
[I] mysql storage initialized successfully
```

### 步骤 6：确认运行正常

- 访问 Web 管理界面，检查客户端和隧道列表
- 检查 NPC 客户端是否正常连接
- 检查 MySQL 数据是否实时更新

---

## 二、回退到 JSON 文件存储

如果需要回退：

1. 停止 NPS 服务
2. 注释或删除 `nps.conf` 中的 `mysql_dsn` 配置
3. 重启 NPS 服务

> 注意：回退后使用的是 JSON 文件中的数据，MySQL 中的后续变更不会自动同步回 JSON。

---

## 三、MySQL 表结构

### nps_client（客户端表）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 客户端ID（主键） |
| verify_key | VARCHAR(255) | 验证密钥 |
| addr | VARCHAR(255) | 客户端地址 |
| remark | VARCHAR(255) | 备注 |
| status | TINYINT | 状态（1=启用） |
| is_connect | TINYINT | 是否在线 |
| rate_limit | INT | 速率限制（KB/s） |
| export_flow | BIGINT | 出口流量 |
| inlet_flow | BIGINT | 入口流量 |
| flow_limit | BIGINT | 流量限制 |
| max_conn | INT | 最大连接数 |
| now_conn | INT | 当前连接数 |
| web_username | VARCHAR(255) | Web登录用户名 |
| web_password | VARCHAR(255) | Web登录密码 |
| config_conn_allow | TINYINT | 允许配置连接 |
| max_tunnel_num | INT | 最大隧道数 |
| version | VARCHAR(64) | 客户端版本 |
| black_ip_list | TEXT | IP黑名单（逗号分隔） |
| create_time | VARCHAR(64) | 创建时间 |
| last_online_time | VARCHAR(64) | 最后在线时间 |
| ip_white | TINYINT | 是否启用IP白名单 |
| ip_white_pass | VARCHAR(255) | 白名单密码 |
| ip_white_list | TEXT | IP白名单（逗号分隔） |
| cnf_u | VARCHAR(255) | SOCKS5认证用户名 |
| cnf_p | VARCHAR(255) | SOCKS5认证密码 |
| cnf_compress | TINYINT | 是否压缩 |
| cnf_crypt | TINYINT | 是否加密 |
| no_display | TINYINT | 是否隐藏 |

### nps_tunnel（隧道表）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 隧道ID（主键） |
| port | INT | 服务端监听端口 |
| server_ip | VARCHAR(64) | 监听IP |
| mode | VARCHAR(32) | 代理模式（tcp/socks5/udp/httpProxy等） |
| status | TINYINT | 状态 |
| run_status | TINYINT | 运行状态 |
| client_id | INT | 关联客户端ID |
| ports | VARCHAR(255) | 端口范围 |
| export_flow | BIGINT | 出口流量 |
| inlet_flow | BIGINT | 入口流量 |
| flow_limit | BIGINT | 流量限制 |
| password | VARCHAR(255) | 密码（secret/p2p模式） |
| remark | VARCHAR(255) | 备注 |
| target_addr | VARCHAR(1024) | 目标地址 |
| local_path | VARCHAR(512) | 本地路径 |
| strip_pre | VARCHAR(255) | URL前缀剥离 |
| proto_version | VARCHAR(32) | PROXY PROTOCOL版本 |
| target | TEXT | 目标配置（JSON） |
| multi_account | TEXT | 多账号配置（JSON） |
| health_check_timeout | INT | 健康检查超时 |
| health_max_fail | INT | 最大失败次数 |
| health_check_interval | INT | 健康检查间隔 |
| http_health_url | VARCHAR(512) | HTTP健康检查URL |
| health_check_type | VARCHAR(32) | 健康检查类型 |
| health_check_target | VARCHAR(512) | 健康检查目标 |

### nps_host（域名代理表）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 主机ID（主键） |
| host | VARCHAR(255) | 域名 |
| header_change | TEXT | 请求头修改 |
| host_change | VARCHAR(255) | Host头修改 |
| location | VARCHAR(255) | URL路径路由 |
| remark | VARCHAR(255) | 备注 |
| scheme | VARCHAR(16) | 协议（http/https/all） |
| cert_file_path | TEXT | 证书文件路径 |
| key_file_path | TEXT | 密钥文件路径 |
| is_close | TINYINT | 是否关闭 |
| auto_https | TINYINT | 自动HTTPS |
| export_flow | BIGINT | 出口流量 |
| inlet_flow | BIGINT | 入口流量 |
| flow_limit | BIGINT | 流量限制 |
| client_id | INT | 关联客户端ID |
| target | TEXT | 目标配置（JSON） |
| local_proxy | TINYINT | 本地代理 |

### nps_global（全局配置表）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT | 固定为1 |
| black_ip_list | TEXT | 全局IP黑名单 |
| server_url | VARCHAR(512) | 服务端URL |

---

## 四、架构说明

### 存储架构

```
启动时：MySQL → 加载到内存 sync.Map → 运行时查询走内存
写入时：内存 sync.Map 更新 + MySQL 增量写入（同步）
流量数据：内存累计 + 每30秒批量同步到 MySQL（异步）
```

### 设计原则

1. **兼容性**：不配置 `mysql_dsn` 时完全使用原有 JSON 文件存储，行为不变
2. **性能**：查询仍走内存 sync.Map，与原版一致；写入改为增量，大幅提升
3. **可靠性**：写入操作同步写入 MySQL，确保数据不丢失
4. **可回退**：迁移不修改原 JSON 文件，可随时回退

### 流量同步机制

流量数据（export_flow / inlet_flow）变化频繁，如果每次变化都写 MySQL 会产生大量 UPDATE 语句。因此采用**定时异步同步**策略：

- 每 30 秒将所有客户端、隧道、域名的流量数据批量 UPDATE 到 MySQL
- 服务停止时建议等待一个同步周期，或手动触发同步

---

## 五、常见问题

### Q: 迁移后原有 JSON 文件会被删除吗？

不会。迁移只是读取 JSON 文件内容写入 MySQL，不会修改或删除任何 JSON 文件。

### Q: 迁移过程中 NPS 服务需要停机吗？

建议在停机状态下迁移，避免迁移过程中数据变化导致不一致。

### Q: MySQL 连接断开怎么办？

go-sql-driver/mysql 内置了自动重连机制。如果连接断开，下次查询时会自动重试。建议配置 `SetConnMaxLifetime` 为 1 小时。

### Q: 如何监控 MySQL 存储是否正常？

```bash
# 检查最近更新的客户端
mysql -u nps -p nps -e "SELECT id, remark, last_online_time, is_connect FROM nps_client ORDER BY last_online_time DESC LIMIT 10;"

# 检查隧道运行状态
mysql -u nps -p nps -e "SELECT id, mode, port, run_status, client_id FROM nps_tunnel WHERE status=1;"

# 检查流量统计
mysql -u nps -p nps -e "SELECT id, remark, export_flow, inlet_flow FROM nps_client ORDER BY export_flow DESC LIMIT 10;"
```

### Q: 0.26.10 版本的数据能直接迁移吗？

可以。0.26.10 和当前版本的 JSON 数据结构兼容，迁移工具会自动处理字段映射。
