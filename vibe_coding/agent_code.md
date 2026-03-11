你是一名 **高级 Python 架构工程师**。  
请设计并实现一个 **生产级 Python 爬虫系统**，用于抓取 **搜狗微信搜索（Sogou Weixin Search）文章列表数据**。

系统必须具备 **良好的反爬能力、可扩展架构、Session Pool、请求调度机制**。

请生成 **完整可运行的 Python 项目代码**，并遵循以下工程规范。

---

# 一、项目目标

实现一个爬虫系统：

输入：

```text
关键词
```

输出：

抓取搜狗微信搜索文章列表，字段包括：

```text
title           # 文章标题
account_name    # 公众号名称
publish_time    # 发布时间
article_desc    # 文章简介
image_url       # 搜索结果图片
sogou_url       # 搜狗跳转URL
```

示例目标页面：

```
https://weixin.sogou.com/weixin?type=2&query=AI
```

---

# 二、系统架构

请实现如下模块化架构：

```
Crawler System
│
├── Request Scheduler
│
├── Session Pool
│
├── Proxy Pool (optional)
│
├── Anti-Spider Detector
│
├── Worker
│
├── Parser
│
└── Storage
```

请求流程：

```
Worker
   │
   ▼
SessionPool.borrow()
   │
   ▼
HTTP Request
   │
   ▼
AntiSpiderDetector
   │
   ▼
Parser
   │
   ▼
SessionPool.feedback()
   │
   ▼
Storage
```

---

# 三、项目目录结构

生成完整工程目录：

```
crawler_project/

main.py

session_pool/
    session_client.py
    session_pool.py
    session_factory.py

proxy_pool/
    proxy_pool.py

scheduler/
    request_scheduler.py

detector/
    antispider_detector.py

crawler/
    worker.py
    sogou_spider.py

parser/
    sogou_parser.py

utils/
    headers_profiles.py
    rate_limiter.py
```

代码必须按模块划分。

---

# 四、Session Pool 设计

实现 **生产级 Session Pool**。

Session Pool 负责：

```
Session生命周期管理
Session健康度
Session调度
Session冷却
Session销毁与创建
```

---

# 五、SessionClient对象

不要直接使用 `requests.Session`。

必须封装为：

```
SessionClient
```

属性包括：

```
session: requests.Session
session_id
proxy
user_agent
headers_profile
cookies
create_time
last_used_time
request_count
success_count
fail_count
health_score
cooldown_until
max_requests
request_interval
```

---

# 六、Session生命周期

Session 生命周期：

```
create
  ↓
available
  ↓
borrowed
  ↓
success → available
  ↓
fail → cooldown
  ↓
too_many_failures → destroy
```

Session Pool 必须实现：

```
borrow_session()
return_session(session, result)
```

---

# 七、Session创建策略

Session 创建由：

```
SessionFactory
```

负责。

创建流程：

```
create requests.Session
↓
设置 headers
↓
绑定 User-Agent
↓
绑定 Proxy (optional)
↓
访问首页预热
↓
返回 SessionClient
```

首页预热：

```
GET https://weixin.sogou.com/
```

---

# 八、Session + Proxy 绑定

Session 与 Proxy 必须 **一对一绑定**：

```
session1 → proxy1
session2 → proxy2
session3 → proxy3
```

禁止：

```
一个session频繁更换proxy
```

原因：

反爬会检测：

```
IP
cookie
UA
行为轨迹
```

---

# 九、Session调度策略

Session Pool 调度必须实现：

### 1 LRU

优先使用：

```
last_used_time 最早
```

避免连续使用同一个 session。

---

### 2 Health Score

Session 维护：

```
health_score
```

计算：

```
health = success_rate - fail_penalty
```

调度优先使用：

```
health_score 高
```

---

### 3 Weighted Random

Session 使用概率：

```
健康 session → 权重高
失败多 session → 权重低
```

---

# 十、Session请求节奏

每个 Session 必须有自己的请求间隔：

```
session.request_interval
```

推荐：

```
3~8 秒随机
```

Scheduler 在调度时检查：

```
now - last_used_time >= request_interval
```

---

# 十一、Session最大请求次数

Session 必须限制：

```
max_requests_per_session
```

推荐：

```
80 ~ 120
```

超过后：

```
销毁 session
创建新 session
```

原因：

```
cookie老化
行为轨迹累积
反爬识别
```

---

# 十二、Session冷却机制

检测到反爬时：

不要立即销毁 session。

例如：

```
验证码
302 antispider
403
429
```

处理：

```
session.cooldown_until = now + cooldown_time
```

推荐：

```
cooldown_time = 30~60 seconds
```

session 放入：

```
cooldown_queue
```

后台线程定期恢复。

---

# 十三、失败分类

必须实现失败分类：

```
NETWORK_ERROR
SERVER_ERROR
PARSE_ERROR
ANTISPIDER
RATE_LIMIT
SUCCESS
```

处理策略：

|错误类型|处理|
|---|---|
|NETWORK_ERROR|retry|
|SERVER_ERROR|retry|
|PARSE_ERROR|ignore|
|ANTISPIDER|session cooldown|
|RATE_LIMIT|session cooldown|
|SUCCESS|session health +|

---

# 十四、AntiSpiderDetector

实现模块：

```
AntiSpiderDetector
```

检测：

```
验证码页面
antispider
302跳转
403
429
HTML异常
```

返回：

```
SUCCESS
FAIL
BLOCKED
```

---

# 十五、行为轨迹模拟

Session 创建时：

访问顺序：

```
homepage
↓
search_page
```

例如：

```
https://weixin.sogou.com/
https://weixin.sogou.com/weixin?query=test
```

建立真实 cookie 轨迹。

---

# 十六、全局请求速率限制

实现：

```
GlobalRateLimiter
```

例如：

```
max 2 requests per second
```

防止整体请求过快。

---

# 十七、Parser

解析：

```
搜狗微信搜索HTML
```

提取：

```
title
account
publish_time
desc
image
sogou_url
```

然后：

```
GET sogou_url
获取302
```

---

# 十八、推荐默认参数

```
session_pool_size = 5~10
max_requests_per_session = 80~120
fail_threshold = 5
cooldown_time = 30~60 seconds
request_interval = 3~8 seconds
global_rate_limit = 2 req/sec
```

---

# 十九、代码要求

代码必须：

- Python 3.10+
    
- 使用 `requests`
    
- 使用 `BeautifulSoup`
    
- 完整类型注解
    
- 面向对象设计
    
- 清晰模块划分
    
- 可运行
    
- 带必要注释
    
- 带示例 main.py
    

---

# 二十、main.py 示例功能

main.py 应实现：

```
输入关键词
设置抓取页数
启动 crawler
```

示例运行：

```
python main.py --keyword AI --pages 5
```

---

# 二十一、最终目标

实现一个：

```
稳定
模块化
抗反爬
可扩展
生产级
```

的 **Python 爬虫架构系统**。