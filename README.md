LittleKV: A Lightweight LevelDB Clone

LittleKV 是一个用 C++17 实现的轻量级、高可扩展性的键值存储引擎，核心结构基于 LSM Tree，支持 WAL、MemTable、SSTable、异步刷盘、Compaction 机制，并具备工业级的恢复与归档能力，适用于数据库系统构建的学习与求职展示。

---

⭐ 项目亮点（可写入简历）

- 完整覆盖 LevelDB 核心设计：包括 Active MemTable + Immutable MemTable、WAL、SSTable、多级索引、Manifest、Compaction 等关键模块，支持高性能读写与持久化。
- 写前日志（WAL）支持 CRC32 校验：支持多 WAL 文件 +WAL Recovery 机制 + CRC32 校验，保证数据写入可靠性与一致性，归档与恢复逻辑完整。支持 Snappy 压缩接口，实现对 value 的压缩存储，节省存储空间。
- 多 WAL + 多 immutable MemTable 实现无阻塞刷盘机制：每次 active MemTable 填满时立即切换至新的 WAL 与 MemTable，旧内存表转为 immutable 并异步刷盘，避免写入阻塞，保障高并发场景下的写入连续性与系统吞吐。
- 模块化后台线程机制：实现独立的刷盘、合并线程与 Compact 管理器，分别负责 MemTable Flush 与 LSM 树 Compaction，支持异步触发与任务调度。
- 压实分数驱动的 Compaction 任务调度策略：自研压实分数算法，L0 层按文件数量评估，L1+ 层按总文件大小与层容量比计算，动态选出压实优先级最高的层，精确控制后台合并时机，减少写放大与读延迟
- SSTable Manifest 原子更新机制：使用临时文件 + rename 技术保证元数据一致性。
- 支持查询路径：活跃 MemTable → 多个 immutable MemTable → L0 → L1-L3（Compaction 后），具备查询优化路径与 BloomFilter 加速。
- 支持删除标记（TOMBSTONE）与 Compaction 清理，提升存储效率。

---

🗂️ 项目结构图

    .
    ├── app/                         # 应用入口与配置
    │   ├── main.cpp                # 启动入口
    │   ├── config.json             # 系统配置（如路径、大小限制等）
    │   └── CMakeLists.txt
    ├── build/                      # 构建输出目录（CMake）
    ├── build_and_test.sh           # 一键构建与测试脚本
    ├── CMakeLists.txt              # 顶层构建配置
    ├── logs/
    │   └── alpha_log/
    │       ├── logs/
    │       │   └── log.txt         # 操作日志
    │       └── wals/
    │           ├── wal_current.log    # 当前活跃WAL
    │           └── manifest.txt       # WAL文件索引元数据
    ├── src/
    │   ├── cli/                    # 命令行交互模块
    │   │   ├── headers/kv_cli.h
    │   │   └── impls/kv_cli.cpp
    │   ├── kv_alpha/               # 存储引擎核心模块
    │   │   ├── headers/
    │   │   │   ├── BloomFilter.h
    │   │   │   ├── CompactionManager.h  # 后台线程独立管理合并任务队列，避免阻塞主线程。
    │   │   │   ├── ConfigLoader.h
    │   │   │   ├── FdCache.h
    │   │   │   ├── json.hpp
    │   │   │   ├── Little_kv.h
    │   │   │   ├── MemTable.h
    │   │   │   ├── MemTableManager.h    # 多个 immutable MemTable，支持并发 flush，降低写阻塞
    │   │   │   ├── SnappyCompressor.h
    │   │   │   ├── SSTableManager.h
    │   │   │   ├── WAL.h
    │   │   │   └── WalManifest.h # 多个 WAL 文件交替使用，写操作完全无阻塞。日志归档 + Manifest 持久化
    │   │   └── impls/
    │   │       ├── BloomFilter.cpp
    │   │       ├── CompactionManager.cpp
    │   │       ├── Little_kv.cpp
    │   │       ├── MemTable.cpp
    │   │       ├── MemTableManager.cpp
    │   │       ├── SnappyCompressor.cpp
    │   │       ├── SSTableManager.cpp
    │   │       ├── WAL.cpp
    │   │       └── WalManifest.cpp
    │   └── CMakeLists.txt
    ├── ssts/
    │   └── alpha_sst/
    │       └── manifest.txt        # SST 文件索引元数据
    ├── test/                       # 测试代码（未展示具体内容）
    └── Testing                     # 可能用于 GoogleTest 构建或测试中间结果

    ┌───────────────────────────────┐
    │          kv_alpha             │  ← 主存储引擎模块
    │ ┌───────────────────────────┐ │
    │ │        Little_kv          │ │  ← 存储核心控制器
    │ │  - put / get / del        │ │
    │ │  - detect_and_schedule()  │◀┐←─ 压实调度器 (根据压实分数)
    │ │  - recovery / restart     │ │
    │ └───────────────────────────┘ │
    │        ▲             ▲        │
    │        │             │        │
    │        │             │        │
    │ ┌────────────┐   ┌─────────────────────────────┐
    │ │ WALManager │   │  MemTableManager            │
    │ │ (多 WAL)   │   │  - active / immutable 分离  │
    │ │ - 支持归档 │   │  - 多表无阻塞 flush        │
    │ └────────────┘   └─────────────────────────────┘
    │
    │ ┌─────────────────────────────┐
    │ │ SSTableManager              │ ← 管理各层SST文件与元数据
    │ │ - 读写SST                  │
    │ │ - 分层管理 Level-0~2       │
    │ └─────────────────────────────┘
    │
    │ ┌─────────────────────────────┐
    │ │ CompactionManager           │ ← 后台压实模块
    │ │ - 任务队列/线程调度        │
    │ │ - 调用 compact_executor     │
    │ └─────────────────────────────┘
    │
    │ ┌─────────────────────────────┐
    │ │ WalManifest (原子元数据)   │ ← 管理 WAL 当前状态
    │ └─────────────────────────────┘
    │
    │ ┌─────────────────────────────┐
    │ │ ConfigLoader / BloomFilter  │ ← 工具模块
    │ └─────────────────────────────┘
    └───────────────────────────────┘
    
    ┌────────────┐     ┌────────────┐     ┌───────────────┐
    │   cli/     │     │  app/      │     │  test/        │
    │ kv_cli.cpp │◀──▶│ main.cpp   │◀──▶│ 单元测试      │
    └────────────┘     └────────────┘     └───────────────┘

---

🔧 模块职责概览

  模块               	功能描述                                    
  Little_kv        	对外暴露的主类，封装 MemTable、WAL、SST、Manifest 管理等核心逻辑
  WAL              	写前日志支持，支持多文件归档与 CRC32 校验恢复机制            
  MemTableManager  	管理多个 active/immutable MemTable 与对应 WAL，提供刷盘与回收接口
  CompactionManager	管理 LSM Compaction 任务队列，异步合并 SST 文件，清理 TOMBSTONE
  Manifest         	管理元信息，支持原子更新、版本切换与多层索引                  
  SSTable          	管理 L0-L3 层级的持久化文件，支持范围检索与 BloomFilter 加速

---

✅ 已实现功能列表

-

---

📄 依赖安装及使用方式

    # 更新软件源
    sudo apt-get update
    
    # 安装必需依赖
    sudo apt-get install build-essential cmake
    
    # 安装 zlib
    sudo apt-get install zlib1g-dev
    
    # 安装 nlohmann/json
    sudo apt-get install nlohmann-json3-dev
    
    # 安装 GoogleTest（可选）
    sudo apt-get install libgtest-dev
    cd /usr/src/gtest
    sudo cmake .
    sudo make
    sudo mv lib/libgtest*.a /usr/lib/
    
    # 构建并运行（项目根目录）
    ./build_and_test.sh 

---

🧠 面试手册（常见问题 + 答案）



Q1: 为什么需要多个 immutable MemTable？

A：当 active MemTable 被刷盘时，可能还有新写入，需立刻生成新的 WAL 和 active MemTable，而旧的 immutable MemTable 仍在后台刷盘中。多个 immutable MemTable 保证写入不会被阻塞，提高系统吞吐量。

Q2: 无阻塞刷盘机制是如何实现的？

A：在传统单 MemTable 系统中，写入线程在刷盘期间会阻塞等待，降低吞吐。而 LittleKV 中设计了多 WAL + 多 immutable MemTable 机制：当 active MemTable 达到阈值时，立即将其转为 immutable，并新建一个 WAL 文件与 active MemTable，允许后续写入不中断，immutable与原wal构成entry加入刷盘队列（MemTableManager）。刷盘与 WAL 归档由后台线程处理，真正实现写入与刷盘解耦，避免堵塞，提高系统响应能力。

Q3: Compaction 机制是如何触发的？

A：CompactionManager 内部有一个任务队列与后台线程，支持条件变量触发与异步合并，定期将 L0 文件压缩合并到更底层，清理重复和删除数据，保持查询效率。

Q4: 你是如何判断某一层是否需要触发压实（Compaction）的？

A：我实现了类似 LevelDB 的“压实分数”机制：

- L0 层：按文件数量与设定阈值计算分数，超过阈值立即合并（写放大最严重，优先处理）。
- L1+ 层：按当前层的总文件大小与设定最大容量比例计算分数，选出压实分数最大的层。
- 调度规则：
  - 压实分数 < 1：不合并
  - 压实分数 ≥ 1：调度后台线程执行对应层压实
  - 压实分数相同时，越接近内存（层级越小）优先级越高

压实调度由 detect_and_schedule() 方法定期触发，线程安全地读写 Manifest 元数据，确保任务调度无竞态。

Q5: WAL 如何保证可靠性？

A：每条记录计算 CRC32 校验值，写入日志文件。恢复时逐条校验校验值，防止中途 crash 导致数据损坏。归档机制支持多文件管理，防止日志过大。

Q6: 如何实现 Manifest 的原子更新？

A：写入临时 Manifest 文件（.manifest.tmp），写完后通过 rename 替换原始 Manifest 文件，利用文件系统 rename 的原子性保证更新一致。

Q7: 查询路径中为何 L0 层要从新到旧查找？

A：L0 层文件未压缩、未排序，可能包含重复 key。需要按照生成逆顺序（文件 ID 从大到小）查找，确保读取到最新写入的值。

Q8: 如何避免查询一致性问题？

A：刷盘和 Compaction 都通过后台线程异步执行，所有可见数据均来自已持久化的 MemTable/WAL/SST；查询时使用共享锁保障 Manifest 的一致性；写入则使用独占锁和条件变量控制刷盘与同步。

Q9: 用到了哪些C++17特性？

A：

- 使用 std::shared_mutex 与 shared_lock 实现了 manifest 的读多写少场景下的高效并发控制；
- 查询接口中用 std::optional 表达“可能为空”的返回，提升了代码的可读性与健壮性；
- 使用 std::filesystem 管理 WAL 文件、归档路径与持久化，检查并创建目录，重命名文件，提升了文件操作的可移植性；

Qn: 其它技术点

  技术点          	面试官可能会问的问题                  	建议回答要点                                  
  WAL 多文件管理    	如何设计多 WAL 文件？写入与归档如何触发？     	每个 MemTable 对应一个 WAL；Flush 后归档 WAL，并维护 Manifest 映射
  WAL 瘦身       	如何实现瘦身？                     	重放WAL到内存 -> 重写WAL（可覆盖无效操作）              
  Crash 恢复机制   	如何保证重启后数据不丢失？               	重放所有 WAL 中未刷盘数据；利用 Manifest 恢复 SST 状态   
  MemTable 刷盘机制	是主动触发还是被动定时？                	条件变量 + 唤醒机制；后台线程阻塞等待刷盘信号或定时触发           
  Compaction 策略	如何触发 compact？合并哪些文件？        	Level0 写入满后触发；根据 Key Range 与大小选择文件合并    
  Bloom Filter 	怎么构建？怎么避免误判？                	SST Manifest 用于管理sst files，在初始化SST Manifest时，为每一个sst file构建并关联一个bloom filter，将该sst file的key填入；只用于过滤，不做最终判断，允许假阳性;
  Snappy 压缩    	为什么要引入压缩？是否必需？              	降低磁盘占用；可选模块，接口封装良好                      
  并发控制         	如何防止刷盘与写入冲突？                	flush_mutex + flush_cv 保证刷盘同步，Immutable MemTable 隔离写入
  文件一致性        	rename + tmp file 方案如何实现原子性？	修改 WAL/SST/Manifest 时先写 tmp，fsync 后 rename；避免中断导致损坏
  模块解耦         	模块之间如何协作？可否单测？              	各模块依赖注入、接口清晰，可 Mock；便于单测与调试             


