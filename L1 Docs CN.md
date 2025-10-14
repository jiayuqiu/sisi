# SISI 项目：一级文档

## 概述

SISI 项目是一个基于 Python 的系统，用于分析航运网络，主要关注：
- 识别中国的煤炭码头
- 生成煤炭船舶的 MMSI 列表
- 分析码头运输指标
- 绘制码头间的聚类路线图

该系统主要依赖点在多边形内的运算来匹配船舶事件与码头位置。

## 基础设施设置

### 1. 数据库初始化

```bash
# 使用所需表初始化数据库
python -m sisi_ops.python.infrastructure.main_init_db --stage_env=dev [--force]
```

**用途：** 此命令使用 SQLAlchemy ORM 在 MySQL 数据库中创建所有必需的表，包括：
- 码头多边形的维度表
- 船舶统计数据的维度表
- 事件和 OD 对的事实表

**参数：**
- `--stage_env` (str, 必需)：处理阶段环境（例如，dev、prod）
- `--force` (标志, 可选)：如果提供，则强制重新创建表

### 2. 上传数据

```bash
# 上传 2023 年 1 月数据集
python -m sisi_ops.python.infrastructure.main_upload_data --stage_env=dev --year=2023 --start_month=1 --end_month=1
```

**用途：** 将事件、静态数据和多边形数据上传到数据库。

**参数：**
- `--stage_env` (str, 必需)：处理阶段环境
- `--year` (int, 必需)：数据年份
- `--start_month` (int, 必需)：数据起始月份
- `--end_month` (int, 必需)：数据结束月份

### 3. 环境配置

在项目根目录创建 `.env` 文件，包含以下内容：

```
SISI_DB_TYPE=mysql
SISI_DB_HOST=127.0.0.1
SISI_DB_PORT=3306
SISI_DB_USER=your_username
SISI_DB_PASSWORD=your_password

DATA_PATH=/your/data/path
ROOT_PATH=/your/project/path
TEST_STAGE_ENV=dev
```

## ShoreNet 操作工作流程

### 步骤 1：上传船舶统计数据

```bash
# 上传船舶统计数据
python -m sisi_ops.python.ShoreNet.main_upload_statics --stage_env=dev
```

**用途：** 将静态船舶数据（MMSI、船舶类型、尺寸等）上传到数据库。

**参数：**
- `--stage_env` (str, 必需)：处理阶段环境

**使用示例：** 加载船舶注册数据以识别煤炭运输船及其特征。

---

### 步骤 2：上传多边形数据

```bash
# 从 KML 文件上传多边形数据
python -m sisi_ops.python.ShoreNet.main_upload_polygon --stage_env=dev
```

**用途：** 将代表码头的多边形数据上传到数据库。多边形通常在 Google Earth Pro 中创建并保存为 KML 文件。

**参数：**
- `--stage_env` (str, 必需)：处理阶段环境

**数据源：** 存储在 `data/{stage_env}/kml/` 目录中的 KML 文件

---

### 步骤 3：上传事件数据

```bash
# 上传事件数据（船舶移动）
python -m sisi_ops.python.ShoreNet.main_upload_events --stage_env=dev --year=2023 --start_month=1 --end_month=12
```

**用途：** 将船舶事件数据（AIS 位置、时间戳、状态）上传到数据库。

**参数：**
- `--stage_env` (str, 必需)：处理阶段环境
- `--year` (int, 必需)：数据年份
- `--start_month` (int, 必需)：数据起始月份
- `--end_month` (int, 必需)：数据结束月份

**示例：**
```bash
# 上传 2023 年第一季度的事件
python -m sisi_ops.python.ShoreNet.main_upload_events --stage_env=dev --year=2023 --start_month=1 --end_month=3
```

---

### 步骤 4：将事件匹配到多边形

```bash
# 将事件映射到码头多边形
python -m sisi_ops.python.ShoreNet.main_map_events_polygons --stage_env=dev --year=2023 --start_month=2 --end_month=12 --reset_flag=true
```

**用途：** 将事件数据与多边形数据匹配，以识别码头停靠。这是核心的点在多边形内操作。

**参数：**
- `--stage_env` (str, 必需)：处理阶段环境
- `--year` (int, 必需)：数据年份
- `--start_month` (int, 必需)：数据起始月份
- `--end_month` (int, 必需)：数据结束月份
- `--reset_flag` (bool, 可选)：重置标志以清除先前的分配

**处理流程：**
1. 从数据库加载事件数据
2. 从 KML 文件或数据库加载多边形定义
3. 使用点在多边形内算法确定每个事件发生在哪个码头（如果有）
4. 使用多边形分配更新数据库

**性能说明：** 匹配过程使用以下任一方式：
- 标准 Python 实现
- 优化的 Cython 实现，速度提高 10 倍

---

### 步骤 5：使用 DBSCAN 聚类识别新码头位置

```bash
# 对没有多边形分配的事件运行 DBSCAN 聚类
python -m sisi_ops.python.ShoreNet.main_dbscan_events --stage_env=dev --year=2023 --start_month=1 --end_month=12
```

**用途：** 使用 DBSCAN 算法对事件进行聚类，以识别没有多边形的潜在码头位置。

**参数：**
- `--stage_env` (str, 必需)：处理阶段环境
- `--year` (int, 必需)：数据年份
- `--start_month` (int, 必需)：数据起始月份
- `--end_month` (int, 必需)：数据结束月份

**处理流程：**
1. 识别没有多边形分配的事件
2. 使用地理距离应用 DBSCAN 聚类算法
3. 围绕聚类创建凸包以定义新的多边形边界
4. 输出新的多边形定义以供人工审核

**输出：** 可以导入到 Google Earth Pro 或直接导入数据库的新多边形定义。

---

### 步骤 7：计算起点-终点对

```bash
# 根据事件序列计算 OD 对
python -m sisi_ops.python.ShoreNet.main_mapping_od_paris --stage_env=dev --year=2023
```

**用途：** 根据顺序码头访问计算航运路线的起点-终点对。

**参数：**
- `--stage_env` (str, 必需)：处理阶段环境
- `--year` (int, 必需)：数据年份

**处理流程：**
1. 分析每艘船的码头访问序列
2. 识别起点和终点对
3. 计算行程统计数据（持续时间、距离、频率）
4. 将 OD 对数据存储在数据库中

**输出：** 形成航运网络分析和路线聚类基础的 OD 对。

---

## 完整工作流程示例

以下是处理 2023 年数据的完整工作流程：

```bash
# 步骤 1：初始化数据库（仅第一次）
python -m sisi_ops.python.infrastructure.main_init_db --stage_env=dev --force

# 步骤 2：上传静态船舶数据
python -m sisi_ops.python.ShoreNet.main_upload_statics --stage_env=dev

# 步骤 3：上传码头多边形定义
python -m sisi_ops.python.ShoreNet.main_upload_polygon --stage_env=dev

# 步骤 4：上传全年的船舶事件
python -m sisi_ops.python.ShoreNet.main_upload_events --stage_env=dev --year=2023 --start_month=1 --end_month=12

# 步骤 5：将事件匹配到多边形
python -m sisi_ops.python.ShoreNet.main_map_events_polygons --stage_env=dev --year=2023 --start_month=1 --end_month=12 --reset_flag=true

# 步骤 6：从未匹配的事件中识别新码头位置
python -m sisi_ops.python.ShoreNet.main_dbscan_events --stage_env=dev --year=2023 --start_month=1 --end_month=12

# 步骤 7：计算起点-终点对
python -m sisi_ops.python.ShoreNet.main_mapping_od_paris --stage_env=dev --year=2023
```

## VS Code 调试配置

项目在 `.vscode/launch.json` 中包含预配置的 VS Code 启动配置：

### 基础设施配置

**main_init_db:**
```json
{
    "name": "main_init_db",
    "type": "debugpy",
    "request": "launch",
    "module": "sisi_ops.python.infrastructure.main_init_db",
    "args": ["--stage_env=dev", "--force"]
}
```

**main_upload_data:**
```json
{
    "name": "main_upload_data",
    "type": "debugpy",
    "request": "launch",
    "module": "sisi_ops.python.infrastructure.main_upload_data",
    "args": ["--stage_env=dev", "--year=2023", "--start_month=10", "--end_month=12"]
}
```

### ShoreNet 配置

**main_map_events_polygons:**
```json
{
    "name": "main_map_events_polygons",
    "type": "debugpy",
    "request": "launch",
    "module": "sisi_ops.python.ShoreNet.main_map_events_polygons",
    "args": ["--stage_env=dev", "--year=2023", "--start_month=2", "--end_month=12", "--reset_flag=true"]
}
```

**main_od_pairs:**
```json
{
    "name": "main_od_pairs",
    "type": "debugpy",
    "request": "launch",
    "module": "sisi_ops.python.ShoreNet.main_mapping_od_paris",
    "args": ["--stage_env=dev", "--year=2023"]
}
```

## 数据存储结构

```
data/
├── dev/                    # 开发环境
│   ├── events/            # 船舶事件数据
│   ├── kml/               # 多边形 KML 文件
│   ├── statics/           # 船舶静态数据
│   └── tmp/               # 临时文件
├── dummy/                 # 测试/虚拟数据
│   ├── ais/
│   ├── events/
│   ├── kml/
│   └── statics/
└── output/                # 分析结果
    ├── demand_mmsi.json   # 船舶 MMSI 列表
    └── csv/               # 导出文件
```

## 故障排除

### 常见问题

1. **数据库连接错误：**
   - 验证 `.env` 文件配置
   - 检查 MySQL 服务是否正在运行
   - 验证凭据

2. **缺少 KML 文件：**
   - 确保 KML 文件位于 `data/{stage_env}/kml/` 目录中
   - 检查文件命名约定
   - 验证 KML 文件结构

3. **点在多边形内性能问题：**
   - 对大型数据集使用 Cython 优化版本
   - 考虑批量处理事件
   - 向数据库添加空间索引

4. **大型数据集的内存问题：**
   - 按月分块处理数据
   - 使用 pandas 分块读取文件
   - 考虑升级到 PySpark 以提高可扩展性

## 测试

项目在 `tests/` 目录中包含单元测试：

```bash
# 运行所有测试
pytest tests/

# 运行特定模块测试
pytest tests/ShoreNet/
pytest tests/infrastructure/
```

## 文档

### Sphinx 文档

生成 API 文档：

```bash
cd docs
conda run -n sisi make html
```

在以下位置查看文档：`docs/_build/html/index.html`

### 模块文档

- 基础设施模块：[sisi_ops/python/infrastructure/README.md](./sisi_ops/python/infrastructure/README.md)
- ShoreNet 模块：[sisi_ops/python/ShoreNet/README.md](./sisi_ops/python/ShoreNet/README.md)


## 支持与贡献

如有问题或想要贡献，请参阅主 [README.md](./README.md) 以了解项目概述和开发状态。
