# 自动数据蒸馏软件 - 项目结构

```
NewDatasetCreator/
├── config.yaml                    # 主配置文件（根目录、模型、日志等全局配置）
├── requirements.txt               # 运行时依赖包列表
├── main.py                        # 主入口：UI 启动 / 状态检查
├── PROJECT_STRUCTURE.md           # 项目结构说明（本文件）
│
├── src/                           # 核心源码目录
│   ├── ui/                        # Web UI 模块目录
│   │   ├── __init__.py            # UI 包初始化
│   │   ├── download_tab.py        # 数据下载标签页
│   │   ├── process_tab.py         # 数据加工标签页
│   │   ├── distill_tab.py         # 蒸馏生成标签页
│   │   ├── model_tab.py           # 模型管理标签页
│   │   └── manage_tab.py          # 数据管理标签页
│   ├── ui_launcher.py             # UI 启动器，整合各标签页
│   ├── dataset_downloader.py      # 数据集下载模块（HF / ModelScope / URL）
│   ├── dataset_previewer.py       # 数据预览模块，多格式预览与智能列显示
│   ├── format_converter.py        # 格式转换模块（CSV/JSON/JSONL/Excel/Markdown/Arrow）
│   ├── field_extractor.py         # 字段提取模块（扁平结构字段识别与抽取）
│   ├── universal_field_extractor.py # 通用字段提取模块（嵌套 JSON / HF 数据集）
│   ├── data_cleaner.py            # 数据清洗模块（空值/去重/敏感词/PII/标准化）
│   ├── data_merger.py             # 数据合并模块（同结构数据集合并与去重）
│   ├── distill_generator.py       # 蒸馏生成模块（多策略 LLM 数据生成）
│   ├── data_manager.py            # 数据管理模块（raw/processed/distilled/backup 管理）
│   ├── model_manager.py           # 模型管理模块（OpenAI/vLLM/SGlang/Ollama 调度）
│   ├── config_manager.py          # 配置管理模块（读取/更新配置与模型信息）
│   ├── log_manager.py             # 日志管理模块（统一日志通道）
│   ├── state_manager.py           # 状态管理模块（任务状态与进度）
│   ├── utils.py                   # 通用工具封装（文件/路径/加解密等）
│   └── __init__.py                # 包初始化
│
├── data/                          # 数据目录（默认 root_dir=./data）
│   ├── raw/                       # 原始下载数据（含HF/ModelScope/URL 结果）
│   ├── processed/                 # 处理后数据（格式转换 / 清洗 / 提取 / 合并输出）
│   ├── distilled/                 # 蒸馏生成后的数据
│   ├── backup/                    # 数据备份目录
│   ├── logs/                      # 各模块运行日志文件
│   └── temp/                      # 临时文件与中间产物
│
├── logs/                          # 系统级日志目录（入口/全局日志）
│
├── DesignFiles/                   # 设计文档（各模块详细设计说明）
│
└── ExplainFiles/                  # 配置与调用说明文档
```

## 核心功能模块概览

### 1. 数据获取与预览
- `dataset_downloader.py`：
	- 支持 HuggingFace / ModelScope / HTTP(S) URL 多源下载
	- 支持断点续传、自动重试和缓存清理
	- 任务进度跟踪、下载元数据与日志记录
- `dataset_previewer.py`：
	- 支持 JSON/JSONL/CSV/TSV/TXT/Arrow/Parquet 等多格式预览
	- 目录级扫描、多文件预览、智能列隐藏与长文本截断
	- 提供快速摘要与 CLI 接口

### 2. 数据格式转换
- `format_converter.py`：
	- 支持 CSV / JSON / JSONL / Excel / Markdown / Arrow 互转
	- 支持 Hugging Face Datasets(Arrow) 读写
	- 分片处理、防止内存溢出，统一元数据与进度跟踪

### 3. 字段提取与结构化
- `field_extractor.py`：
	- 针对扁平结构数据（CSV/Excel/JSON/JSONL/Markdown）的字段识别
	- 支持字段选择、重命名、过滤条件、多文件分片抽取与断点续传
- `universal_field_extractor.py`：
	- 针对嵌套 JSON 和 HF 数据集的字段路径枚举（a.b[0].c 形式）
	- 提供通用字段抽取 API，统一输出 JSONL

### 4. 数据清洗与合并
- `data_cleaner.py`：
	- 支持空值过滤、模糊去重（字段级）、敏感词过滤、PII 脱敏、文本标准化
	- 输出清洗后的数据文件 + 详细 `clean_report.json` 和 `meta.json`
- `data_merger.py`：
	- 支持同结构数据集的纵向合并（新建/追加）、字段一致性校验
	- 支持按字段去重、合并元数据与“合并信息.txt”说明

### 5. 蒸馏生成与模型管理
- `distill_generator.py`：
	- 提供扩充/增强/改写/分类/Q→A/自定义等多种蒸馏策略
	- 支持 JSON/JSONL/流式 JSONL 大规模生成，带 checkpoint 断点续跑和质量评估
- `model_manager.py`：
	- 管理 OpenAI / vLLM / SGlang / Ollama 等多种模型后端
	- 提供统一 `generate_text` 接口与模型测试、状态统计

### 6. 数据管理
- `data_manager.py`：
	- 管理 raw / processed / distilled / backup 各类数据集
	- 支持列表、预览、搜索、备份/恢复、删除和存储统计

### 7. UI 与系统支撑
- `ui/` 目录：
	- 包含 `download_tab.py`, `process_tab.py`, `distill_tab.py`, `model_tab.py`, `manage_tab.py` 等分模块的 UI 实现
- `ui_launcher.py`：
	- 基于 Gradio 的多标签页 Web UI，整合各标签页模块
- `config_manager.py` / `log_manager.py` / `state_manager.py` / `utils.py`：
	- 提供统一配置、日志、任务状态和通用工具封装

## 典型使用方式

### 启动 Web 界面
在项目根目录执行：

```bash
python main.py               # 默认启动 Web UI（端口 7860）
```

或使用简化入口：

```bash
python start_ui.py
```

### 命令行工具示例
各核心模块均提供单独 CLI，可在 `src/` 下调用，例如：

```bash
# 数据预览
python -m src.dataset_previewer data/raw/sample.jsonl --max-rows 20

# 格式转换
python -m src.format_converter --source data/raw/a.jsonl --target csv

# 数据清洗
python -m src.data_cleaner clean --source data/raw/a.jsonl --operations remove_empty deduplicate
```

## 特性摘要

- 支持多源数据下载和多格式处理（含 HF/Arrow）
- 覆盖“下载 → 预览 → 转换 → 提取 → 清洗 → 合并 → 蒸馏 → 管理”的完整链路
- 支持 GB 级数据分片处理与断点续传
- 统一的任务状态、日志和元数据记录，便于追踪与调试
