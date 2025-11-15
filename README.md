# 自动数据蒸馏软件（NewDatasetCreator）

自动数据蒸馏软件是一个面向数据工程和模型训练前期准备的工具集，提供从「数据获取 → 格式转换 → 字段提取 → 数据合并 → 清洗 → 蒸馏生成」的端到端流程支持，并内置模型管理和 Web 界面。

本仓库的设计文档和 Explain 文档已系统整理，所有说明均以当前源码为唯一依据，不包含未实现的“理想功能”。

---

## 1. 环境要求

- Python 版本：3.8+
- 操作系统：Windows / Linux / macOS
- 建议：使用虚拟环境（`venv` 或 Conda）

---

## 2. 安装与初始化

在项目根目录执行：

```bash
cd d:\work\backup\NewDatasetCreator
pip install -r requirements.txt
```

（如使用虚拟环境，请先激活再安装依赖。）

可选：在 PowerShell 下做一次语法检查（这里用 `python -c`，避免 here-doc 在 PowerShell 中报错）：

```powershell
python -c "import compileall; ok = compileall.compile_dir('d:/work/backup/NewDatasetCreator/src', force=True, quiet=1); print('COMPILEALL_OK=', ok)"
```

若输出中包含 `COMPILEALL_OK= True`，说明源码在语法层面通过检查。

---

## 3. 启动方式

### 3.1 Web 界面（推荐入口）

统一入口为 `main.py`：

```bash
# 默认端口 7860
python main.py

# 指定端口
python main.py --port 8080

# 创建公共链接（Gradio share）
python main.py --share
```

启动成功后，在浏览器访问：

- 本地访问：`http://localhost:7860`（或你指定的端口）

### 3.2 命令行模式

目前命令行模式主要作为占位入口：

```bash
python main.py --cli
```

### 3.3 系统状态检查

快速查看数据目录、活跃任务和在线模型数：

```bash
python main.py --status
```

---

## 4. 目录结构概览

```text
NewDatasetCreator/
  config.yaml                # 全局配置文件
  main.py                    # 统一启动入口（UI / CLI / 状态）
  requirements.txt           # 依赖列表
  src/
    config_manager.py        # 配置管理
    log_manager.py           # 日志管理
    state_manager.py         # 状态管理
    utils.py                 # 工具函数
    ui_launcher.py           # Gradio Web UI 启动器
    dataset_downloader.py    # 数据获取模块
    format_converter.py      # 格式转换模块
    field_extractor.py       # 字段提取模块
    data_merger.py           # 数据合并模块
    data_cleaner.py          # 数据清洗模块
    distill_generator.py     # 蒸馏生成模块
    model_manager.py         # 模型管理模块
  ExplainFiles/              # 各模块配置和调用指南（v2.0）
  DesignFiles/               # 各模块设计文档
  data/                      # 数据目录（原始/处理后/临时/日志等）
```

> 以上为说明性示意，实际结构以仓库为准。

---

## 5. 核心模块简介

### 5.1 数据获取模块（`dataset_downloader.py`）

- 支持 Hugging Face / ModelScope / URL 等多种数据源；
- 通过 `DatasetDownloader` 管理下载任务，记录进度与元数据；
- 详细配置与调用见：`ExplainFiles/核心功能_数据获取模块配置和调用指南.md`。

### 5.2 格式转换模块（`format_converter.py`）

- 支持 JSONL/CSV/Excel/JSON 等常见格式之间转换；
- 提供 `FormatConverter` 类与模块级便捷函数；
- 详见：`ExplainFiles/核心功能_格式转换模块配置和调用指南.md`。

### 5.3 字段提取模块（`field_extractor.py`）

- 通过 `FieldExtractor` 对数据文件字段进行识别与提取；
- 支持字段筛选、重命名和简单过滤；
- 详见：`ExplainFiles/核心功能_字段提取模块配置和调用指南.md`。

### 5.4 数据合并模块（`data_merger.py`）

- 支持多文件纵向合并（`merge/append` 两种模式）；
- 可选开启简单去重，并生成合并元数据；
- 详见：`ExplainFiles/核心功能_数据合并模块配置和调用指南.md`。

### 5.5 数据清洗模块（`data_cleaner.py`）

- 提供常见的数据清洗操作（去空值、去重、敏感内容处理等）；
- 详见：`ExplainFiles/核心功能_数据清洗模块配置和调用指南.md`。

### 5.6 模型管理模块（`model_manager.py`）

- 统一管理 vLLM / OpenAI / SGlang / Ollama 等模型配置与状态；
- 提供 `generate_text` 统一文本生成接口；
- 详见：`ExplainFiles/核心功能_模型管理配置和调用指南.md`。

---

## 6. 文档体系

- 设计文档（面向开发者）：位于 `DesignFiles/`，如：
  - `核心功能_数据获取模块设计文档.md`
  - `核心功能_数据合并模块设计文档.md`
  - `自动数据蒸馏软件模块化设计文档.md`

- 配置与调用指南（Explain 文档，面向使用者）：位于 `ExplainFiles/`，如：
  - `核心功能_数据获取模块配置和调用指南.md`
  - `核心功能_格式转换模块配置和调用指南.md`
  - `基础支撑层配置和调用指南.md`
  - `交互层配置和调用指南.md`

所有 Explain 文档均已刷新为 v2.0，只描述当前源码实际存在的接口与配置。

---

## 7. 已知限制与注意事项

- 仓库已删除历史测试脚本与冗余入口，文档中出现的部分测试代码示例仅作说明，不对应真实存在的测试文件。
- 部分实现存在局限（例如数据合并去重策略的 `keep_last` 语义），Explain 文档中已用保守表述说明“以源码为准”。
- 如需在本项目之上构建 HTTP API、前端控制台或调度系统，请在现有模块之上自行封装。

---

## 8. 典型使用流程

1. 启动 Web UI：`python main.py`；
2. 在“模型配置”中添加可用模型（可选）；
3. 通过“数据集下载”获取原始数据，或直接上传本地数据；
4. 使用“数据加工”完成格式转换、字段提取、合并、清洗；
5. 使用“蒸馏生成”结合模型完成数据增强/蒸馏；
6. 在“数据管理”中查看和导出处理结果。

如在使用中遇到问题，可优先查看对应模块的 Explain 文档和日志输出，再结合设计文档排查。