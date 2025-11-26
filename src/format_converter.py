#!/usr/bin/env python3
"""
格式转换模块

本模块提供多种数据格式的双向转换功能，支持markdown/excel/csv/json/jsonl/arrow格式互转。
功能特点：
- 支持六种格式双向互转：markdown ↔ excel ↔ csv ↔ json ↔ jsonl ↔ arrow
- 支持Hugging Face Datasets格式（Arrow）的读取和生成
- 大文件分片处理，避免内存溢出
- 数据类型智能适配（日期、文本、嵌套结构）
- 特殊字符和编码精细化处理
- 转换质量校验与异常追溯
- 断点续传与进度记录

设计原则：
- 模块化设计，职责单一，仅负责格式转换
- 完全独立，仅依赖基础支撑模块
- 健壮的错误处理和数据质量保障
- 详细的转换日志和进度追踪

作者：自动数据蒸馏软件团队
版本：v1.1
许可：商业软件
"""

import os
import sys
import json
import csv
import hashlib
import argparse
import logging
import time
import threading
import shutil
import random
import string
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, TypedDict, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor

# 统一依赖管理导入
from .dependencies import (
    pd, jsonlines,
    openpyxl, HAS_OPENPYXL as EXCEL_WRITE_AVAILABLE,
    chardet, HAS_CHARDET as CHARDET_AVAILABLE,
    datasets, pa, HAS_DATASETS, HAS_PYARROW
)

# 检查核心依赖
if pd is None:
    print("错误: 缺少核心依赖库 pandas")
    print("请运行: pip install pandas")
    exit(1)

if jsonlines is None:
    print("错误: 缺少核心依赖库 jsonlines")
    print("请运行: pip install jsonlines")
    exit(1)

# Arrow格式支持
ARROW_AVAILABLE = HAS_DATASETS and HAS_PYARROW


# 导入统一异常类
try:
    from .exceptions import ConvertError, UnsupportedFormatError, ConvertFailedError
except ImportError:
    # 如果导入失败，使用本地定义（向后兼容）
    class ConvertError(Exception):
        """格式转换相关异常类"""
        pass
    
    class UnsupportedFormatError(ConvertError):
        """不支持的格式异常"""
        pass
    
    class ConvertFailedError(ConvertError):
        """转换失败异常"""
        pass


class TaskParams(TypedDict, total=False):
    """
    任务参数类型定义
    
    定义格式转换任务的所有配置参数，支持类型检查和IDE提示。
    """
    task_id: str                           # 任务唯一标识
    source_path: str                       # 源文件路径（必填）
    target_format: str                     # 目标格式（必填）
    output_dir: Optional[str]              # 输出目录
    chunk_size: Optional[int]              # 分片大小（行）
    encoding: Optional[str]                # 源文件编码
    output_encoding: Optional[str]         # 输出编码
    excel_sheet: Optional[str]             # Excel工作表名
    csv_delimiter: Optional[str]           # CSV分隔符
    markdown_table_header: Optional[bool]  # Markdown表头
    json_ensure_ascii: Optional[bool]      # JSON ASCII编码
    date_fields: Optional[List[str]]       # 日期类型字段
    text_fields: Optional[List[str]]       # 文本类型字段
    nest_depth: Optional[int]              # 嵌套结构展开深度
    keep_nest: Optional[bool]              # 保留嵌套结构
    skip_error_rows: Optional[bool]        # 跳过错误行
    skip_empty_rows: Optional[bool]        # 跳过空行
    clean_invisible: Optional[bool]        # 清理不可见字符
    split_file: Optional[bool]             # 拆分大文件
    preview_rows: Optional[int]            # 预览行数
    # Arrow格式专用参数
    arrow_split: Optional[str]             # Arrow数据集分割名称（默认'train'）
    max_rows: Optional[int]                # 最大行数限制（用于大数据集）


class ConvertMeta(TypedDict):
    """
    转换元数据类型定义
    
    记录转换任务的详细信息和质量统计数据。
    """
    task_id: str
    source_path: str
    source_format: str
    target_path: str
    target_format: str
    start_time: str
    end_time: str
    status: str
    params: TaskParams
    data_quality: Dict[str, Any]
    source_hash: str
    error_log_path: Optional[str]


class ConfigManager:
    """
    配置管理器
    
    负责管理格式转换模块的各项配置参数，包括文件处理配置、
    格式专属配置、性能优化配置等。
    
    设计原则：
    - 提供合理的默认配置
    - 支持运行时配置修改
    - 确保配置参数的有效性
    """
    
    def __init__(self):
        """初始化配置管理器，设置默认配置参数"""
        self.config = {
            'base': {
                'root_dir': './data',
                'chunk_size': 1000,
                'encoding': 'utf-8'
            },
            'process': {
                'excel_engine': 'openpyxl',
                'csv_delimiter': ',',
                'max_preview_rows': 100,
                'excel_max_rows': 1048576,
                'excel_max_cols': 16384
            },
            'convert': {
                'default_output_dir': './data/processed',
                'temp_dir': './data/temp',
                'max_memory_usage': '500MB',
                'enable_progress_bar': True
            }
        }
    
    def get_config(self, key: str, default=None):
        """
        获取配置值
        
        Args:
            key: 配置键，支持点分割的层级键
            default: 默认值
            
        Returns:
            配置值或默认值
        """
        keys = key.split('.')
        value = self.config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def set_config(self, key: str, value: Any):
        """
        设置配置值
        
        Args:
            key: 配置键
            value: 配置值
        """
        keys = key.split('.')
        config = self.config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
    
    def ensure_directories(self):
        """
        确保必要的目录结构存在
        
        创建处理数据需要的各个目录：
        - processed: 转换后数据存储
        - temp: 临时文件存储
        """
        root_dir = Path(self.get_config('base.root_dir'))
        directories = ['processed', 'temp']
        
        for dir_name in directories:
            dir_path = root_dir / dir_name
            dir_path.mkdir(parents=True, exist_ok=True)


class Logger:
    """
    日志管理器
    
    负责格式转换模块的日志记录，支持控制台输出和文件输出。
    提供不同级别的日志记录功能，便于调试和监控。
    
    特性：
    - 支持任务ID关联的日志记录
    - 自动文件日志轮转
    - 优雅的错误处理
    """
    
    def __init__(self, name: str = "format_converter"):
        """
        初始化日志管理器
        
        Args:
            name: 日志器名称
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # 避免重复添加处理器
        if not self.logger.handlers:
            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # 文件处理器
            log_dir = Path('./data/logs')
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"format_converter_{datetime.now().strftime('%Y%m%d')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            
            # 格式化器
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(formatter)
            file_handler.setFormatter(formatter)
            
            self.logger.addHandler(console_handler)
            self.logger.addHandler(file_handler)
    
    def info(self, msg: str, task_id: str = ""):
        """记录信息级别日志"""
        if task_id:
            msg = f"[{task_id}] {msg}"
        self.logger.info(msg)
    
    def error(self, msg: str, task_id: str = ""):
        """记录错误级别日志"""
        if task_id:
            msg = f"[{task_id}] {msg}"
        self.logger.error(msg)
    
    def warning(self, msg: str, task_id: str = ""):
        """记录警告级别日志"""
        if task_id:
            msg = f"[{task_id}] {msg}"
        self.logger.warning(msg)
    
    def debug(self, msg: str, task_id: str = ""):
        """记录调试级别日志"""
        if task_id:
            msg = f"[{task_id}] {msg}"
        self.logger.debug(msg)


class ProgressTracker:
    """
    进度跟踪器
    
    负责跟踪转换任务的进度状态，包括处理速度、剩余时间、
    错误状态等信息。支持线程安全的状态更新。
    
    状态定义：
    - pending: 等待开始
    - running: 正在转换
    - paused: 暂停中
    - completed: 转换完成
    - failed: 转换失败
    """
    
    def __init__(self, task_id: str):
        """
        初始化进度跟踪器
        
        Args:
            task_id: 任务唯一标识符
        """
        self.task_id = task_id
        self.status = "pending"
        self.progress = 0
        self.processed_rows = 0
        self.total_rows = 0
        self.start_time = None
        self.speed = 0
        self.eta = 0
        self.last_update = time.time()
        self.last_processed_rows = 0  # 上次更新的处理行数，用于计算速度
        self.error_msg = ""
        self.output_file = ""  # 添加输出文件路径记录
        self.lock = threading.Lock()
    
    def start(self, total_rows: int = 0):
        """开始进度跟踪"""
        with self.lock:
            self.status = "running"
            self.total_rows = total_rows
            self.start_time = time.time()
            self.last_update = self.start_time
            self.last_processed_rows = 0  # 重置上次处理行数
    
    def update(self, processed_rows: int):
        """更新处理进度"""
        with self.lock:
            # 保存上一次的处理行数，用于计算速度
            previous_rows = self.processed_rows
            self.processed_rows = processed_rows
            
            if self.total_rows > 0:
                self.progress = int((processed_rows / self.total_rows) * 100)
            
            current_time = time.time()
            elapsed = current_time - self.last_update
            
            # 计算速度：使用增量行数和时间差
            if elapsed > 0 and processed_rows > previous_rows:
                rows_increment = processed_rows - previous_rows
                self.speed = rows_increment / elapsed
                
                # 计算预计剩余时间
                if self.speed > 0 and self.total_rows > processed_rows:
                    remaining_rows = self.total_rows - processed_rows
                    self.eta = remaining_rows / self.speed
            elif self.start_time and elapsed > 0:
                # 如果无法计算增量速度，使用总时间计算平均速度
                total_elapsed = current_time - self.start_time
                if total_elapsed > 0:
                    self.speed = processed_rows / total_elapsed
                    if self.speed > 0 and self.total_rows > processed_rows:
                        remaining_rows = self.total_rows - processed_rows
                        self.eta = remaining_rows / self.speed
            
            self.last_update = current_time
    
    def complete(self, output_file: str = ""):
        """标记任务完成"""
        with self.lock:
            self.status = "completed"
            self.progress = 100
            if output_file:
                self.output_file = output_file
    
    def fail(self, error_msg: str = ""):
        """标记任务失败"""
        with self.lock:
            self.status = "failed"
            self.error_msg = error_msg
    
    def get_info(self) -> Dict[str, Any]:
        """获取进度信息"""
        with self.lock:
            return {
                'task_id': self.task_id,
                'status': self.status,
                'progress': self.progress,
                'processed_rows': self.processed_rows,
                'total_rows': self.total_rows,
                'speed': f"{self.speed:.1f} rows/s" if self.speed > 0 else "计算中",
                'eta': f"{self.eta:.0f}s" if self.eta > 0 else "未知",
                'error_msg': self.error_msg,
                'output_file': self.output_file  # 添加输出文件路径
            }


class FormatConverter:
    """
    格式转换器主类
    
    这是转换模块的核心类，负责管理所有的转换任务。
    支持多种格式的转换，包括数据类型适配、错误处理等高级功能。
    
    设计特点：
    - 单一职责：专注于格式转换，不涉及其他业务逻辑
    - 任务管理：支持多任务并发和状态跟踪
    - 容错机制：完善的错误处理和恢复策略
    - 扩展性：易于添加新的格式支持
    """
    
    def __init__(self):
        """初始化转换器"""
        self.config = ConfigManager()
        self.logger = Logger("format_converter")
        self.config.ensure_directories()
        self.tasks = {}  # 任务存储字典
        
        # 支持的格式列表
        self.supported_formats = ['csv', 'json', 'jsonl', 'excel', 'markdown', 'arrow']
        
        # Excel行列限制
        self.excel_limits = {
            'max_rows': self.config.get_config('process.excel_max_rows'),
            'max_cols': self.config.get_config('process.excel_max_cols')
        }
    
    def generate_task_id(self) -> str:
        """
        生成唯一的任务标识符
        
        Returns:
            格式为 'conv-YYYYMMDDHHMMSS-xxxxxx' 的任务ID
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        import random
        import string
        rand_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"conv-{timestamp}-{rand_str}"
    
    def add_convert_task(self, **kwargs) -> str:
        """
        添加转换任务到任务队列
        
        Args:
            source_path: 源文件路径（必填）
            target_format: 目标格式（必填）
            output_dir: 输出目录（可选）
            **kwargs: 其他转换参数
        
        Returns:
            str: 任务ID
            
        Raises:
            ValueError: 当必需参数缺失或参数值无效时
        """
        # 验证必需参数
        if 'source_path' not in kwargs:
            raise ValueError("缺少必需参数: source_path")
        if 'target_format' not in kwargs:
            raise ValueError("缺少必需参数: target_format")
        
        source_path = kwargs['source_path']
        target_format = kwargs['target_format'].lower()
        
        # 验证文件存在
        if not os.path.exists(source_path):
            raise ValueError(f"源文件不存在: {source_path}")
        
        # 验证目标格式
        if target_format not in self.supported_formats:
            raise ValueError(f"不支持的目标格式: {target_format}")
        
        # 生成任务ID并设置任务参数
        task_id = self.generate_task_id()
        task_params = TaskParams(
            task_id=task_id,
            source_path=source_path,
            target_format=target_format,
            output_dir=kwargs.get('output_dir') or self.config.get_config('convert.default_output_dir'),
            chunk_size=kwargs.get('chunk_size') or self.config.get_config('base.chunk_size'),
            encoding=kwargs.get('encoding'),
            output_encoding=kwargs.get('output_encoding', 'utf-8'),
            excel_sheet=kwargs.get('excel_sheet', 'Sheet1'),
            csv_delimiter=kwargs.get('csv_delimiter') or self.config.get_config('process.csv_delimiter'),
            markdown_table_header=kwargs.get('markdown_table_header', True),
            json_ensure_ascii=kwargs.get('json_ensure_ascii', False),
            date_fields=kwargs.get('date_fields', []),
            text_fields=kwargs.get('text_fields', []),
            nest_depth=kwargs.get('nest_depth', 2),
            keep_nest=kwargs.get('keep_nest', False),
            skip_error_rows=kwargs.get('skip_error_rows', False),
            skip_empty_rows=kwargs.get('skip_empty_rows', False),
            clean_invisible=kwargs.get('clean_invisible', False),
            split_file=kwargs.get('split_file', False),
            preview_rows=kwargs.get('preview_rows', 0),
            # Arrow格式专用参数
            arrow_split=kwargs.get('arrow_split', 'train'),
            max_rows=kwargs.get('max_rows')
        )
        
        # 创建进度跟踪器
        tracker = ProgressTracker(task_id)
        self.tasks[task_id] = {
            'params': task_params,
            'tracker': tracker
        }
        
        self.logger.info(f"转换任务已创建: {source_path} -> {target_format}", task_id)
        return task_id
    
    def start_task(self, task_id: str) -> bool:
        """
        启动转换任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 是否成功启动
        """
        if task_id not in self.tasks:
            self.logger.error(f"任务不存在: {task_id}")
            return False
        
        task = self.tasks[task_id]
        params = task['params']
        tracker = task['tracker']
        
        try:
            self.logger.info(f"开始格式转换: {params['source_path']} -> {params['target_format']}", task_id)
            
            # 启动转换线程
            thread = threading.Thread(
                target=self._execute_convert,
                args=(task_id, params, tracker),
                daemon=True
            )
            thread.start()
            
            return True
            
        except Exception as e:
            self.logger.error(f"任务启动失败: {str(e)}", task_id)
            tracker.fail(str(e))
            return False
    
    def _execute_convert(self, task_id: str, params: TaskParams, tracker: ProgressTracker):
        """
        执行转换任务（内部方法）
        
        Args:
            task_id: 任务ID
            params: 任务参数
            tracker: 进度跟踪器
        """
        try:
            # 检测源文件格式
            source_format = self._detect_source_format(params['source_path'])
            if not source_format:
                raise ConvertError("无法识别源文件格式")
            
            self.logger.info(f"检测到源格式: {source_format}", task_id)
            
            # 准备输出目录 - 改进：可配置是否创建子目录
            use_subdirectory = params.get('use_subdirectory', True)
            if use_subdirectory:
                output_dir = Path(params['output_dir']) / task_id
            else:
                output_dir = Path(params['output_dir'])
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成目标文件路径 - 改进命名规则
            source_name = Path(params['source_path']).stem
            
            # 确定正确的文件扩展名
            format_extensions = {
                'jsonl': 'jsonl',
                'csv': 'csv', 
                'xlsx': 'xlsx',
                'json': 'json',
                'xml': 'xml',
                'markdown': 'md'  # 修复：markdown格式使用.md扩展名
            }
            
            target_extension = format_extensions.get(params['target_format'], params['target_format'])
            
            # 改进文件命名：原名_源格式2目标格式.扩展名
            source_format_clean = source_format.replace('excel', 'xlsx')
            target_format_clean = params['target_format'].replace('markdown', 'md')
            conversion_name = f"{source_format_clean}2{target_format_clean}"
            
            target_filename = f"{source_name}_{conversion_name}.{target_extension}"
            target_path = output_dir / target_filename
            
            # 执行格式转换
            tracker.start()
            
            if source_format == 'csv':
                data = self._read_csv(params['source_path'], params)
            elif source_format == 'json':
                data = self._read_json(params['source_path'], params, tracker)
            elif source_format == 'jsonl':
                data = self._read_jsonl(params['source_path'], params, tracker)
            elif source_format == 'excel':
                data = self._read_excel(params['source_path'], params)
            elif source_format == 'markdown':
                data = self._read_markdown(params['source_path'], params)
            elif source_format == 'arrow':
                data = self._read_arrow(params['source_path'], params)
            else:
                raise ConvertError(f"不支持的源格式: {source_format}")
            
            # 如果读取的是列表（非流式），设置总行数以便进度条显示
            if isinstance(data, list):
                tracker.total_rows = len(data)
            
            # 数据预处理 (现在是生成器)
            data = self._preprocess_data(data, params, task_id, tracker)
            
            # 写入目标格式
            write_stats = {}
            
            if params['target_format'] == 'csv':
                # CSV现在支持流式写入
                self._write_csv(data, target_path, params)
                # 注意：流式写入后无法直接获取准确的row_count，除非在_write_csv中统计
                # 这里我们使用tracker中的processed_rows作为近似值
                write_stats = {'row_count': tracker.processed_rows, 'fields': []}
            elif params['target_format'] == 'json':
                write_stats = self._write_json(data, target_path, params)
            elif params['target_format'] == 'jsonl':
                write_stats = self._write_jsonl(data, target_path, params)
            elif params['target_format'] == 'excel':
                if not isinstance(data, list):
                    data = list(data)
                self._write_excel(data, target_path, params)
                write_stats = {'row_count': len(data), 'fields': list(data[0].keys()) if data else []}
            elif params['target_format'] == 'markdown':
                if not isinstance(data, list):
                    data = list(data)
                self._write_markdown(data, target_path, params)
                write_stats = {'row_count': len(data), 'fields': list(data[0].keys()) if data else []}
            elif params['target_format'] == 'arrow':
                if not isinstance(data, list):
                    data = list(data)
                target_path = Path(self._write_arrow(data, target_path, params))
                write_stats = {'row_count': len(data), 'fields': list(data[0].keys()) if data else []}
            
            # 生成元数据
            # 构造符合_generate_metadata期望的数据结构
            # 注意：data可能已经被消费（如果是生成器），所以不能再使用data
            dummy_data = []
            if write_stats.get('fields'):
                dummy_data = [{k: None for k in write_stats['fields']}] # 仅用于传递字段信息
            
            meta = self._generate_metadata(task_id, params, source_format, str(target_path), dummy_data)
            # 更新真实的行数
            meta['data_quality']['row_count']['source'] = write_stats.get('row_count', 0)
            meta['data_quality']['row_count']['target'] = write_stats.get('row_count', 0)
            
            meta_path = output_dir / 'meta.json'
            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(meta, f, indent=2, ensure_ascii=False)
            
            tracker.complete(str(target_path))
            self.logger.info(f"转换完成: {target_path}", task_id)
            
        except Exception as e:
            self.logger.error(f"转换失败: {str(e)}", task_id)
            tracker.fail(str(e))
    
    def _detect_source_format(self, file_path: str) -> Optional[str]:
        """
        检测源文件格式
        
        Args:
            file_path: 文件路径
            
        Returns:
            格式字符串或None
        """
        file_path_obj = Path(file_path)
        
        # 检查是否是Arrow数据集目录
        if file_path_obj.is_dir():
            # 检查是否包含Arrow数据集文件
            arrow_indicators = [
                'dataset_info.json',
                'state.json', 
                'dataset_dict.json',  # 新增：数据集字典文件
                '*.arrow'
            ]
            
            has_arrow_files = False
            for indicator in arrow_indicators:
                if indicator.endswith('*'):
                    # 通配符匹配 - 递归搜索
                    pattern = indicator.replace('*', '')
                    found_files = list(file_path_obj.rglob(f"*{pattern}"))
                    has_arrow_files = len(found_files) > 0
                else:
                    # 精确匹配 - 递归搜索
                    found_files = list(file_path_obj.rglob(indicator))
                    has_arrow_files = len(found_files) > 0
                
                if has_arrow_files:
                    break
            
            if has_arrow_files:
                return 'arrow'
        
        # 文件扩展名检测
        suffix = file_path_obj.suffix.lower()
        format_map = {
            '.csv': 'csv',
            '.json': 'json',
            '.jsonl': 'jsonl',
            '.xlsx': 'excel',
            '.xls': 'excel',
            '.md': 'markdown',
            '.markdown': 'markdown',
            '.arrow': 'arrow'
        }
        return format_map.get(suffix)
    
    def _detect_encoding(self, file_path: str) -> str:
        """
        检测文件编码
        
        Args:
            file_path: 文件路径
            
        Returns:
            编码格式字符串
        """
        if not CHARDET_AVAILABLE:
            return 'utf-8'
        
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10240)  # 读取前10KB
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            
            # 如果检测为ascii，强制使用utf-8（因为JSON通常是UTF-8，且ascii是utf-8的子集）
            # 这样可以避免后续遇到非ascii字符时报错
            if encoding and encoding.lower() == 'ascii':
                return 'utf-8'
                
            return encoding
        except:
            return 'utf-8'
    
    def _read_csv(self, file_path: str, params: TaskParams) -> Iterator[Dict]:
        """读取CSV文件"""
        encoding = params.get('encoding') or self._detect_encoding(file_path)
        delimiter = params.get('csv_delimiter', ',')
        chunk_size = params.get('chunk_size', 1000)
        
        try:
            # 使用chunksize进行流式读取
            for chunk in pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, chunksize=chunk_size):
                # 将NaN替换为None，避免JSON序列化错误
                chunk = chunk.where(pd.notnull(chunk), None)
                for record in chunk.to_dict('records'):
                    yield record
        except Exception as e:
            raise ConvertError(f"读取CSV文件失败: {str(e)}")
    
    def _read_json(self, file_path: str, params: TaskParams, tracker: Optional[ProgressTracker] = None) -> Iterator[Dict]:
        """读取JSON文件 - 支持流式读取"""
        encoding = params.get('encoding') or self._detect_encoding(file_path)
        
        try:
            # 尝试使用ijson进行流式读取
            try:
                import ijson
                has_ijson = True
            except ImportError:
                has_ijson = False
            
            if has_ijson:
                file_size = os.path.getsize(file_path)
                last_update_time = time.time()
                
                with open(file_path, 'rb') as f:
                    # 简单的启发式检查：读取第一个非空字符
                    pos = f.tell()
                    first_char = f.read(1)
                    while first_char and first_char.isspace():
                        first_char = f.read(1)
                    f.seek(pos)
                    
                    is_list = first_char == b'['
                    
                    if is_list:
                        # 列表模式：流式读取每个元素
                        items = ijson.items(f, 'item')
                        for i, item in enumerate(items):
                            # 更新进度
                            current_time = time.time()
                            if tracker and (i % 100 == 0 or current_time - last_update_time > 0.5):
                                try:
                                    current_pos = f.tell()
                                    read_progress = int((current_pos / file_size) * 99)
                                    tracker.progress = read_progress
                                    tracker.processed_rows = i + 1
                                    tracker.last_update = current_time
                                    last_update_time = current_time
                                except:
                                    pass
                                    
                            if isinstance(item, dict):
                                yield item
                    else:
                        # 对象模式：读取整个对象（可能很大，但ijson不支持流式读取顶层对象的字段作为记录）
                        # 如果是单个对象，我们假设它是一个记录
                        # 或者我们可以尝试解析顶层字段如果是列表的话
                        # 这里简单处理：回退到普通加载，或者只yield一次
                        f.seek(0) # 重新读取
                        # ijson items('') 解析顶层对象
                        items = ijson.items(f, '')
                        for item in items:
                            if isinstance(item, dict):
                                yield item
                            elif isinstance(item, list):
                                for sub_item in item:
                                    if isinstance(sub_item, dict):
                                        yield sub_item
                return


            # 回退到普通加载
            with open(file_path, 'r', encoding=encoding) as f:
                data = json.load(f)
            
            if isinstance(data, list):
                for item in data:
                    yield item
            elif isinstance(data, dict):
                yield data
            else:
                raise ConvertError("JSON数据格式不支持")
        except Exception as e:
            raise ConvertError(f"读取JSON文件失败: {str(e)}")
    
    def _read_jsonl(self, file_path: str, params: TaskParams, tracker: Optional[ProgressTracker] = None) -> Iterator[Dict]:
        """读取JSONL文件"""
        encoding = params.get('encoding') or self._detect_encoding(file_path)
        
        try:
            file_size = os.path.getsize(file_path)
            last_update_time = time.time()
            
            with open(file_path, 'r', encoding=encoding) as f:
                for line_num, line in enumerate(f, 1):
                    # 更新进度 (基于文件读取位置，占100%进度)
                    # 优化：每0.5秒或每1000行更新一次，避免频繁更新
                    current_time = time.time()
                    if tracker and (line_num % 1000 == 0 or current_time - last_update_time > 0.5):
                        try:
                            current_pos = f.tell()
                            read_progress = int((current_pos / file_size) * 99) # 保留1%给完成状态
                            tracker.progress = read_progress
                            tracker.processed_rows = line_num
                            tracker.last_update = current_time
                            last_update_time = current_time
                        except:
                            pass
                            
                    line = line.strip()
                    
                    # 跳过空行
                    if not line:
                        continue
                    
                    # 跳过注释行
                    if line.startswith('#') or line.startswith('//'):
                        continue
                    
                    try:
                        obj = json.loads(line)
                        yield obj
                    except json.JSONDecodeError as je:
                        # 提供更详细的错误信息
                        raise ConvertError(f"line contains invalid json: {str(je)} (line {line_num})")
                    except Exception as e:
                        raise ConvertError(f"error parsing line {line_num}: {str(e)}")
            
        except ConvertError:
            raise
        except Exception as e:
            raise ConvertError(f"读取JSONL文件失败: {str(e)}")
        except ConvertError:
            raise
        except Exception as e:
            raise ConvertError(f"读取JSONL文件失败: {str(e)}")
    
    def _read_excel(self, file_path: str, params: TaskParams) -> List[Dict]:
        """读取Excel文件"""
        sheet_name = params.get('excel_sheet', 'Sheet1')
        
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            return df.to_dict('records')
        except Exception as e:
            raise ConvertError(f"读取Excel文件失败: {str(e)}")
    
    def _read_markdown(self, file_path: str, params: TaskParams) -> List[Dict]:
        """读取Markdown表格文件"""
        encoding = params.get('encoding') or self._detect_encoding(file_path)
        
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            
            # 简单的Markdown表格解析（基础实现）
            lines = content.strip().split('\n')
            table_lines = []
            in_table = False
            
            for line in lines:
                line = line.strip()
                if line.startswith('|') and line.endswith('|'):
                    if not in_table:
                        in_table = True
                    table_lines.append(line)
                elif in_table and not line:
                    break
                elif in_table:
                    table_lines.append(line)
            
            if not table_lines:
                raise ConvertError("未找到Markdown表格")
            
            # 解析表头
            header_line = table_lines[0]
            headers = [col.strip() for col in header_line.split('|')[1:-1]]
            
            # 跳过分隔行
            data_lines = table_lines[2:] if len(table_lines) > 2 else []
            
            data = []
            for line in data_lines:
                if line.startswith('|') and line.endswith('|'):
                    values = [col.strip() for col in line.split('|')[1:-1]]
                    if len(values) == len(headers):
                        data.append(dict(zip(headers, values)))
            
            return data
        except Exception as e:
            raise ConvertError(f"读取Markdown文件失败: {str(e)}")
    
    def _read_arrow(self, file_path: str, params: TaskParams) -> List[Dict]:
        """读取Arrow格式数据集（Hugging Face Datasets）"""
        if not ARROW_AVAILABLE:
            raise ConvertError("Arrow格式支持不可用，请安装: pip install datasets pyarrow")
        
        try:
            file_path_obj = Path(file_path)
            
            # 如果是目录，尝试加载整个数据集
            if file_path_obj.is_dir():
                dataset = datasets.load_from_disk(str(file_path_obj))
                
                # 处理多分割数据集
                if hasattr(dataset, 'keys') and isinstance(dataset, dict):
                    # 获取指定分割或默认使用第一个分割
                    split_name = params.get('arrow_split', 'train')
                    if split_name in dataset:
                        data_split = dataset[split_name]
                    else:
                        # 使用第一个可用分割
                        available_splits = list(dataset.keys())
                        if available_splits:
                            data_split = dataset[available_splits[0]]
                            self.logger.warning(f"未找到分割 '{split_name}'，使用 '{available_splits[0]}'")
                        else:
                            raise ConvertError("数据集中没有找到任何分割")
                else:
                    # 单分割数据集
                    data_split = dataset
                
                # 限制数据量（避免内存问题）
                max_rows = params.get('max_rows')
                if max_rows and len(data_split) > max_rows:
                    data_split = data_split.select(range(max_rows))
                    self.logger.info(f"数据集已限制为前 {max_rows} 行")
                
                # 转换为字典列表
                return [dict(item) for item in data_split]
            
            # 如果是单个Arrow文件
            elif file_path_obj.suffix.lower() == '.arrow':
                table = pa.ipc.open_file(file_path_obj).read_all()
                df = table.to_pandas()
                return df.to_dict('records')
            
            else:
                raise ConvertError(f"不支持的Arrow文件格式: {file_path}")
                
        except Exception as e:
            raise ConvertError(f"读取Arrow文件失败: {str(e)}")
    
    def _preprocess_data(self, data: Iterable[Dict], params: TaskParams, task_id: str, tracker: Optional[ProgressTracker] = None) -> Iterator[Dict]:
        """
        数据预处理
        
        Args:
            data: 原始数据
            params: 任务参数
            task_id: 任务ID
            tracker: 进度跟踪器
            
        Returns:
            处理后的数据
        """
        skipped_count = 0
        processed_count = 0
        
        for row in data:
            processed_count += 1
            
            # 更新进度
            if tracker and processed_count % 100 == 0:
                tracker.update(processed_count)
            
            # 跳过空行
            if params.get('skip_empty_rows', False):
                if not any(str(v).strip() for v in row.values()):
                    skipped_count += 1
                    continue
            
            # 清理不可见字符
            if params.get('clean_invisible', False):
                row = self._clean_invisible_chars(row)
            
            # 数据类型适配
            row = self._adapt_data_types(row, params)
            
            yield row
        
        # 确保最后更新一次进度
        if tracker:
            tracker.update(processed_count)
        
        if skipped_count > 0:
            self.logger.info(f"跳过了 {skipped_count} 行空数据", task_id)
    
    def _clean_invisible_chars(self, row: Dict) -> Dict:
        """清理不可见字符"""
        cleaned_row = {}
        for key, value in row.items():
            if isinstance(value, str):
                # 移除常见的不可见字符
                value = value.replace('\r', '').replace('\ufeff', '')
                value = ''.join(char for char in value if ord(char) >= 32 or char in '\t\n')
            cleaned_row[key] = value
        return cleaned_row
    
    def _adapt_data_types(self, row: Dict, params: TaskParams) -> Dict:
        """数据类型适配"""
        adapted_row = {}
        date_fields = params.get('date_fields', [])
        text_fields = params.get('text_fields', [])
        
        for key, value in row.items():
            # 强制文本类型
            if key in text_fields:
                adapted_row[key] = str(value) if value is not None else ''
            # 强制日期类型
            elif key in date_fields:
                adapted_row[key] = self._parse_date(value)
            else:
                adapted_row[key] = value
        
        return adapted_row
    
    def _parse_date(self, value) -> str:
        """解析日期值为ISO格式字符串"""
        if pd.isna(value) or value is None:
            return ''
        
        try:
            if isinstance(value, (int, float)):
                # Excel日期序列号
                date_obj = pd.to_datetime('1900-01-01') + pd.Timedelta(days=value-2)
                return date_obj.strftime('%Y-%m-%d')
            else:
                # 尝试解析字符串日期
                date_obj = pd.to_datetime(value)
                return date_obj.strftime('%Y-%m-%d')
        except:
            return str(value)
    
    def _write_csv(self, data: Iterable[Dict], target_path: Path, params: TaskParams):
        """写入CSV文件"""
        delimiter = params.get('csv_delimiter', ',')
        encoding = params.get('output_encoding', 'utf-8')
        chunk_size = 1000
        
        try:
            first_chunk = True
            mode = 'w'
            header = True
            chunk = []
            
            for item in data:
                chunk.append(item)
                if len(chunk) >= chunk_size:
                    df = pd.DataFrame(chunk)
                    df.to_csv(target_path, index=False, sep=delimiter, encoding=encoding, mode=mode, header=header)
                    chunk = []
                    first_chunk = False
                    mode = 'a'
                    header = False
            
            if chunk:
                df = pd.DataFrame(chunk)
                df.to_csv(target_path, index=False, sep=delimiter, encoding=encoding, mode=mode, header=header)
            elif first_chunk:
                # 如果没有数据，创建一个空文件
                Path(target_path).touch()
                
        except Exception as e:
            raise ConvertError(f"写入CSV文件失败: {str(e)}")
    
    def _write_json(self, data: Iterable[Dict], target_path: Path, params: TaskParams) -> Dict[str, Any]:
        """写入JSON文件 - 流式写入以支持大文件"""
        encoding = params.get("output_encoding", "utf-8")
        ensure_ascii = params.get("json_ensure_ascii", False)
        
        stats = {'row_count': 0, 'fields': []}

        try:
            with open(target_path, "w", encoding=encoding, newline="\n") as f:
                f.write('[\n')
                first = True
                for item in data:
                    if first:
                        stats['fields'] = list(item.keys())
                        first = False
                    else:
                        f.write(',\n')
                    
                    # 格式化JSON字符串并缩进
                    json_str = json.dumps(item, indent=2, ensure_ascii=ensure_ascii, default=str)
                    # 为每一行添加缩进
                    indented_json_str = '\n'.join('  ' + line for line in json_str.split('\n'))
                    f.write(indented_json_str)
                    stats['row_count'] += 1
                
                f.write('\n]\n')
            return stats
        except Exception as e:
            raise ConvertError(f"写入JSON文件失败: {str(e)}")

    def _write_jsonl(self, data: Iterable[Dict], target_path: Path, params: TaskParams) -> Dict[str, Any]:
        """写入JSONL文件 - 流式写入"""
        encoding = params.get("output_encoding", "utf-8")
        stats = {'row_count': 0, 'fields': []}

        try:
            with open(target_path, "w", encoding=encoding, newline="\n") as f:
                for i, item in enumerate(data):
                    if i == 0:
                        stats['fields'] = list(item.keys())
                    
                    json_str = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
                    f.write(json_str + "\n")
                    stats['row_count'] += 1
            return stats
        except Exception as e:
            raise ConvertError(f"写入JSONL文件失败: {str(e)}")
    
    def _write_excel(self, data: List[Dict], target_path: Path, params: TaskParams):
        """写入Excel文件"""
        if not EXCEL_WRITE_AVAILABLE:
            raise ConvertError("Excel写入功能不可用，请安装openpyxl: pip install openpyxl")
        
        if not data:
            raise ConvertError("没有数据可写入")
        
        # 检查行数限制
        if len(data) > self.excel_limits['max_rows']:
            if params.get('split_file', False):
                self._write_excel_split(data, target_path, params)
                return
            else:
                raise ConvertError(f"数据行数({len(data)})超过Excel限制({self.excel_limits['max_rows']})，建议启用split_file或转换为CSV格式")
        
        sheet_name = params.get('excel_sheet', 'Sheet1')
        
        try:
            df = pd.DataFrame(data)
            df.to_excel(target_path, index=False, sheet_name=sheet_name)
        except Exception as e:
            raise ConvertError(f"写入Excel文件失败: {str(e)}")
    
    def _write_excel_split(self, data: List[Dict], target_path: Path, params: TaskParams):
        """分割写入Excel文件"""
        max_rows = self.excel_limits['max_rows']
        sheet_name = params.get('excel_sheet', 'Sheet1')
        
        # 计算需要的文件数
        file_count = (len(data) + max_rows - 1) // max_rows
        
        for i in range(file_count):
            start_idx = i * max_rows
            end_idx = min((i + 1) * max_rows, len(data))
            chunk_data = data[start_idx:end_idx]
            
            # 生成分割文件名
            base_name = target_path.stem
            split_path = target_path.parent / f"{base_name}_part{i+1}.xlsx"
            
            df = pd.DataFrame(chunk_data)
            df.to_excel(split_path, index=False, sheet_name=sheet_name)
    
    def _write_markdown(self, data: List[Dict], target_path: Path, params: TaskParams):
        """写入Markdown表格文件"""
        if not data:
            raise ConvertError("没有数据可写入")
        
        encoding = params.get('output_encoding', 'utf-8')
        include_header = params.get('markdown_table_header', True)
        
        try:
            headers = list(data[0].keys())
            
            with open(target_path, 'w', encoding=encoding) as f:
                if include_header:
                    # 写入表头
                    f.write('| ' + ' | '.join(headers) + ' |\n')
                    f.write('| ' + ' | '.join(['---'] * len(headers)) + ' |\n')
                
                # 写入数据行
                for row in data:
                    values = [str(row.get(header, '')) for header in headers]
                    f.write('| ' + ' | '.join(values) + ' |\n')
        except Exception as e:
            raise ConvertError(f"写入Markdown文件失败: {str(e)}")
    
    def _write_arrow(self, data: List[Dict], target_path: Path, params: TaskParams):
        """写入Arrow格式数据集（Hugging Face Datasets格式）"""
        if not ARROW_AVAILABLE:
            raise ConvertError("Arrow格式支持不可用，请安装: pip install datasets pyarrow")
        
        if not data:
            raise ConvertError("没有数据可写入")
        
        try:
            # 创建目标目录
            dataset_dir = target_path.parent / target_path.stem
            dataset_dir.mkdir(parents=True, exist_ok=True)
            
            # 转换为pandas DataFrame
            df = pd.DataFrame(data)
            
            # 创建Hugging Face Dataset
            dataset = datasets.Dataset.from_pandas(df)
            
            # 保存为Arrow格式
            dataset.save_to_disk(str(dataset_dir))
            
            # 更新target_path指向保存的目录
            target_path = dataset_dir
            
            self.logger.info(f"Arrow数据集已保存到: {dataset_dir}")
            
            return str(target_path)
            
        except Exception as e:
            raise ConvertError(f"写入Arrow文件失败: {str(e)}")
    
    def _generate_metadata(self, task_id: str, params: TaskParams, source_format: str, target_path: str, data: List[Dict]) -> ConvertMeta:
        """生成转换元数据"""
        source_hash = self._calculate_file_hash(params['source_path'])
        
        return ConvertMeta(
            task_id=task_id,
            source_path=params['source_path'],
            source_format=source_format,
            target_path=target_path,
            target_format=params['target_format'],
            start_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            end_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            status='success',
            params=params,
            data_quality={
                'row_count': {
                    'source': len(data),
                    'target': len(data),
                    'skipped': 0,
                    'skipped_reason': ''
                },
                'field_mapping': {
                    'original': list(data[0].keys()) if data else [],
                    'target': list(data[0].keys()) if data else [],
                    'duplicates': {}
                },
                'data_type': {
                    'date_fields': params.get('date_fields', []),
                    'text_fields': params.get('text_fields', []),
                    'nested_fields': []
                }
            },
            source_hash=source_hash,
            error_log_path=None
        )
    
    def _calculate_file_hash(self, file_path: str, algorithm: str = 'md5') -> str:
        """计算文件哈希值"""
        hash_func = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_func.update(chunk)
            return f"{algorithm}:{hash_func.hexdigest()}"
        except Exception:
            return f"{algorithm}:unknown"
    
    def get_task_progress(self, task_id: str) -> Dict[str, Any]:
        """获取任务进度"""
        if task_id not in self.tasks:
            return {'error': '任务不存在'}
        
        return self.tasks[task_id]['tracker'].get_info()
    
    def list_tasks(self) -> List[Dict[str, Any]]:
        """列出所有任务"""
        tasks_info = []
        for task_id, task in self.tasks.items():
            info = task['tracker'].get_info()
            info.update({
                'source_path': task['params']['source_path'],
                'target_format': task['params']['target_format']
            })
            tasks_info.append(info)
        return tasks_info


# 全局转换器实例
_converter_instance = None

def get_converter() -> FormatConverter:
    """
    获取全局转换器实例
    
    使用单例模式，确保整个应用中只有一个转换器实例。
    
    Returns:
        FormatConverter: 转换器实例
    """
    global _converter_instance
    if _converter_instance is None:
        _converter_instance = FormatConverter()
    return _converter_instance


# 公共API接口
def convert_format(source_path: str, target_format: str, **kwargs) -> str:
    """
    简化的格式转换接口
    
    Args:
        source_path: 源文件路径
        target_format: 目标格式
        **kwargs: 其他转换参数
    
    Returns:
        str: 任务ID
    """
    converter = get_converter()
    return converter.add_convert_task(
        source_path=source_path,
        target_format=target_format,
        **kwargs
    )


def start_convert(task_id: str) -> bool:
    """启动转换任务"""
    converter = get_converter()
    return converter.start_task(task_id)


def get_convert_progress(task_id: str) -> Dict[str, Any]:
    """获取转换进度"""
    converter = get_converter()
    return converter.get_task_progress(task_id)


def list_converts() -> List[Dict[str, Any]]:
    """列出所有转换任务"""
    converter = get_converter()
    return converter.list_tasks()


# 命令行界面
def main():
    """
    命令行工具入口点
    
    提供完整的命令行界面，支持格式转换任务的创建和管理。
    """
    parser = argparse.ArgumentParser(
        description='格式转换工具 v1.1',
        epilog="""
使用示例:
  # CSV转JSON
  python format_converter.py --source data.csv --target json
  
  # Excel转JSONL（指定工作表）
  python format_converter.py --source data.xlsx --target jsonl --excel-sheet Sheet2
  
  # JSONL转CSV（指定文本字段）
  python format_converter.py --source data.jsonl --target csv --text-fields phone,id_card
  
  # 查看任务进度
  python format_converter.py --progress conv-20241002-abc123
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 基本转换参数
    parser.add_argument('--source', help='源文件路径')
    parser.add_argument('--target', choices=['csv', 'json', 'jsonl', 'excel', 'markdown', 'arrow'],
                       help='目标格式')
    
    # 转换选项
    parser.add_argument('--output-dir', help='输出目录')
    parser.add_argument('--encoding', help='源文件编码')
    parser.add_argument('--output-encoding', default='utf-8', help='输出文件编码')
    parser.add_argument('--excel-sheet', default='Sheet1', help='Excel工作表名')
    parser.add_argument('--csv-delimiter', default=',', help='CSV分隔符')
    parser.add_argument('--date-fields', help='日期字段列表（逗号分隔）')
    parser.add_argument('--text-fields', help='文本字段列表（逗号分隔）')
    parser.add_argument('--skip-empty-rows', action='store_true', help='跳过空行')
    parser.add_argument('--clean-invisible', action='store_true', help='清理不可见字符')
    parser.add_argument('--split-file', action='store_true', help='超限时拆分文件')
    
    # Arrow格式专用选项
    parser.add_argument('--arrow-split', default='train', help='Arrow数据集分割名称')
    parser.add_argument('--max-rows', type=int, help='最大行数限制')
    
    # 任务管理选项
    parser.add_argument('--list-tasks', action='store_true', help='列出所有转换任务')
    parser.add_argument('--progress', metavar='TASK_ID', help='查看指定任务的进度')
    
    args = parser.parse_args()
    
    converter = get_converter()
    
    try:
        if args.list_tasks:
            tasks = converter.list_tasks()
            if tasks:
                print(f"共 {len(tasks)} 个转换任务:")
                for task in tasks:
                    print(f"  {task['task_id']}: {task['source_path']} -> {task['target_format']} ({task['status']})")
            else:
                print("暂无转换任务")
        
        elif args.progress:
            progress = converter.get_task_progress(args.progress)
            if 'error' in progress:
                print(f"错误: {progress['error']}")
            else:
                print(f"任务 {progress['task_id']} 进度:")
                print(f"  状态: {progress['status']}")
                print(f"  进度: {progress['progress']}%")
                print(f"  已处理: {progress['processed_rows']}/{progress['total_rows']} 行")
                print(f"  速度: {progress['speed']}")
                if progress['error_msg']:
                    print(f"  错误: {progress['error_msg']}")
        
        elif args.source and args.target:
            # 创建转换任务
            kwargs = {
                'output_dir': args.output_dir,
                'encoding': args.encoding,
                'output_encoding': args.output_encoding,
                'excel_sheet': args.excel_sheet,
                'csv_delimiter': args.csv_delimiter,
                'skip_empty_rows': args.skip_empty_rows,
                'clean_invisible': args.clean_invisible,
                'split_file': args.split_file,
                'arrow_split': args.arrow_split,
                'max_rows': args.max_rows
            }
            
            if args.date_fields:
                kwargs['date_fields'] = [field.strip() for field in args.date_fields.split(',')]
            if args.text_fields:
                kwargs['text_fields'] = [field.strip() for field in args.text_fields.split(',')]
            
            task_id = converter.add_convert_task(
                source_path=args.source,
                target_format=args.target,
                **{k: v for k, v in kwargs.items() if v is not None}
            )
            
            print(f"创建转换任务: {task_id}")
            
            # 启动任务
            if converter.start_task(task_id):
                print("任务已启动")
                
                # 等待任务完成（简单实现）
                while True:
                    progress = converter.get_task_progress(task_id)
                    if progress['status'] in ['completed', 'failed']:
                        break
                    time.sleep(1)
                
                if progress['status'] == 'completed':
                    print("转换完成")
                else:
                    print(f"转换失败: {progress['error_msg']}")
            else:
                print("任务启动失败")
        
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        print("\n转换已中断")
    except Exception as e:
        print(f"错误: {str(e)}")


# 创建全局实例
format_converter = FormatConverter()

if __name__ == '__main__':
    main()
