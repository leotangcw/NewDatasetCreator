#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字段提取模块 - field_extractor.py

核心功能：
1. 自动识别数据集字段并推断类型
2. 按用户选择提取指定字段
3. 支持多维度条件过滤
4. 支持字段重命名和大文件分片处理

设计思想：
- 模块化设计，独立完成字段分析和提取任务
- 支持多种数据格式(CSV/Excel/JSON/JSONL/Markdown)
- 提供前端交互接口和命令行调用方式
- 断点续传和异常恢复机制
- 完整的元数据追踪和日志记录

作者: GitHub Copilot
创建时间: 2025-08-24
"""

import os
import sys
import json
import csv
from .dependencies import pd, jsonlines
import re
import logging
import argparse
from typing import Dict, List, Union, Optional, TypedDict, Literal, Any
from pathlib import Path
import time
import uuid
from datetime import datetime

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from .config_manager import ConfigManager
    from .log_manager import LogManager 
    from .state_manager import StateManager
    from .utils import (
        validate_file, get_file_encoding, infer_data_type, 
        handle_null, read_file_chunk, write_file_chunk
    )
except ImportError:
    try:
        # 尝试直接导入（用于命令行执行）
        import sys
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sys.path.insert(0, current_dir)
        
        from config_manager import ConfigManager
        from log_manager import LogManager 
        from state_manager import StateManager
        from utils import (
            validate_file, get_file_encoding, infer_data_type, 
            handle_null, read_file_chunk, write_file_chunk
        )
    except ImportError as e:
        print(f"警告: 无法导入依赖模块 {e}，将使用简化版本")
        ConfigManager = None
        LogManager = None
        StateManager = None

# 类型定义
class FieldInfo(TypedDict):
    """字段信息结构"""
    name: str                           # 字段名
    type: Literal["string", "number", "date", "boolean"]  # 数据类型
    sample_values: List[Any]            # 前3条非空示例值
    null_ratio: float                   # 空值比例(0~1)

class FilterCondition(TypedDict):
    """过滤条件结构"""
    field: str                          # 目标字段名
    op: Literal[                       # 运算符
        ">", "<", ">=", "<=", "==", "!=",           # 数值/日期比较
        "contains", "not_contains", "regex",         # 字符串匹配
        "len_gt", "len_lt", "len_eq",               # 字符串长度
        "is_null", "not_null"                       # 空值判断
    ]
    value: Union[str, int, float, None] # 比较值

class TaskParams(TypedDict):
    """任务参数字典"""
    # 基础标识参数
    task_id: str                        # 任务唯一标识
    source_path: str                    # 源文件路径
    
    # 字段配置参数
    selected_fields: List[str]          # 用户选中的字段列表
    field_rename: Dict[str, str]        # 字段重命名映射(可选)
    
    # 过滤配置参数
    filter_conditions: List[FilterCondition]  # 过滤条件列表(可选)
    filter_logic: Literal["and", "or"]  # 多条件组合逻辑
    
    # 输出配置参数
    output_dir: str                     # 输出目录
    target_format: str                  # 输出格式
    encoding: str                       # 输出文件编码
    
    # 系统控制参数
    chunk_size: int                     # 分片大小
    resume: bool                        # 是否启用断点续传

class FieldExtractor:
    """字段提取器核心类
    
    提供字段识别、提取和过滤功能的主要实现类。
    支持多种数据格式的处理和用户自定义的提取规则。
    """
    
    def __init__(self):
        """初始化字段提取器
        
        设置默认配置、初始化日志器和状态管理器
        加载系统配置并准备工作环境
        """
        self.config_manager = None
        self.log_manager = None
        self.state_manager = None
        self.logger = None
        
        # 默认配置
        self.default_config = {
            'sample_rows': 100,
            'chunk_size': 1000,
            'supported_formats': ['csv', 'xlsx', 'xls', 'json', 'jsonl', 'md'],
            'default_encoding': 'utf-8',
            'output_dir': './data/processed'
        }
        
        # 支持的文件格式
        self.supported_formats = ['csv', 'xlsx', 'xls', 'json', 'jsonl', 'md']
        
    def init_extractor(self) -> bool:
        """初始化模块环境
        
        加载配置、日志与状态管理器，检查依赖库
        
        Returns:
            bool: True表示初始化成功，False表示失败
            
        Raises:
            ImportError: 当必需的依赖库缺失时
            Exception: 当配置文件损坏或其他初始化错误时
        """
        try:
            # 初始化配置管理器
            if ConfigManager:
                self.config_manager = ConfigManager()
                config = self.config_manager.get_config('field_extractor', self.default_config)
                self.sample_rows = config.get('sample_rows', 100)
                self.chunk_size = config.get('chunk_size', 1000)
                self.output_dir = config.get('output_dir', './data/processed')
            else:
                self.sample_rows = 100
                self.chunk_size = 1000
                self.output_dir = './data/processed'
            
            # 初始化日志管理器
            if LogManager:
                self.log_manager = LogManager()
                self.logger = self.log_manager.get_logger('field_extractor')
                self.logger.info("字段提取器初始化开始")
            else:
                # 创建简单的日志器
                self.logger = logging.getLogger('field_extractor')
                self.logger.setLevel(logging.INFO)
                if not self.logger.handlers:
                    handler = logging.StreamHandler()
                    formatter = logging.Formatter(
                        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    )
                    handler.setFormatter(formatter)
                    self.logger.addHandler(handler)
            
            # 初始化状态管理器
            if StateManager:
                self.state_manager = StateManager()
                self.state_manager.init_state('field_extractor')
            
            # 检查必需的依赖库
            missing_deps = []
            if pd is None:
                missing_deps.append('pandas')
            
            if jsonlines is None:
                missing_deps.append('jsonlines')
                
            try:
                import openpyxl
            except ImportError:
                missing_deps.append('openpyxl')
            
            if missing_deps:
                error_msg = f"缺少必需的依赖库: {', '.join(missing_deps)}"
                if self.logger:
                    self.logger.error(error_msg)
                return False
            
            if self.logger:
                self.logger.info("字段提取器初始化成功")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"字段提取器初始化失败: {str(e)}")
            return False
    
    def get_fields(self, source_path: str) -> Optional[List[FieldInfo]]:
        """解析源文件，返回字段信息列表
        
        自动识别文件格式，提取字段名称、推断数据类型、
        统计空值比例并提供示例值，供前端展示使用
        
        Args:
            source_path (str): 源文件路径，需确保文件存在且格式合法
            
        Returns:
            Optional[List[FieldInfo]]: 字段信息列表，失败时返回None
            包含字段名、类型、示例值和空值比例信息
            
        Raises:
            FileNotFoundError: 当源文件不存在时
            ValueError: 当文件格式不支持时
            Exception: 当文件解析失败时
        """
        try:
            if self.logger:
                self.logger.info(f"开始解析文件字段: {source_path}")
            
            # 文件校验
            if not self._validate_file(source_path):
                if self.logger:
                    self.logger.error(f"文件校验失败: {source_path}")
                return None
            
            # 格式识别
            file_format = self._detect_format(source_path)
            if not file_format:
                if self.logger:
                    self.logger.error(f"不支持的文件格式: {source_path}")
                return None
            
            if self.logger:
                self.logger.info(f"识别文件格式: {file_format}")
            
            # 根据格式解析字段
            if file_format in ['json', 'jsonl']:
                fields = self._parse_json_fields(source_path, file_format)
            elif file_format in ['csv']:
                fields = self._parse_csv_fields(source_path)
            elif file_format in ['xlsx', 'xls']:
                fields = self._parse_excel_fields(source_path)
            elif file_format == 'md':
                fields = self._parse_markdown_fields(source_path)
            else:
                if self.logger:
                    self.logger.error(f"未实现的格式解析: {file_format}")
                return None
            
            if self.logger:
                self.logger.info(f"成功解析到 {len(fields)} 个字段")
                
            return fields
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"字段解析失败: {str(e)}")
            return None
    
    def extract_fields(self, task_params: TaskParams) -> Optional[str]:
        """根据用户配置提取字段并生成目标文件
        
        按照指定的字段列表和过滤条件，从源文件中提取数据
        支持字段重命名、分片处理和断点续传机制
        
        Args:
            task_params (TaskParams): 任务参数字典，包含所有提取配置
            
        Returns:
            Optional[str]: 成功时返回输出文件路径，失败时返回None
            
        Raises:
            ValueError: 当参数校验失败时
            FileNotFoundError: 当源文件不存在时
            Exception: 当提取过程中发生错误时
        """
        try:
            task_id = task_params['task_id']
            if self.logger:
                self.logger.info(f"开始字段提取任务: {task_id}")
            
            # 参数校验
            if not self._validate_task_params(task_params):
                return None
            
            # 创建输出目录
            output_dir = os.path.join(task_params['output_dir'], task_id)
            os.makedirs(output_dir, exist_ok=True)
            
            # 字段合法性检查
            source_fields = self.get_fields(task_params['source_path'])
            if not source_fields:
                if self.logger:
                    self.logger.error("无法获取源文件字段信息")
                return None
            
            source_field_names = [f['name'] for f in source_fields]
            for field in task_params['selected_fields']:
                if field not in source_field_names:
                    if self.logger:
                        self.logger.error(f"字段不存在: {field}")
                    return None
            
            # 检查断点续传状态
            start_chunk = 0
            if task_params.get('resume', True) and self.state_manager:
                state = self.state_manager.get_state(f"task.{task_id}")
                if state and state.get('status') == 'running':
                    start_chunk = state.get('current_chunk', 0)
                    if self.logger:
                        self.logger.info(f"从第 {start_chunk} 个分片恢复任务")
            
            # 分片提取处理
            output_file = self._process_extraction(task_params, output_dir, start_chunk)
            
            if output_file:
                # 生成元数据
                self._generate_metadata(task_params, output_file, output_dir)
                
                if self.logger:
                    self.logger.info(f"字段提取任务完成: {output_file}")
                
                # 更新任务状态
                if self.state_manager:
                    self.state_manager.set_state(f"task.{task_id}", {
                        'status': 'completed',
                        'output_file': output_file,
                        'end_time': datetime.now().isoformat()
                    })
                
                return output_file
            else:
                if self.logger:
                    self.logger.error("字段提取任务失败")
                return None
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"字段提取任务异常: {str(e)}")
            return None
    
    def _validate_file(self, file_path: str) -> bool:
        """验证文件是否存在且格式支持
        
        Args:
            file_path (str): 文件路径
            
        Returns:
            bool: 验证结果
        """
        if not os.path.exists(file_path):
            return False
        
        file_ext = Path(file_path).suffix.lower()
        supported_exts = ['.csv', '.xlsx', '.xls', '.json', '.jsonl', '.md']
        
        return file_ext in supported_exts
    
    def _detect_format(self, file_path: str) -> Optional[str]:
        """检测文件格式
        
        Args:
            file_path (str): 文件路径
            
        Returns:
            Optional[str]: 文件格式字符串
        """
        file_ext = Path(file_path).suffix.lower()
        
        format_map = {
            '.csv': 'csv',
            '.xlsx': 'xlsx',
            '.xls': 'xls',
            '.json': 'json',
            '.jsonl': 'jsonl',
            '.md': 'md'
        }
        
        return format_map.get(file_ext)
    
    def _parse_json_fields(self, file_path: str, format_type: str) -> List[FieldInfo]:
        """解析JSON/JSONL文件字段
        
        Args:
            file_path (str): 文件路径
            format_type (str): 格式类型 ('json' 或 'jsonl')
            
        Returns:
            List[FieldInfo]: 字段信息列表
        """
        fields_data = {}
        sample_count = 0
        
        try:
            if format_type == 'jsonl':
                with jsonlines.open(file_path, 'r') as reader:
                    for item in reader:
                        if sample_count >= self.sample_rows:
                            break
                        
                        if isinstance(item, dict):
                            for key, value in item.items():
                                if key not in fields_data:
                                    fields_data[key] = {
                                        'values': [],
                                        'null_count': 0,
                                        'total_count': 0
                                    }
                                
                                fields_data[key]['total_count'] += 1
                                if value is None or value == '':
                                    fields_data[key]['null_count'] += 1
                                else:
                                    if len(fields_data[key]['values']) < 3:
                                        fields_data[key]['values'].append(value)
                        
                        sample_count += 1
            else:  # json
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                if isinstance(data, list):
                    for item in data[:self.sample_rows]:
                        if isinstance(item, dict):
                            sample_count += 1
                            for key, value in item.items():
                                if key not in fields_data:
                                    fields_data[key] = {
                                        'values': [],
                                        'null_count': 0,
                                        'total_count': 0
                                    }
                                
                                fields_data[key]['total_count'] += 1
                                if value is None or value == '':
                                    fields_data[key]['null_count'] += 1
                                else:
                                    if len(fields_data[key]['values']) < 3:
                                        fields_data[key]['values'].append(value)
                elif isinstance(data, dict):
                    sample_count = 1
                    for key, value in data.items():
                        fields_data[key] = {
                            'values': [value] if value is not None and value != '' else [],
                            'null_count': 1 if value is None or value == '' else 0,
                            'total_count': 1
                        }
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"JSON文件解析失败: {str(e)}")
            return []
        
        # 转换为FieldInfo格式
        result = []
        for field_name, field_data in fields_data.items():
            field_type = self._infer_field_type(field_data['values'])
            null_ratio = field_data['null_count'] / max(field_data['total_count'], 1)
            
            result.append(FieldInfo(
                name=field_name,
                type=field_type,
                sample_values=field_data['values'][:3],
                null_ratio=null_ratio
            ))
        
        return result
    
    def _parse_csv_fields(self, file_path: str) -> List[FieldInfo]:
        """解析CSV文件字段
        
        Args:
            file_path (str): 文件路径
            
        Returns:
            List[FieldInfo]: 字段信息列表
        """
        try:
            # 检测编码
            encoding = self._detect_encoding(file_path)
            
            # 读取CSV文件
            df = pd.read_csv(file_path, encoding=encoding, nrows=self.sample_rows)
            
            result = []
            for column in df.columns:
                # 获取非空值样本
                non_null_values = df[column].dropna().tolist()
                sample_values = non_null_values[:3]
                
                # 计算空值比例
                null_ratio = df[column].isnull().sum() / len(df)
                
                # 推断字段类型
                field_type = self._infer_field_type(sample_values)
                
                result.append(FieldInfo(
                    name=str(column),
                    type=field_type,
                    sample_values=sample_values,
                    null_ratio=float(null_ratio)
                ))
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"CSV文件解析失败: {str(e)}")
            return []
    
    def _parse_excel_fields(self, file_path: str) -> List[FieldInfo]:
        """解析Excel文件字段
        
        Args:
            file_path (str): 文件路径
            
        Returns:
            List[FieldInfo]: 字段信息列表
        """
        try:
            # 读取Excel文件
            df = pd.read_excel(file_path, nrows=self.sample_rows)
            
            result = []
            for column in df.columns:
                # 获取非空值样本
                non_null_values = df[column].dropna().tolist()
                sample_values = non_null_values[:3]
                
                # 计算空值比例
                null_ratio = df[column].isnull().sum() / len(df)
                
                # 推断字段类型
                field_type = self._infer_field_type(sample_values)
                
                result.append(FieldInfo(
                    name=str(column),
                    type=field_type,
                    sample_values=sample_values,
                    null_ratio=float(null_ratio)
                ))
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Excel文件解析失败: {str(e)}")
            return []
    
    def _parse_markdown_fields(self, file_path: str) -> List[FieldInfo]:
        """解析Markdown表格字段
        
        Args:
            file_path (str): 文件路径
            
        Returns:
            List[FieldInfo]: 字段信息列表
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 查找表格
            table_pattern = r'\|(.+)\|\s*\n\|[-\s|:]+\|\s*\n((?:\|.+\|\s*\n)*)'
            matches = re.findall(table_pattern, content)
            
            if not matches:
                return []
            
            # 取第一个表格
            header_line, data_lines = matches[0]
            
            # 解析表头
            headers = [h.strip() for h in header_line.split('|') if h.strip()]
            
            # 解析数据行
            rows = []
            for line in data_lines.strip().split('\n'):
                if line.strip():
                    row = [cell.strip() for cell in line.split('|') if cell.strip()]
                    if len(row) == len(headers):
                        rows.append(row)
            
            # 限制采样行数
            rows = rows[:self.sample_rows]
            
            result = []
            for i, header in enumerate(headers):
                # 获取该列的值
                column_values = []
                null_count = 0
                
                for row in rows:
                    if i < len(row):
                        value = row[i]
                        if value == '' or value.lower() in ['null', 'none', 'na']:
                            null_count += 1
                        else:
                            column_values.append(value)
                
                # 计算空值比例
                null_ratio = null_count / max(len(rows), 1)
                
                # 推断字段类型
                field_type = self._infer_field_type(column_values[:3])
                
                result.append(FieldInfo(
                    name=header,
                    type=field_type,
                    sample_values=column_values[:3],
                    null_ratio=null_ratio
                ))
            
            return result
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Markdown文件解析失败: {str(e)}")
            return []
    
    def _infer_field_type(self, values: List[Any]) -> str:
        """推断字段数据类型
        
        基于采样值的特征判断字段类型
        
        Args:
            values (List[Any]): 字段值列表
            
        Returns:
            str: 数据类型 ('string', 'number', 'date', 'boolean')
        """
        if not values:
            return "string"
        
        # 检查是否为数值类型
        numeric_count = 0
        for value in values:
            try:
                float(value)
                numeric_count += 1
            except (ValueError, TypeError):
                pass
        
        if numeric_count == len(values):
            return "number"
        
        # 检查是否为布尔类型
        boolean_values = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n'}
        boolean_count = 0
        for value in values:
            if str(value).lower() in boolean_values:
                boolean_count += 1
        
        if boolean_count == len(values):
            return "boolean"
        
        # 检查是否为日期类型
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
        ]
        
        date_count = 0
        for value in values:
            str_value = str(value)
            for pattern in date_patterns:
                if re.match(pattern, str_value):
                    date_count += 1
                    break
        
        if date_count == len(values):
            return "date"
        
        # 默认为字符串类型
        return "string"
    
    def _detect_encoding(self, file_path: str) -> str:
        """检测文件编码
        
        Args:
            file_path (str): 文件路径
            
        Returns:
            str: 编码名称
        """
        try:
            import chardet
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # 读取前10KB用于检测
                result = chardet.detect(raw_data)
                return result['encoding'] or 'utf-8'
        except ImportError:
            return 'utf-8'
        except Exception:
            return 'utf-8'
    
    def _validate_task_params(self, task_params: TaskParams) -> bool:
        """验证任务参数
        
        Args:
            task_params (TaskParams): 任务参数
            
        Returns:
            bool: 验证结果
        """
        required_fields = ['task_id', 'source_path', 'selected_fields']
        
        for field in required_fields:
            if field not in task_params or not task_params[field]:
                if self.logger:
                    self.logger.error(f"缺少必需参数: {field}")
                return False
        
        # 验证源文件存在
        if not os.path.exists(task_params['source_path']):
            if self.logger:
                self.logger.error(f"源文件不存在: {task_params['source_path']}")
            return False
        
        # 验证过滤条件
        if 'filter_conditions' in task_params:
            for condition in task_params['filter_conditions']:
                if not self._validate_filter_condition(condition):
                    return False
        
        return True
    
    def _validate_filter_condition(self, condition: FilterCondition) -> bool:
        """验证过滤条件
        
        Args:
            condition (FilterCondition): 过滤条件
            
        Returns:
            bool: 验证结果
        """
        required_ops = [
            ">", "<", ">=", "<=", "==", "!=",
            "contains", "not_contains", "regex",
            "len_gt", "len_lt", "len_eq",
            "is_null", "not_null"
        ]
        
        if condition.get('op') not in required_ops:
            if self.logger:
                self.logger.error(f"不支持的操作符: {condition.get('op')}")
            return False
        
        # 空值判断不需要value参数
        if condition.get('op') in ['is_null', 'not_null']:
            return True
        
        if 'value' not in condition:
            if self.logger:
                self.logger.error(f"过滤条件缺少value参数: {condition}")
            return False
        
        return True
    
    def _process_extraction(self, task_params: TaskParams, output_dir: str, start_chunk: int = 0) -> Optional[str]:
        """处理字段提取的核心逻辑
        
        Args:
            task_params (TaskParams): 任务参数
            output_dir (str): 输出目录
            start_chunk (int): 起始分片索引
            
        Returns:
            Optional[str]: 输出文件路径
        """
        try:
            source_path = task_params['source_path']
            task_id = task_params['task_id']
            chunk_size = task_params.get('chunk_size', self.chunk_size)
            
            # 内存保护：检查可用内存
            try:
                import psutil
                available_memory_mb = psutil.virtual_memory().available / 1024 / 1024
                # 如果可用内存小于500MB，减小chunk_size
                if available_memory_mb < 500:
                    chunk_size = min(chunk_size, 100)  # 限制为100行
                    self.logger.warning(f"可用内存不足({available_memory_mb:.0f}MB)，减小chunk_size到{chunk_size}")
            except ImportError:
                # psutil不可用时跳过内存检查
                pass
            
            # 确定输出文件路径
            source_format = self._detect_format(source_path)
            target_format = task_params.get('target_format', source_format)
            
            output_file = os.path.join(output_dir, f"extracted.{target_format}")
            
            # 获取文件总行数（估算）
            total_rows = self._estimate_total_rows(source_path, source_format)
            total_chunks = (total_rows + chunk_size - 1) // chunk_size
            
            processed_rows = 0
            filtered_rows = 0
            
            # 更新任务状态
            if self.state_manager:
                self.state_manager.set_state(f"task.{task_id}", {
                    'status': 'running',
                    'total_chunks': total_chunks,
                    'current_chunk': start_chunk,
                    'start_time': datetime.now().isoformat()
                })
            
            # 分片处理
            for chunk_idx in range(start_chunk, total_chunks):
                if self.logger:
                    self.logger.info(f"处理分片 {chunk_idx + 1}/{total_chunks}")
                
                # 读取分片数据
                chunk_data = self._read_chunk_data(source_path, source_format, chunk_idx, chunk_size)
                
                if not chunk_data:
                    continue
                
                # 字段提取
                extracted_data = self._extract_fields_from_chunk(chunk_data, task_params)
                
                # 条件过滤
                if task_params.get('filter_conditions'):
                    filtered_data = self._apply_filters(extracted_data, task_params)
                else:
                    filtered_data = extracted_data
                
                # 写入数据
                if filtered_data:
                    self._write_chunk_data(filtered_data, output_file, target_format, chunk_idx == start_chunk)
                    filtered_rows += len(filtered_data)
                
                processed_rows += len(chunk_data)
                
                # 更新进度
                if self.state_manager:
                    progress = int((chunk_idx + 1) / total_chunks * 100)
                    self.state_manager.set_state(f"task.{task_id}", {
                        'current_chunk': chunk_idx + 1,
                        'progress': progress,
                        'processed_rows': processed_rows,
                        'filtered_rows': filtered_rows
                    })
            
            if self.logger:
                self.logger.info(f"提取完成，处理 {processed_rows} 行，输出 {filtered_rows} 行")
            
            return output_file
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"数据提取处理失败: {str(e)}")
            return None
    
    def _estimate_total_rows(self, file_path: str, file_format: str) -> int:
        """估算文件总行数
        
        Args:
            file_path (str): 文件路径
            file_format (str): 文件格式
            
        Returns:
            int: 估算的总行数
        """
        try:
            if file_format == 'csv':
                with open(file_path, 'r', encoding=self._detect_encoding(file_path)) as f:
                    return sum(1 for _ in f) - 1  # 减去表头行
            elif file_format == 'jsonl':
                with open(file_path, 'r', encoding='utf-8') as f:
                    return sum(1 for _ in f)
            elif file_format in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, nrows=1)
                # 这里只是简单估算，实际实现中可能需要更精确的方法
                return 1000  # 默认估算值
            else:
                return 1000  # 默认估算值
        except Exception:
            return 1000
    
    def _read_chunk_data(self, file_path: str, file_format: str, chunk_idx: int, chunk_size: int) -> List[Dict]:
        """读取分片数据
        
        Args:
            file_path (str): 文件路径
            file_format (str): 文件格式
            chunk_idx (int): 分片索引
            chunk_size (int): 分片大小
            
        Returns:
            List[Dict]: 分片数据列表
        """
        try:
            skip_rows = chunk_idx * chunk_size
            
            if file_format == 'csv':
                encoding = self._detect_encoding(file_path)
                df = pd.read_csv(file_path, encoding=encoding, skiprows=skip_rows, nrows=chunk_size)
                return df.to_dict('records')
                
            elif file_format == 'jsonl':
                data = []
                with jsonlines.open(file_path, 'r') as reader:
                    for i, item in enumerate(reader):
                        if i < skip_rows:
                            continue
                        if len(data) >= chunk_size:
                            break
                        data.append(item)
                return data
                
            elif file_format in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, skiprows=skip_rows, nrows=chunk_size)
                return df.to_dict('records')
                
            elif file_format == 'json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    start_idx = skip_rows
                    end_idx = start_idx + chunk_size
                    return data[start_idx:end_idx]
                else:
                    return [data] if chunk_idx == 0 else []
            
            return []
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"读取分片数据失败: {str(e)}")
            return []
    
    def _extract_fields_from_chunk(self, chunk_data: List[Dict], task_params: TaskParams) -> List[Dict]:
        """从分片数据中提取指定字段
        
        Args:
            chunk_data (List[Dict]): 分片数据
            task_params (TaskParams): 任务参数
            
        Returns:
            List[Dict]: 提取后的数据
        """
        selected_fields = task_params['selected_fields']
        field_rename = task_params.get('field_rename', {})
        
        result = []
        for row in chunk_data:
            extracted_row = {}
            for field in selected_fields:
                if field in row:
                    # 应用字段重命名
                    output_field = field_rename.get(field, field)
                    extracted_row[output_field] = row[field]
            
            if extracted_row:  # 只保留非空行
                result.append(extracted_row)
        
        return result
    
    def _apply_filters(self, data: List[Dict], task_params: TaskParams) -> List[Dict]:
        """应用过滤条件
        
        Args:
            data (List[Dict]): 待过滤的数据
            task_params (TaskParams): 任务参数
            
        Returns:
            List[Dict]: 过滤后的数据
        """
        filter_conditions = task_params.get('filter_conditions', [])
        filter_logic = task_params.get('filter_logic', 'and')
        
        if not filter_conditions:
            return data
        
        result = []
        for row in data:
            if self._check_row_filters(row, filter_conditions, filter_logic):
                result.append(row)
        
        return result
    
    def _check_row_filters(self, row: Dict, conditions: List[FilterCondition], logic: str) -> bool:
        """检查单行数据是否满足过滤条件
        
        Args:
            row (Dict): 数据行
            conditions (List[FilterCondition]): 过滤条件列表
            logic (str): 逻辑关系 ('and' 或 'or')
            
        Returns:
            bool: 是否满足条件
        """
        results = []
        
        for condition in conditions:
            field = condition['field']
            op = condition['op']
            value = condition['value']
            
            field_value = row.get(field)
            result = self._evaluate_condition(field_value, op, value)
            results.append(result)
        
        if logic == 'and':
            return all(results)
        else:  # or
            return any(results)
    
    def _evaluate_condition(self, field_value: Any, op: str, compare_value: Any) -> bool:
        """评估单个过滤条件
        
        Args:
            field_value (Any): 字段值
            op (str): 操作符
            compare_value (Any): 比较值
            
        Returns:
            bool: 条件评估结果
        """
        try:
            # 空值判断
            if op == 'is_null':
                return field_value is None or field_value == ''
            elif op == 'not_null':
                return field_value is not None and field_value != ''
            
            # 如果字段值为空，其他条件都返回False
            if field_value is None or field_value == '':
                return False
            
            # 字符串操作
            if op == 'contains':
                return str(compare_value) in str(field_value)
            elif op == 'not_contains':
                return str(compare_value) not in str(field_value)
            elif op == 'regex':
                return bool(re.search(str(compare_value), str(field_value)))
            
            # 字符串长度操作
            elif op == 'len_gt':
                return len(str(field_value)) > int(compare_value)
            elif op == 'len_lt':
                return len(str(field_value)) < int(compare_value)
            elif op == 'len_eq':
                return len(str(field_value)) == int(compare_value)
            
            # 数值比较
            elif op in ['>', '<', '>=', '<=', '==', '!=']:
                try:
                    field_num = float(field_value)
                    compare_num = float(compare_value)
                    
                    if op == '>':
                        return field_num > compare_num
                    elif op == '<':
                        return field_num < compare_num
                    elif op == '>=':
                        return field_num >= compare_num
                    elif op == '<=':
                        return field_num <= compare_num
                    elif op == '==':
                        return field_num == compare_num
                    elif op == '!=':
                        return field_num != compare_num
                except ValueError:
                    # 如果不能转换为数值，按字符串比较
                    if op == '==':
                        return str(field_value) == str(compare_value)
                    elif op == '!=':
                        return str(field_value) != str(compare_value)
            
            return False
            
        except Exception as e:
            if self.logger:
                self.logger.warning(f"条件评估失败: {str(e)}")
            return False
    
    def _write_chunk_data(self, data: List[Dict], output_file: str, target_format: str, is_first_chunk: bool):
        """写入分片数据到输出文件
        
        Args:
            data (List[Dict]): 待写入的数据
            output_file (str): 输出文件路径
            target_format (str): 目标格式
            is_first_chunk (bool): 是否为第一个分片
        """
        try:
            if target_format == 'csv':
                mode = 'w' if is_first_chunk else 'a'
                df = pd.DataFrame(data)
                df.to_csv(output_file, mode=mode, index=False, 
                         header=is_first_chunk, encoding='utf-8')
                
            elif target_format == 'jsonl':
                mode = 'w' if is_first_chunk else 'a'
                with jsonlines.open(output_file, mode=mode) as writer:
                    for item in data:
                        writer.write(item)
                        
            elif target_format == 'json':
                # JSON格式需要特殊处理，因为要保持数组结构
                if is_first_chunk:
                    all_data = data
                else:
                    # 读取现有数据并合并
                    with open(output_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                    all_data = existing_data + data
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(all_data, f, ensure_ascii=False, indent=2)
                    
            elif target_format in ['xlsx', 'xls']:
                if is_first_chunk:
                    df = pd.DataFrame(data)
                    df.to_excel(output_file, index=False)
                else:
                    # Excel追加比较复杂，这里简化处理
                    existing_df = pd.read_excel(output_file)
                    new_df = pd.DataFrame(data)
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    combined_df.to_excel(output_file, index=False)
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"写入数据失败: {str(e)}")
    
    def _generate_metadata(self, task_params: TaskParams, output_file: str, output_dir: str):
        """生成任务元数据
        
        Args:
            task_params (TaskParams): 任务参数
            output_file (str): 输出文件路径
            output_dir (str): 输出目录
        """
        try:
            # 统计输出文件信息
            file_stats = os.stat(output_file)
            
            metadata = {
                'task_id': task_params['task_id'],
                'module': 'field_extractor',
                'action': 'extract',
                'start_time': task_params.get('start_time', ''),
                'end_time': datetime.now().isoformat(),
                'status': 'completed',
                'source_info': {
                    'file_path': task_params['source_path'],
                    'format': self._detect_format(task_params['source_path'])
                },
                'extraction_config': {
                    'selected_fields': task_params['selected_fields'],
                    'field_rename': task_params.get('field_rename', {}),
                    'filter_conditions': task_params.get('filter_conditions', []),
                    'filter_logic': task_params.get('filter_logic', 'and'),
                    'chunk_size': task_params.get('chunk_size', self.chunk_size)
                },
                'output_info': {
                    'file_path': output_file,
                    'format': task_params.get('target_format'),
                    'file_size': file_stats.st_size,
                    'encoding': task_params.get('encoding', 'utf-8')
                },
                'statistics': {
                    'processed_rows': task_params.get('processed_rows', 0),
                    'filtered_rows': task_params.get('filtered_rows', 0)
                }
            }
            
            # 写入元数据文件
            meta_file = os.path.join(output_dir, 'meta.json')
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # 保存字段信息
            field_info_file = os.path.join(output_dir, 'field_info.json')
            source_fields = self.get_fields(task_params['source_path'])
            if source_fields:
                with open(field_info_file, 'w', encoding='utf-8') as f:
                    json.dump(source_fields, f, ensure_ascii=False, indent=2)
            
            # 保存过滤规则
            if task_params.get('filter_conditions'):
                filter_file = os.path.join(output_dir, 'filter_rules.json')
                filter_data = {
                    'conditions': task_params['filter_conditions'],
                    'logic': task_params.get('filter_logic', 'and')
                }
                with open(filter_file, 'w', encoding='utf-8') as f:
                    json.dump(filter_data, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"生成元数据失败: {str(e)}")

def main():
    """命令行主入口函数
    
    解析命令行参数并执行相应的字段提取操作
    支持字段查询和字段提取两种操作模式
    """
    parser = argparse.ArgumentParser(description='字段提取模块')
    parser.add_argument('--action', choices=['get_fields', 'extract'], 
                       required=True, help='操作类型')
    parser.add_argument('--source_path', required=True, help='源文件路径')
    parser.add_argument('--task_id', help='任务ID')
    parser.add_argument('--selected_fields', help='选中的字段(逗号分隔)')
    parser.add_argument('--field_rename', help='字段重命名(JSON格式)')
    parser.add_argument('--filter_conditions', help='过滤条件(JSON格式)')
    parser.add_argument('--filter_logic', choices=['and', 'or'], 
                       default='and', help='过滤逻辑')
    parser.add_argument('--target_format', help='目标格式')
    parser.add_argument('--output_dir', help='输出目录')
    parser.add_argument('--chunk_size', type=int, help='分片大小')
    parser.add_argument('--print_fields', type=bool, default=False, 
                       help='是否打印字段信息')
    
    args = parser.parse_args()
    
    # 初始化提取器
    extractor = FieldExtractor()
    if not extractor.init_extractor():
        print(json.dumps({
            'status': 'failed',
            'error': '字段提取器初始化失败'
        }, ensure_ascii=False))
        return
    
    try:
        if args.action == 'get_fields':
            # 获取字段信息
            fields = extractor.get_fields(args.source_path)
            
            if fields:
                if args.print_fields:
                    print(f"从 {args.source_path} 识别到 {len(fields)} 个字段:")
                    for i, field in enumerate(fields, 1):
                        print(f"{i}. {field['name']}({field['type']}，"
                              f"空值比例：{field['null_ratio']:.2f}，"
                              f"示例：{field['sample_values']})")
                
                print(json.dumps({
                    'status': 'success',
                    'data': fields
                }, ensure_ascii=False))
            else:
                print(json.dumps({
                    'status': 'failed',
                    'error': '字段识别失败'
                }, ensure_ascii=False))
                
        elif args.action == 'extract':
            # 准备任务参数
            task_params = TaskParams(
                task_id=args.task_id or f"extract-{int(time.time())}-{uuid.uuid4().hex[:6]}",
                source_path=args.source_path,
                selected_fields=args.selected_fields.split(',') if args.selected_fields else [],
                field_rename=json.loads(args.field_rename) if args.field_rename else {},
                filter_conditions=json.loads(args.filter_conditions) if args.filter_conditions else [],
                filter_logic=args.filter_logic,
                output_dir=args.output_dir or './data/processed',
                target_format=args.target_format or '',
                encoding='utf-8',
                chunk_size=args.chunk_size or 1000,
                resume=True
            )
            
            # 执行提取
            output_file = extractor.extract_fields(task_params)
            
            if output_file:
                print(json.dumps({
                    'status': 'success',
                    'task_id': task_params['task_id'],
                    'output': output_file
                }, ensure_ascii=False))
            else:
                print(json.dumps({
                    'status': 'failed',
                    'task_id': task_params['task_id'],
                    'error': '字段提取失败'
                }, ensure_ascii=False))
                
    except Exception as e:
        print(json.dumps({
            'status': 'failed',
            'error': str(e)
        }, ensure_ascii=False))


# 创建全局实例
field_extractor = FieldExtractor()
field_extractor.init_extractor()  # 初始化全局实例

# 全局API函数
def get_fields(source_path: str) -> Optional[List[FieldInfo]]:
    """获取文件字段信息的全局API"""
    return field_extractor.get_fields(source_path)

def extract_fields(source_path: str, fields: List[str], output_dir: str = None, **kwargs) -> str:
    """提取字段的全局API"""
    task_params = {
        'task_id': f"extract-{int(time.time())}-{uuid.uuid4().hex[:6]}",
        'source_path': source_path,
        'selected_fields': fields,
        'field_rename': kwargs.get('field_rename', {}),
        'filter_conditions': kwargs.get('filter_conditions', []),
        'filter_logic': kwargs.get('filter_logic', 'and'),
        'output_dir': output_dir or './data/processed',
        'target_format': kwargs.get('target_format', ''),
        'encoding': kwargs.get('encoding', 'utf-8'),
        'chunk_size': kwargs.get('chunk_size', 1000),
        'resume': True
    }
    return field_extractor.extract_fields(task_params)

if __name__ == '__main__':
    main()
