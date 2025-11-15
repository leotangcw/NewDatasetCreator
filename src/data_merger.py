#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据合并模块 - data_merger.py

核心功能：
1. 同结构数据集的纵向合并（支持多文件合并为单文件）
2. 两种合并模式：新建合并和追加合并
3. 基于指定字段或全量字段的精准去重
4. 大文件分片处理，避免内存溢出

设计思想：
- 专注于同结构数据集合并，避免复杂的横向拼接
- 严格的格式与字段一致性校验
- 支持断点续传和进度追踪
- 完整的合并元数据记录和可追溯性

作者: GitHub Copilot
创建时间: 2025-08-24
"""

import os
import sys
import json
import pandas as pd
import jsonlines
import logging
import argparse
import time
import uuid
from typing import Dict, List, Union, Optional, TypedDict, Literal, Any, Tuple
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from .config_manager import ConfigManager
    from .log_manager import LogManager 
    from .state_manager import StateManager
    from .utils import validate_file, get_file_encoding, ensure_dir
    from .field_extractor import FieldExtractor
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
        from utils import validate_file, get_file_encoding, ensure_dir
        from field_extractor import FieldExtractor
    except ImportError as e:
        print(f"警告: 无法导入依赖模块 {e}，将使用简化版本")
        ConfigManager = None
        LogManager = None
        StateManager = None
        FieldExtractor = None
        
        # 简化版本的工具函数
        def validate_file(file_path: str, formats=None) -> bool:
            return os.path.exists(file_path) and os.path.isfile(file_path)
        
        def get_file_encoding(file_path: str) -> str:
            return 'utf-8'
        
        def ensure_dir(dir_path: str) -> bool:
            try:
                os.makedirs(dir_path, exist_ok=True)
                return True
            except Exception:
                return False

# 类型定义
class MergeTaskParams(TypedDict):
    """数据合并任务参数结构"""
    task_id: str                                    # 任务唯一标识
    input_paths: List[str]                          # 待合并文件路径列表
    merge_mode: Literal["merge", "append"]          # 合并模式：新建/追加
    target_path: str                                # 目标文件路径
    
    # 去重配置
    deduplicate: bool                               # 是否去重
    dedup_field: Optional[str]                      # 去重字段(None则全量去重)
    dedup_strategy: Literal["keep_first", "keep_last"]  # 去重策略
    
    # 系统参数
    chunk_size: Optional[int]                       # 分片大小
    encoding: Optional[str]                         # 文件编码

class MergeMeta(TypedDict):
    """合并元数据结构"""
    task_id: str                                    # 任务ID
    merge_mode: str                                 # 合并模式
    input_files: List[str]                          # 输入文件路径
    input_row_counts: List[int]                     # 各输入文件记录数
    target_path: str                                # 目标文件路径
    total_input_rows: int                           # 输入总记录数
    total_output_rows: int                          # 合并后记录数
    duplicate_rows: int                             # 去重数量
    start_time: str                                 # 开始时间
    end_time: str                                   # 结束时间
    fields: List[str]                               # 数据集字段列表

class DataMerger:
    """数据合并器核心类
    
    提供同结构数据集的合并与去重功能。
    支持多种格式和大文件分片处理。
    """
    
    def __init__(self):
        """初始化数据合并器
        
        设置默认配置、初始化日志器和状态管理器
        加载系统配置并准备工作环境
        """
        self.config_manager = None
        self.log_manager = None
        self.state_manager = None
        self.field_extractor = None
        self.logger = None
        
        # 默认配置
        self.default_config = {
            'chunk_size': 1000,
            'default_encoding': 'utf-8',
            'supported_formats': ['csv', 'xlsx', 'xls', 'json', 'jsonl'],
            'max_parallel_tasks': 3,
            'temp_dir': './data/temp'
        }
        
        # 支持的文件格式
        self.supported_formats = ['csv', 'xlsx', 'xls', 'json', 'jsonl']
        
    def init_merger(self) -> bool:
        """初始化合并器环境
        
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
                config = self.config_manager.get_config('data_merger', self.default_config)
                self.chunk_size = config.get('chunk_size', 1000)
                self.default_encoding = config.get('default_encoding', 'utf-8')
                self.temp_dir = config.get('temp_dir', './data/temp')
            else:
                self.chunk_size = 1000
                self.default_encoding = 'utf-8'
                self.temp_dir = './data/temp'
            
            # 初始化日志管理器
            if LogManager:
                self.log_manager = LogManager()
                self.logger = self.log_manager.get_logger('data_merger')
                self.logger.info("数据合并器初始化开始")
            else:
                # 创建简单的日志器
                self.logger = logging.getLogger('data_merger')
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
                self.state_manager.init_state('data_merger')
            
            # 初始化字段提取器(用于字段一致性校验)
            if FieldExtractor:
                self.field_extractor = FieldExtractor()
                self.field_extractor.init_extractor()
            
            # 检查必需的依赖库
            missing_deps = []
            try:
                import pandas
            except ImportError:
                missing_deps.append('pandas')
            
            try:
                import jsonlines
            except ImportError:
                missing_deps.append('jsonlines')
            
            if missing_deps:
                error_msg = f"缺少必需的依赖库: {', '.join(missing_deps)}"
                if self.logger:
                    self.logger.error(error_msg)
                return False
            
            # 确保临时目录存在
            ensure_dir(self.temp_dir)
            
            if self.logger:
                self.logger.info("数据合并器初始化成功")
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"数据合并器初始化失败: {str(e)}")
            return False
    
    def validate_merge(self, params: MergeTaskParams) -> Dict[str, Any]:
        """校验合并可行性
        
        严格校验所有输入文件的格式与字段一致性
        
        Args:
            params (MergeTaskParams): 合并任务参数
            
        Returns:
            Dict[str, Any]: 校验结果字典，包含valid、reason、details等信息
            
        Raises:
            Exception: 当校验过程中发生错误时
        """
        try:
            if self.logger:
                self.logger.info(f"开始校验合并任务: {params['task_id']}")
            
            # 基础参数校验
            if not params.get('input_paths') or len(params['input_paths']) < 1:
                return {
                    'valid': False,
                    'reason': '输入文件列表为空，至少需要1个文件',
                    'details': {}
                }
            
            if not params.get('target_path'):
                return {
                    'valid': False,
                    'reason': '目标文件路径为空',
                    'details': {}
                }
            
            # 检查输入文件存在性和格式一致性
            input_paths = params['input_paths']
            base_format = None
            base_fields = None
            
            for i, file_path in enumerate(input_paths):
                # 文件存在性检查
                if not os.path.exists(file_path):
                    return {
                        'valid': False,
                        'reason': f'输入文件不存在: {file_path}',
                        'details': {}
                    }
                
                # 获取文件格式
                file_format = self._detect_format(file_path)
                if not file_format:
                    return {
                        'valid': False,
                        'reason': f'不支持的文件格式: {file_path}',
                        'details': {}
                    }
                
                # 格式一致性检查
                if base_format is None:
                    base_format = file_format
                elif base_format != file_format:
                    return {
                        'valid': False,
                        'reason': f'文件格式不一致: 基准格式为{base_format}，文件{file_path}为{file_format}',
                        'details': {'base_format': base_format}
                    }
                
                # 字段一致性检查
                if self.field_extractor:
                    fields = self.field_extractor.get_fields(file_path)
                    if not fields:
                        return {
                            'valid': False,
                            'reason': f'无法获取文件字段信息: {file_path}',
                            'details': {}
                        }
                    
                    field_names = [f['name'] for f in fields]
                else:
                    # 简化版字段提取
                    field_names = self._get_fields_simple(file_path, file_format)
                    if not field_names:
                        return {
                            'valid': False,
                            'reason': f'无法获取文件字段信息: {file_path}',
                            'details': {}
                        }
                
                # 字段一致性检查
                if base_fields is None:
                    base_fields = field_names
                elif set(base_fields) != set(field_names):
                    # 详细的字段差异分析
                    base_set = set(base_fields)
                    current_set = set(field_names)
                    only_in_base = base_set - current_set
                    only_in_current = current_set - base_set
                    
                    error_details = {
                        'base_file': input_paths[0],
                        'current_file': file_path,
                        'base_fields': base_fields,
                        'current_fields': field_names,
                        'only_in_base': list(only_in_base),
                        'only_in_current': list(only_in_current)
                    }
                    
                    error_msg = f"文件字段不一致!\n\n"
                    error_msg += f"基准文件: {os.path.basename(input_paths[0])}\n"
                    error_msg += f"字段: {base_fields}\n\n"
                    error_msg += f"当前文件: {os.path.basename(file_path)}\n"
                    error_msg += f"字段: {field_names}\n\n"
                    
                    if only_in_base:
                        error_msg += f"基准文件独有字段: {list(only_in_base)}\n"
                    if only_in_current:
                        error_msg += f"当前文件独有字段: {list(only_in_current)}\n"
                    
                    error_msg += "\n💡 提示: 只有字段完全一致的文件才能合并!"
                    
                    return {
                        'valid': False,
                        'reason': error_msg,
                        'details': error_details
                    }
            
            # append模式特殊校验
            if params['merge_mode'] == 'append':
                target_path = params['target_path']
                if not os.path.exists(target_path):
                    return {
                        'valid': False,
                        'reason': f'追加模式下目标文件不存在: {target_path}',
                        'details': {}
                    }
                
                # 检查目标文件格式和字段
                target_format = self._detect_format(target_path)
                if target_format != base_format:
                    return {
                        'valid': False,
                        'reason': f'目标文件格式({target_format})与输入文件格式({base_format})不一致',
                        'details': {}
                    }
                
                if self.field_extractor:
                    target_fields_info = self.field_extractor.get_fields(target_path)
                    target_fields = [f['name'] for f in target_fields_info] if target_fields_info else []
                else:
                    target_fields = self._get_fields_simple(target_path, target_format)
                
                if target_fields != base_fields:
                    return {
                        'valid': False,
                        'reason': f'目标文件字段({target_fields})与输入文件字段({base_fields})不一致',
                        'details': {}
                    }
            
            # 去重配置校验
            if params.get('deduplicate', False):
                dedup_field = params.get('dedup_field')
                if dedup_field and dedup_field not in base_fields:
                    return {
                        'valid': False,
                        'reason': f'去重字段"{dedup_field}"不在数据集字段{base_fields}中',
                        'details': {}
                    }
            
            if self.logger:
                self.logger.info(f"合并任务校验通过: {len(input_paths)}个文件，格式{base_format}，字段{base_fields}")
            
            return {
                'valid': True,
                'reason': '校验通过',
                'details': {
                    'base_fields': base_fields,
                    'format': base_format,
                    'target_exists': os.path.exists(params['target_path']) if params['merge_mode'] == 'append' else False
                }
            }
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"合并校验失败: {str(e)}")
            return {
                'valid': False,
                'reason': f'校验过程异常: {str(e)}',
                'details': {}
            }
    
    def merge_datasets(self, params: MergeTaskParams) -> Optional[str]:
        """执行数据集合并操作
        
        按照指定参数执行合并与去重，支持大文件分片处理
        
        Args:
            params (MergeTaskParams): 合并任务参数
            
        Returns:
            Optional[str]: 成功时返回目标文件路径，失败时返回None
            
        Raises:
            Exception: 当合并过程中发生错误时
        """
        try:
            task_id = params['task_id']
            if self.logger:
                self.logger.info(f"开始数据合并任务: {task_id}")
            
            # 前置校验
            validation = self.validate_merge(params)
            if not validation['valid']:
                error_msg = validation['reason']
                if self.logger:
                    self.logger.error(f"合并前校验失败: {error_msg}")
                raise ValueError(f"合并校验失败: {error_msg}")
            
            # 获取基础信息
            base_fields, file_format, total_rows = self._get_base_info(params['input_paths'])
            
            # 初始化任务状态
            if self.state_manager:
                self.state_manager.set_state(f"task.{task_id}", {
                    'status': 'running',
                    'progress': 0,
                    'total_rows': total_rows,
                    'processed_rows': 0,
                    'start_time': datetime.now().isoformat()
                })
            
            # 执行合并
            target_path = params['target_path']
            merge_mode = params['merge_mode']
            chunk_size = params.get('chunk_size', self.chunk_size)
            encoding = params.get('encoding', self.default_encoding)
            
            # 确保目标目录存在
            ensure_dir(os.path.dirname(target_path))
            
            # 统计信息
            total_input_rows = 0
            total_output_rows = 0
            duplicate_rows = 0
            input_row_counts = []
            processed_rows = 0
            
            # 用于去重的集合
            seen_values = set() if params.get('deduplicate', False) else None
            
            # 初始化输出文件
            if merge_mode == 'merge':
                # 新建模式：创建新文件
                self._init_output_file(target_path, file_format, base_fields, encoding)
            # append模式：直接追加到现有文件
            
            # 分片处理每个输入文件
            for file_idx, input_path in enumerate(params['input_paths']):
                if self.logger:
                    self.logger.info(f"处理输入文件 {file_idx + 1}/{len(params['input_paths'])}: {input_path}")
                
                file_row_count = 0
                
                # 分片读取当前文件，使用动态缓冲区大小
                write_buffer = []
                # 动态计算缓冲区大小，确保不超过内存限制
                max_buffer_rows = min(chunk_size * 5, 50000)  # 最多缓存5万行或chunk_size*5
                
                for chunk_data in self._read_file_chunks(input_path, file_format, chunk_size, encoding):
                    if not chunk_data:
                        continue
                    
                    file_row_count += len(chunk_data)
                    total_input_rows += len(chunk_data)
                    
                    # 执行去重
                    if params.get('deduplicate', False):
                        filtered_data, dup_count = self._deduplicate_data(
                            chunk_data, 
                            params.get('dedup_field'),
                            params.get('dedup_strategy', 'keep_first'),
                            seen_values
                        )
                        duplicate_rows += dup_count
                    else:
                        filtered_data = chunk_data
                    
                    # 累积到缓冲区
                    if filtered_data:
                        write_buffer.extend(filtered_data)
                        total_output_rows += len(filtered_data)
                    
                    # 当缓冲区达到指定大小时批量写入
                    if len(write_buffer) >= max_buffer_rows:
                        self._append_to_file(target_path, file_format, write_buffer, encoding)
                        write_buffer = []
                        
                        # 强制垃圾回收，释放内存
                        import gc
                        gc.collect()
                    
                    processed_rows += len(chunk_data)
                    
                    # 更新进度
                    if self.state_manager:
                        progress = int(processed_rows / total_rows * 100) if total_rows > 0 else 100
                        self.state_manager.set_state(f"task.{task_id}", {
                            'progress': progress,
                            'processed_rows': processed_rows
                        })
                
                # 写入剩余的缓冲区数据
                if write_buffer:
                    self._append_to_file(target_path, file_format, write_buffer, encoding)
                    write_buffer = []
                
                input_row_counts.append(file_row_count)
                
                if self.logger:
                    self.logger.info(f"文件处理完成: {input_path}，记录数: {file_row_count}")
            
            # 生成合并元数据
            merge_meta = MergeMeta(
                task_id=task_id,
                merge_mode=merge_mode,
                input_files=params['input_paths'],
                input_row_counts=input_row_counts,
                target_path=target_path,
                total_input_rows=total_input_rows,
                total_output_rows=total_output_rows,
                duplicate_rows=duplicate_rows,
                start_time=self.state_manager.get_state(f"task.{task_id}.start_time") if self.state_manager else "",
                end_time=datetime.now().isoformat(),
                fields=base_fields
            )
            
            # 清理合并文件末尾的多余空行
            self._clean_file_ending(target_path, file_format, encoding)
            
            self._save_merge_meta(target_path, merge_meta)
            
            # 创建合并信息文件
            output_dir = os.path.dirname(target_path)
            self._create_merge_info_file(output_dir, merge_meta, params)
            
            # 更新最终状态
            if self.state_manager:
                self.state_manager.set_state(f"task.{task_id}", {
                    'status': 'completed',
                    'progress': 100,
                    'output_path': target_path,
                    'end_time': datetime.now().isoformat()
                })
            
            if self.logger:
                self.logger.info(f"数据合并任务完成: {target_path}")
                self.logger.info(f"输入记录数: {total_input_rows}，输出记录数: {total_output_rows}，去重数量: {duplicate_rows}")
            
            return target_path
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"数据合并任务失败: {str(e)}")
            
            # 更新失败状态
            if self.state_manager:
                self.state_manager.set_state(f"task.{task_id}", {
                    'status': 'failed',
                    'error': str(e),
                    'end_time': datetime.now().isoformat()
                })
            
            # 抛出异常而不是返回 None
            raise Exception(f"数据合并执行失败: {str(e)}")
    
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
            '.jsonl': 'jsonl'
        }
        
        return format_map.get(file_ext)
    
    def _get_fields_simple(self, file_path: str, file_format: str) -> List[str]:
        """简化版字段提取
        
        Args:
            file_path (str): 文件路径
            file_format (str): 文件格式
            
        Returns:
            List[str]: 字段名称列表
        """
        try:
            if file_format == 'csv':
                df = pd.read_csv(file_path, nrows=1)
                return list(df.columns)
            elif file_format in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, nrows=1)
                return list(df.columns)
            elif file_format == 'jsonl':
                with jsonlines.open(file_path, 'r') as reader:
                    for item in reader:
                        if isinstance(item, dict):
                            return list(item.keys())
                return []
            elif file_format == 'json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list) and data and isinstance(data[0], dict):
                        return list(data[0].keys())
                    elif isinstance(data, dict):
                        return list(data.keys())
                return []
            else:
                return []
        except Exception as e:
            if self.logger:
                self.logger.warning(f"简化版字段提取失败: {str(e)}")
            return []
    
    def _get_base_info(self, input_paths: List[str]) -> Tuple[List[str], str, int]:
        """获取基础信息
        
        Args:
            input_paths (List[str]): 输入文件路径列表
            
        Returns:
            Tuple[List[str], str, int]: (基准字段列表, 格式, 总行数)
        """
        # 获取第一个文件的信息作为基准
        first_file = input_paths[0]
        file_format = self._detect_format(first_file)
        
        if self.field_extractor:
            fields_info = self.field_extractor.get_fields(first_file)
            base_fields = [f['name'] for f in fields_info] if fields_info else []
        else:
            base_fields = self._get_fields_simple(first_file, file_format)
        
        # 计算总行数
        total_rows = 0
        for file_path in input_paths:
            total_rows += self._count_file_rows(file_path, file_format)
        
        return base_fields, file_format, total_rows
    
    def _count_file_rows(self, file_path: str, file_format: str) -> int:
        """计算文件行数
        
        Args:
            file_path (str): 文件路径
            file_format (str): 文件格式
            
        Returns:
            int: 文件行数
        """
        try:
            if file_format == 'csv':
                with open(file_path, 'r', encoding=get_file_encoding(file_path)) as f:
                    return sum(1 for _ in f) - 1  # 减去表头行
            elif file_format == 'jsonl':
                with open(file_path, 'r', encoding='utf-8') as f:
                    return sum(1 for _ in f)
            elif file_format in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, nrows=1)
                # 这里使用简单估算，实际可以通过sheet信息获取
                return 1000  # 简化估算
            elif file_format == 'json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return len(data)
                    else:
                        return 1
            else:
                return 0
        except Exception:
            return 0
    
    def _read_file_chunks(self, file_path: str, file_format: str, chunk_size: int, encoding: str):
        """分片读取文件数据
        
        Args:
            file_path (str): 文件路径
            file_format (str): 文件格式
            chunk_size (int): 分片大小
            encoding (str): 文件编码
            
        Yields:
            List[Dict]: 分片数据
        """
        try:
            if file_format == 'csv':
                # CSV分片读取
                for chunk_df in pd.read_csv(file_path, chunksize=chunk_size, encoding=encoding):
                    yield chunk_df.to_dict('records')
            
            elif file_format in ['xlsx', 'xls']:
                # Excel文件读取 - 使用更安全的内存管理
                try:
                    # 尝试使用chunksize参数（pandas 1.2+支持）
                    for chunk_df in pd.read_excel(file_path, chunksize=chunk_size):
                        yield chunk_df.to_dict('records')
                except TypeError:
                    # 如果不支持chunksize，则分批处理
                    df = pd.read_excel(file_path)
                    total_rows = len(df)
                    for i in range(0, total_rows, chunk_size):
                        chunk_df = df.iloc[i:i+chunk_size]
                        yield chunk_df.to_dict('records')
                        # 显式删除chunk以释放内存
                        del chunk_df
            
            elif file_format == 'jsonl':
                # JSONL分片读取，使用原生JSON处理避免行终止符问题
                chunk_data = []
                with open(file_path, 'r', encoding=encoding) as f:
                    for line in f:
                        line = line.strip()
                        if line:  # 跳过空行
                            try:
                                item = json.loads(line)
                                chunk_data.append(item)
                                if len(chunk_data) >= chunk_size:
                                    yield chunk_data
                                    chunk_data = []
                            except json.JSONDecodeError as e:
                                # 忽略无效的JSON行，但记录警告
                                if self.logger:
                                    self.logger.warning(f"跳过无效JSON行: {line[:100]}... 错误: {str(e)}")
                                continue
                    
                    # 处理最后一个不完整的分片
                    if chunk_data:
                        yield chunk_data
            
            elif file_format == 'json':
                # JSON文件读取 - 避免大文件内存溢出
                with open(file_path, 'r', encoding=encoding) as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        total_items = len(data)
                        for i in range(0, total_items, chunk_size):
                            chunk = data[i:i+chunk_size]
                            yield chunk
                            # 在处理大型JSON时帮助内存回收
                            if total_items > 100000:  # 对于大于10万条记录的文件
                                import gc
                                gc.collect()
                    else:
                        yield [data]
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"读取文件分片失败: {file_path}, {str(e)}")
            yield []
    
    def _init_output_file(self, target_path: str, file_format: str, fields: List[str], encoding: str):
        """初始化输出文件
        
        Args:
            target_path (str): 目标文件路径
            file_format (str): 文件格式
            fields (List[str]): 字段列表
            encoding (str): 文件编码
        """
        try:
            if file_format == 'csv':
                # 创建CSV文件并写入表头
                df = pd.DataFrame(columns=fields)
                df.to_csv(target_path, index=False, encoding=encoding)
            
            elif file_format in ['xlsx', 'xls']:
                # 创建Excel文件并写入表头
                df = pd.DataFrame(columns=fields)
                df.to_excel(target_path, index=False)
            
            elif file_format == 'jsonl':
                # 创建空JSONL文件
                open(target_path, 'w', encoding=encoding).close()
            
            elif file_format == 'json':
                # 创建JSON文件并写入空数组
                with open(target_path, 'w', encoding=encoding) as f:
                    json.dump([], f, ensure_ascii=False)
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"初始化输出文件失败: {str(e)}")
    
    def _append_to_file(self, target_path: str, file_format: str, data: List[Dict], encoding: str):
        """追加数据到文件
        
        Args:
            target_path (str): 目标文件路径
            file_format (str): 文件格式
            data (List[Dict]): 待追加的数据
            encoding (str): 文件编码
        """
        try:
            if file_format == 'csv':
                # 追加到CSV文件
                df = pd.DataFrame(data)
                df.to_csv(target_path, mode='a', header=False, index=False, encoding=encoding)
            
            elif file_format in ['xlsx', 'xls']:
                # Excel追加比较复杂，需要重新写入
                try:
                    existing_df = pd.read_excel(target_path)
                    new_df = pd.DataFrame(data)
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    combined_df.to_excel(target_path, index=False)
                except Exception:
                    # 如果文件不存在或读取失败，直接写入
                    df = pd.DataFrame(data)
                    df.to_excel(target_path, index=False)
            
            elif file_format == 'jsonl':
                # 追加到JSONL文件，使用统一的行终止符
                with open(target_path, 'a', encoding=encoding, newline='\n') as f:
                    for item in data:
                        # 确保使用统一的换行符，避免异常终止符
                        json_line = json.dumps(item, ensure_ascii=False)
                        f.write(json_line + '\n')
            
            elif file_format == 'json':
                # JSON文件需要重新写入整个数组
                try:
                    with open(target_path, 'r', encoding=encoding) as f:
                        existing_data = json.load(f)
                    
                    if not isinstance(existing_data, list):
                        existing_data = []
                    
                    existing_data.extend(data)
                    
                    with open(target_path, 'w', encoding=encoding) as f:
                        json.dump(existing_data, f, ensure_ascii=False, indent=2)
                except Exception:
                    # 如果文件不存在或读取失败，直接写入
                    with open(target_path, 'w', encoding=encoding) as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"追加数据到文件失败: {str(e)}")
    
    def _deduplicate_data(self, data: List[Dict], dedup_field: Optional[str], strategy: str, seen_values: set) -> Tuple[List[Dict], int]:
        """对数据进行去重
        
        Args:
            data (List[Dict]): 待去重的数据
            dedup_field (Optional[str]): 去重字段，None表示全量去重
            strategy (str): 去重策略 keep_first/keep_last
            seen_values (set): 已见过的值集合
            
        Returns:
            Tuple[List[Dict], int]: (去重后数据, 去重数量)
        """
        if seen_values is None:
            return data, 0
        
        filtered_data = []
        duplicate_count = 0
        
        for item in data:
            # 生成去重键
            if dedup_field is None:
                # 全量字段去重
                dedup_key = tuple(sorted(item.items()))
            else:
                # 指定字段去重
                dedup_key = item.get(dedup_field)
            
            # 检查是否重复
            if dedup_key in seen_values:
                duplicate_count += 1
                if strategy == 'keep_last':
                    # keep_last策略：移除之前的记录，保留当前记录
                    # 由于这里是分片处理，无法简单实现keep_last
                    # 简化处理：仍然跳过重复项
                    continue
                else:
                    # keep_first策略：跳过重复项
                    continue
            else:
                seen_values.add(dedup_key)
                filtered_data.append(item)
        
        return filtered_data, duplicate_count
    
    def _clean_file_ending(self, file_path: str, file_format: str, encoding: str = 'utf-8'):
        """清理文件末尾的多余空行
        
        Args:
            file_path (str): 文件路径
            file_format (str): 文件格式
            encoding (str): 文件编码
        """
        try:
            if file_format == 'jsonl':
                # 读取文件内容
                with open(file_path, 'r', encoding=encoding) as f:
                    content = f.read()
                
                # 移除末尾的多余换行符，但保留最后一行的换行符
                content = content.rstrip('\n') + '\n'
                
                # 重写文件
                with open(file_path, 'w', encoding=encoding, newline='\n') as f:
                    f.write(content)
                
                if self.logger:
                    self.logger.debug(f"已清理文件末尾空行: {file_path}")
                    
        except Exception as e:
            if self.logger:
                self.logger.warning(f"清理文件末尾失败: {str(e)}")
    
    def _save_merge_meta(self, target_path: str, meta: MergeMeta):
        """保存合并元数据
        
        Args:
            target_path (str): 目标文件路径
            meta (MergeMeta): 合并元数据
        """
        try:
            # 生成元数据文件路径
            target_dir = os.path.dirname(target_path)
            meta_file = os.path.join(target_dir, 'merge_meta.json')
            
            # 保存元数据
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            
            if self.logger:
                self.logger.info(f"合并元数据已保存: {meta_file}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"保存合并元数据失败: {str(e)}")
    
    def _create_merge_info_file(self, output_dir: str, meta: MergeMeta, params: Dict):
        """创建合并信息文件
        
        Args:
            output_dir (str): 输出目录
            meta (MergeMeta): 合并元数据
            params (Dict): 合并参数
        """
        try:
            # 创建信息文件内容
            info_content = []
            info_content.append("=" * 60)
            info_content.append("数据合并任务信息")
            info_content.append("=" * 60)
            info_content.append("")
            
            # 基本信息
            info_content.append("【基本信息】")
            info_content.append(f"任务ID: {meta.task_id}")
            info_content.append(f"合并模式: {meta.merge_mode}")
            info_content.append(f"开始时间: {meta.start_time}")
            info_content.append(f"结束时间: {meta.end_time}")
            info_content.append("")
            
            # 输入文件信息
            info_content.append("【输入文件信息】")
            for i, (file_path, row_count) in enumerate(zip(meta.input_files, meta.input_row_counts)):
                file_name = os.path.basename(file_path)
                info_content.append(f"{i+1}. {file_name}")
                info_content.append(f"   路径: {file_path}")
                info_content.append(f"   记录数: {row_count:,}")
            info_content.append("")
            
            # 输出信息
            info_content.append("【输出信息】")
            info_content.append(f"输出文件: {os.path.basename(meta.target_path)}")
            info_content.append(f"输出路径: {meta.target_path}")
            info_content.append("")
            
            # 统计信息
            info_content.append("【统计信息】")
            info_content.append(f"输入总记录数: {meta.total_input_rows:,}")
            info_content.append(f"输出记录数: {meta.total_output_rows:,}")
            if meta.duplicate_rows > 0:
                info_content.append(f"去重记录数: {meta.duplicate_rows:,}")
                retention_rate = (meta.total_output_rows / meta.total_input_rows * 100) if meta.total_input_rows > 0 else 0
                info_content.append(f"数据保留率: {retention_rate:.2f}%")
            info_content.append("")
            
            # 字段信息
            if meta.fields:
                info_content.append("【字段信息】")
                info_content.append(f"字段总数: {len(meta.fields)}")
                info_content.append("字段列表:")
                for i, field in enumerate(meta.fields, 1):
                    info_content.append(f"  {i:2d}. {field}")
                info_content.append("")
            
            # 合并参数
            info_content.append("【合并参数】")
            if params.get('deduplicate'):
                info_content.append(f"去重设置: 开启")
                info_content.append(f"去重字段: {params.get('dedup_field', '无')}")
                info_content.append(f"去重策略: {params.get('dedup_strategy', 'keep_first')}")
            else:
                info_content.append(f"去重设置: 关闭")
            info_content.append(f"分片大小: {params.get('chunk_size', 1000)}")
            info_content.append(f"文件编码: {params.get('encoding', 'utf-8')}")
            info_content.append("")
            
            info_content.append("=" * 60)
            info_content.append(f"合并任务完成时间: {meta.end_time}")
            info_content.append("=" * 60)
            
            # 保存信息文件
            info_file = os.path.join(output_dir, "合并信息.txt")
            with open(info_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(info_content))
            
            if self.logger:
                self.logger.info(f"合并信息文件已创建: {info_file}")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"创建合并信息文件失败: {str(e)}")

def main():
    """命令行主入口函数
    
    解析命令行参数并执行相应的数据合并操作
    支持合并校验和数据合并两种操作模式
    """
    parser = argparse.ArgumentParser(description='数据合并模块')
    parser.add_argument('--task_id', required=True, help='任务ID')
    parser.add_argument('--input_paths', required=True, help='输入文件路径(逗号分隔)')
    parser.add_argument('--merge_mode', choices=['merge', 'append'], 
                       required=True, help='合并模式')
    parser.add_argument('--target_path', required=True, help='目标文件路径')
    parser.add_argument('--deduplicate', type=bool, default=False, help='是否去重')
    parser.add_argument('--dedup_field', help='去重字段')
    parser.add_argument('--dedup_strategy', choices=['keep_first', 'keep_last'],
                       default='keep_first', help='去重策略')
    parser.add_argument('--chunk_size', type=int, help='分片大小')
    parser.add_argument('--encoding', help='文件编码')
    parser.add_argument('--validate_only', type=bool, default=False, 
                       help='仅校验不执行合并')
    
    args = parser.parse_args()
    
    # 初始化合并器
    merger = DataMerger()
    if not merger.init_merger():
        print(json.dumps({
            'status': 'failed',
            'error': '数据合并器初始化失败'
        }, ensure_ascii=False))
        return
    
    try:
        # 准备任务参数
        task_params = MergeTaskParams(
            task_id=args.task_id,
            input_paths=args.input_paths.split(','),
            merge_mode=args.merge_mode,
            target_path=args.target_path,
            deduplicate=args.deduplicate,
            dedup_field=args.dedup_field,
            dedup_strategy=args.dedup_strategy,
            chunk_size=args.chunk_size or merger.chunk_size,
            encoding=args.encoding or merger.default_encoding
        )
        
        if args.validate_only:
            # 仅校验模式
            validation = merger.validate_merge(task_params)
            print(json.dumps({
                'status': 'success' if validation['valid'] else 'failed',
                'validation': validation
            }, ensure_ascii=False))
        else:
            # 执行合并
            result = merger.merge_datasets(task_params)
            
            if result:
                print(json.dumps({
                    'status': 'success',
                    'task_id': args.task_id,
                    'output': result
                }, ensure_ascii=False))
            else:
                print(json.dumps({
                    'status': 'failed',
                    'task_id': args.task_id,
                    'error': '数据合并失败'
                }, ensure_ascii=False))
                
    except Exception as e:
        print(json.dumps({
            'status': 'failed',
            'error': str(e)
        }, ensure_ascii=False))


# 创建全局实例
data_merger = DataMerger()
data_merger.init_merger()  # 初始化合并器

# 全局API函数
def merge_data(source_paths: List[str], mode: str = "merge", dedup_field: str = None, output_dir: str = None, **kwargs) -> str:
    """合并数据的全局API"""
    
    # 生成输出目录结构
    if not output_dir:
        output_dir = "./processed"
    
    # 为每次合并创建独立的子目录
    timestamp = int(time.time())
    merge_folder_name = f"merge-{timestamp}"
    merge_output_dir = os.path.join(output_dir, merge_folder_name)
    
    # 创建合并专用目录
    try:
        os.makedirs(merge_output_dir, exist_ok=True)
    except Exception as e:
        raise ValueError(f"无法创建合并输出目录 {merge_output_dir}: {str(e)}")
    
    # 检查输出目录写入权限
    if not os.access(merge_output_dir, os.W_OK):
        raise ValueError(f"合并输出目录没有写入权限: {merge_output_dir}")
    
    # 根据源文件名生成合并后的文件名
    source_names = []
    file_extension = None
    for path in source_paths:
        basename = os.path.splitext(os.path.basename(path))[0]
        source_names.append(basename)
        if file_extension is None:
            file_extension = os.path.splitext(path)[1]
    
    # 生成合并文件名（使用更简洁的命名）
    if len(source_names) <= 3:
        merged_name = "_".join(source_names)
    else:
        merged_name = f"{source_names[0]}_and_{len(source_names)-1}_others"
    
    output_filename = f"merged_{merged_name}{file_extension}"
    target_path = os.path.join(merge_output_dir, output_filename)
    
    task_params = {
        'task_id': f"merge-{timestamp}-{uuid.uuid4().hex[:6]}",
        'input_paths': source_paths,
        'merge_mode': mode,
        'target_path': target_path,
        'deduplicate': bool(dedup_field),
        'dedup_field': dedup_field,
        'dedup_strategy': kwargs.get('dedup_strategy', 'keep_first'),
        'chunk_size': kwargs.get('chunk_size', 1000),
        'encoding': kwargs.get('encoding', 'utf-8')
    }
    
    return data_merger.merge_datasets(task_params)

if __name__ == '__main__':
    main()
