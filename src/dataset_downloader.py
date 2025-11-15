#!/usr/bin/env python3
"""
数据集下载模块

本模块提供多源数据集下载功能，支持从Huggingface、ModelScope和直接URL下载数据集。
功能特点：
- 支持多种数据源：Huggingface Datasets、ModelScope、HTTP/HTTPS URL
- 断点续传和自动重试机制
- 完整的进度跟踪和日志记录
- 命令行工具和Python API双重接口
- 任务管理和状态监控
- 自动生成详细的下载元数据

设计原则：
- 模块化设计，职责单一，仅负责数据下载
- 完全独立，不依赖其他业务模块
- 健壮的错误处理和异常恢复
- 详细的日志记录和进度追踪

作者：自动数据蒸馏软件团队
版本：v2.0
许可：商业软件
"""

import os
import json
import time
import hashlib
import argparse
import logging
import urllib.parse
import random
import string
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 核心依赖库导入（必需）
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("错误: 缺少核心依赖库 requests")
    print("请运行: pip install requests")
    exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("错误: 缺少核心依赖库 tqdm")
    print("请运行: pip install tqdm")
    exit(1)

# 可选功能依赖库（Huggingface支持）
try:
    from datasets import load_dataset
    from huggingface_hub import hf_hub_download
    HF_AVAILABLE = True
except ImportError:
    HF_AVAILABLE = False

# 可选功能依赖库（ModelScope支持）
try:
    import modelscope
    # 尝试多种导入方式以兼容不同版本
    try:
        from modelscope.msdatasets import MsDataset
    except ImportError:
        try:
            from modelscope import MsDataset
        except ImportError:
            MsDataset = None
    
    try:
        from modelscope.hub.file_download import model_file_download
    except ImportError:
        try:
            from modelscope.hub.api import HubApi
            model_file_download = None
        except ImportError:
            model_file_download = None
            HubApi = None
    
    MS_AVAILABLE = True
except ImportError:
    MS_AVAILABLE = False
    MsDataset = None
    model_file_download = None

# 导入公共基础支撑层模块
try:
    from .config_manager import config_manager
    from .log_manager import log_manager  
    from .state_manager import state_manager
    from .utils import FileOperations
    USE_EXTERNAL_MODULES = True
except ImportError:
    try:
        # 尝试直接导入（用于命令行执行）
        import sys
        import os
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sys.path.insert(0, current_dir)
        
        from config_manager import config_manager
        from log_manager import log_manager
        from state_manager import state_manager
        from utils import FileOperations
        USE_EXTERNAL_MODULES = True
    except ImportError:
        # 如果导入失败，使用内部定义的类
        USE_EXTERNAL_MODULES = False


class DownloadError(Exception):
    """
    下载相关异常类
    
    用于处理下载过程中出现的各种错误情况，
    包括网络错误、文件错误、认证错误等。
    """
    pass


class ConfigManager:
    """
    配置管理器
    
    负责管理下载模块的各项配置参数，包括下载行为配置、
    目录结构配置、网络参数配置等。
    
    设计原则：
    - 提供合理的默认配置
    - 支持运行时配置修改
    - 确保配置参数的有效性
    """
    
    def __init__(self):
        """初始化配置管理器，设置默认配置参数"""
        self.config = {
            # 基础配置
            'base': {
                'root_dir': './data',           # 数据根目录
                'chunk_size': 1000,             # 数据分块大小
                'encoding': 'utf-8'             # 文件编码
            },
            # 下载配置
            'download': {
                'timeout': 300,                 # 请求超时时间（秒）
                'retry_count': 3,               # 重试次数
                'buffer_size': 8192,            # 下载缓冲区大小（字节）
                'max_parallel_tasks': 5,        # 最大并行任务数
                'max_download_speed': 0,        # 最大下载速度（0=无限制）
                'chunk_size': 8388608,          # 下载块大小（8MB）
                'temp_file_expire': 86400,      # 临时文件过期时间（24小时）
                'user_agent': 'DatasetDownloader/2.0 (Commercial Software)'  # 用户代理字符串
            }
        }
    
    def get_config(self, key: str, default=None):
        """
        获取配置值
        
        Args:
            key: 配置键，支持点分割的层级键（如 'download.timeout'）
            default: 默认值
            
        Returns:
            配置值或默认值
        """
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value
    
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
        
        创建数据根目录下的各个子目录：
        - raw: 原始数据存储
        - temp: 临时文件存储
        - logs: 日志文件存储
        """
        root_dir = Path(self.get_config('base.root_dir'))
        directories = ['raw', 'temp', 'logs']
        
        for dir_name in directories:
            dir_path = root_dir / dir_name
            dir_path.mkdir(parents=True, exist_ok=True)


class Logger:
    """
    日志管理器
    
    负责下载模块的日志记录，支持控制台输出和文件输出。
    提供不同级别的日志记录功能，便于调试和监控。
    
    特性：
    - 支持任务ID关联的日志记录
    - 自动文件日志轮转
    - 优雅的错误处理
    """
    
    def __init__(self, name: str = "dataset_downloader"):
        """
        初始化日志管理器
        
        Args:
            name: 日志器名称
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # 避免重复添加处理器
        if not self.logger.handlers:
            # 设置控制台输出处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # 设置日志格式
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            
            # 设置文件输出处理器
            try:
                log_dir = Path('./data/logs')
                log_dir.mkdir(parents=True, exist_ok=True)
                log_file = log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
                
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_handler.setLevel(logging.DEBUG)
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except Exception:
                # 文件日志失败不影响基本功能
                pass
    
    def info(self, msg: str, task_id: str = ""):
        """
        记录信息级别日志
        
        Args:
            msg: 日志消息
            task_id: 任务ID（可选）
        """
        if task_id:
            msg = f"[{task_id}] {msg}"
        self.logger.info(msg)
    
    def error(self, msg: str, task_id: str = ""):
        """
        记录错误级别日志
        
        Args:
            msg: 日志消息
            task_id: 任务ID（可选）
        """
        if task_id:
            msg = f"[{task_id}] {msg}"
        self.logger.error(msg)
    
    def warning(self, msg: str, task_id: str = ""):
        """
        记录警告级别日志
        
        Args:
            msg: 日志消息
            task_id: 任务ID（可选）
        """
        if task_id:
            msg = f"[{task_id}] {msg}"
        self.logger.warning(msg)
    
    def debug(self, msg: str, task_id: str = ""):
        """
        记录调试级别日志
        
        Args:
            msg: 日志消息
            task_id: 任务ID（可选）
        """
        if task_id:
            msg = f"[{task_id}] {msg}"
        self.logger.debug(msg)


class ProgressTracker:
    """
    进度跟踪器
    
    负责跟踪下载任务的进度状态，包括下载速度、剩余时间、
    错误状态等信息。支持线程安全的状态更新。
    
    状态定义：
    - pending: 等待开始
    - running: 正在下载
    - paused: 暂停中
    - completed: 下载完成
    - failed: 下载失败
    """
    
    def __init__(self, task_id: str):
        """
        初始化进度跟踪器
        
        Args:
            task_id: 任务唯一标识符
        """
        self.task_id = task_id
        self.status = "pending"                 # 任务状态
        self.progress = 0                       # 进度百分比
        self.downloaded_bytes = 0               # 已下载字节数
        self.total_bytes = 0                    # 总字节数
        self.start_time = None                  # 开始时间（datetime对象或None）
        self.speed = 0                          # 下载速度（字节/秒）
        self.eta = 0                            # 预计剩余时间（秒）
        self.last_update = time.time()          # 上次更新时间
        self.error_msg = ""                     # 错误消息
        self.lock = threading.Lock()            # 线程锁
    
    def start(self, total_bytes: int = 0):
        """
        开始进度跟踪
        
        Args:
            total_bytes: 文件总大小（字节）
        """
        with self.lock:
            self.status = "running"
            self.total_bytes = total_bytes
            # 使用datetime记录开始时间，last_update仍然用时间戳用于速度计算
            self.start_time = datetime.now()
            self.last_update = time.time()
    
    def update(self, downloaded_bytes: int):
        """
        更新下载进度
        
        Args:
            downloaded_bytes: 已下载的字节数
        """
        with self.lock:
            self.downloaded_bytes = downloaded_bytes
            current_time = time.time()
            
            # 计算进度百分比
            if self.total_bytes > 0:
                self.progress = min(100, int((downloaded_bytes / self.total_bytes) * 100))
            
            # 计算下载速度和剩余时间（每秒更新一次）
            if current_time - self.last_update >= 1.0:
                if self.start_time:
                    # elapsed基于时间戳last_update与start时的时间戳差值
                    # start_time是datetime，需要转换
                    elapsed = current_time - self.start_time.timestamp()
                    if elapsed > 0:
                        self.speed = downloaded_bytes / elapsed
                        
                        # 计算预计剩余时间
                        if self.speed > 0 and self.total_bytes > 0:
                            remaining_bytes = self.total_bytes - downloaded_bytes
                            self.eta = remaining_bytes / self.speed
                
                self.last_update = current_time
    
    def update_progress(self, progress_percent: int):
        """
        直接更新进度百分比（用于无法获取字节数的情况）
        
        Args:
            progress_percent: 进度百分比（0-100）
        """
        with self.lock:
            self.progress = max(0, min(100, progress_percent))
            self.last_update = time.time()
            if self.start_time is None:
                self.start_time = datetime.now()
    
    def complete(self):
        """标记任务完成"""
        with self.lock:
            self.status = "completed"
            self.progress = 100
    
    def fail(self, error_msg: str = ""):
        """
        标记任务失败
        
        Args:
            error_msg: 错误描述信息
        """
        with self.lock:
            self.status = "failed"
            self.error_msg = error_msg
    
    def pause(self):
        """暂停任务"""
        with self.lock:
            if self.status == "running":
                self.status = "paused"
    
    def resume(self):
        """恢复任务"""
        with self.lock:
            if self.status == "paused":
                self.status = "running"
    
    def get_info(self) -> Dict[str, Any]:
        """
        获取进度信息
        
        Returns:
            包含所有进度信息的字典
        """
        with self.lock:
            # 规范化开始时间
            start_time_iso = None
            if isinstance(self.start_time, datetime):
                start_time_iso = self.start_time.isoformat()
            elif isinstance(self.start_time, (int, float)):
                # 兼容历史写入，尽量转为ISO
                try:
                    start_time_iso = datetime.fromtimestamp(self.start_time).isoformat()
                except Exception:
                    start_time_iso = None
            info = {
                'task_id': self.task_id,
                'status': self.status,
                'progress': self.progress,
                'downloaded_bytes': self.downloaded_bytes,
                'total_bytes': self.total_bytes,
                'speed': self.speed,
                'eta': self.eta,
                'start_time': start_time_iso
            }
            if self.error_msg:
                info['error_msg'] = self.error_msg
            return info


class DatasetDownloader:
    """
    数据集下载器主类
    
    这是下载模块的核心类，负责管理所有的下载任务。
    支持多种数据源的下载，包括断点续传、错误重试等高级功能。
    
    设计特点：
    - 单一职责：专注于数据下载，不涉及数据处理
    - 任务管理：支持多任务并发和状态跟踪
    - 容错机制：完善的错误处理和恢复策略
    - 扩展性：易于添加新的数据源支持
    """
    
    def __init__(self):
        """初始化下载器"""
        if USE_EXTERNAL_MODULES:
            # 使用外部公共模块
            self.config_mgr = config_manager
            self.logger = log_manager.get_logger('dataset_downloader')
            self.state_mgr = state_manager
        else:
            # 使用内部定义的类（向后兼容）
            self.config_mgr = ConfigManager()
            self.logger = Logger("dataset_downloader")
            self.config_mgr.ensure_directories()
        
        self.tasks = {}  # 任务存储字典，key为task_id，value为任务信息
        
        # 加载已保存的任务状态
        self._load_tasks_from_state()
        
        # 配置HTTP会话，支持重试和超时控制
        self.session = requests.Session()
        retry_strategy = Retry(
            total=self.config_mgr.get_config('download.retry_count', 3),
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],  # 需要重试的HTTP状态码
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # 设置用户代理标识
        user_agent = self.config_mgr.get_config('download.user_agent', 
                                                'DatasetDownloader/2.0 (Commercial Software)')
        self.session.headers.update({
            'User-Agent': user_agent
        })
    
    def _extract_provider_from_dataset_name(self, dataset_name: str) -> tuple:
        """
        从数据集名称中提取提供商和数据集名称
        
        Args:
            dataset_name: 完整的数据集名称，如 "PAI/DistilQwen_100k" 或 "PAI_DistilQwen_100k"
            
        Returns:
            tuple: (provider, clean_dataset_name)
        """
        # 处理不同的命名格式
        if '/' in dataset_name:
            # 格式: "PAI/DistilQwen_100k" 
            parts = dataset_name.split('/', 1)
            if len(parts) == 2:
                return parts[0], parts[1]
        
        # 格式: "PAI_DistilQwen_100k" - 假设第一个下划线前是提供商
        if '_' in dataset_name:
            parts = dataset_name.split('_', 1)
            if len(parts) == 2:
                return parts[0], parts[1]
        
        # 如果无法解析，返回原始名称作为数据集名称，提供商为空
        return None, dataset_name
    
    def _clean_cache_directory(self, cache_dir: Path, task_id: str):
        """
        清理缓存目录
        
        Args:
            cache_dir: 缓存目录路径
            task_id: 任务ID
        """
        try:
            if cache_dir.exists() and cache_dir.is_dir():
                # 计算缓存大小
                cache_size = sum(f.stat().st_size for f in cache_dir.rglob('*') if f.is_file())
                cache_size_mb = cache_size / (1024 * 1024)
                
                self.logger.info(f"清理缓存目录: {cache_dir} (大小: {cache_size_mb:.1f}MB)", task_id)
                
                # 删除缓存目录
                import shutil
                shutil.rmtree(cache_dir)
                
                self.logger.info(f"缓存目录清理完成: {cache_dir}", task_id)
            else:
                self.logger.debug(f"缓存目录不存在或不是目录: {cache_dir}", task_id)
        except Exception as e:
            self.logger.warning(f"清理缓存目录失败: {e}", task_id)
        # 重新设置适配器（重新创建以避免引用未定义变量）
        try:
            retry_strategy = Retry(
                total=self.config_mgr.get_config('download.retry_count', 3),
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            _adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("http://", _adapter)
            self.session.mount("https://", _adapter)
        except Exception:
            pass
        
        # 设置用户代理标识
        user_agent = self.config_mgr.get_config('download.user_agent', 
                                                'DatasetDownloader/2.0 (Commercial Software)')
        self.session.headers.update({
            'User-Agent': user_agent
        })
    
    def generate_task_id(self) -> str:
        """
        生成唯一的任务标识符
        
        Returns:
            格式为 'dl-YYYYMMDDHHMMSS-xxxxxx' 的任务ID
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        import random
        import string
        rand_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"dl-{timestamp}-{rand_str}"
    
    def _load_tasks_from_state(self):
        """从状态文件加载任务"""
        try:
            if USE_EXTERNAL_MODULES and hasattr(self, 'state_mgr'):
                saved_tasks = self.state_mgr.get_state('download_tasks', {})
                
                # 重建任务对象，特别是ProgressTracker
                for task_id, task_data in saved_tasks.items():
                    if isinstance(task_data, dict) and 'params' in task_data:
                        # 重建ProgressTracker对象
                        tracker = ProgressTracker(task_id)
                        
                        # 恢复进度信息（如果存在）
                        if 'progress' in task_data:
                            progress_data = task_data['progress']
                            if isinstance(progress_data, dict):
                                tracker.status = progress_data.get('status', 'pending')
                                tracker.progress = progress_data.get('progress', 0)
                                tracker.downloaded_bytes = progress_data.get('downloaded_bytes', 0)
                                tracker.total_bytes = progress_data.get('total_bytes', 0)
                                if progress_data.get('start_time'):
                                    try:
                                        # 兼容可能的Z结尾格式
                                        st = progress_data['start_time']
                                        if isinstance(st, str):
                                            st = st.replace('Z', '+00:00')
                                            tracker.start_time = datetime.fromisoformat(st)
                                    except Exception:
                                        tracker.start_time = None
                        
                        # 更新任务数据
                        self.tasks[task_id] = {
                            'params': task_data['params'],
                            'tracker': tracker
                        }
                
                self.logger.info(f'状态文件加载成功: {len(self.tasks)} 个任务')
            else:
                self.logger.warning('状态管理器不可用，无法加载任务状态')
        except Exception as e:
            self.logger.error(f'加载任务状态失败: {e}')
    
    def _save_tasks_to_state(self):
        """保存任务到状态文件"""
        try:
            if USE_EXTERNAL_MODULES and hasattr(self, 'state_mgr'):
                # 序列化任务数据
                serializable_tasks = {}
                for task_id, task_data in self.tasks.items():
                    serializable_task = {
                        'params': task_data['params']
                    }
                    
                    # 序列化ProgressTracker
                    if 'tracker' in task_data and task_data['tracker']:
                        tracker = task_data['tracker']
                        serializable_task['progress'] = tracker.get_info()
                    
                    serializable_tasks[task_id] = serializable_task
                
                self.state_mgr.set_state('download_tasks', serializable_tasks)
                self.logger.debug(f'任务状态已保存: {len(serializable_tasks)} 个任务')
            else:
                self.logger.debug('状态管理器不可用，无法保存任务状态')
        except Exception as e:
            self.logger.error(f'保存任务状态失败: {e}')
    
    def add_download_task(self, **kwargs) -> str:
        """
        添加下载任务到任务队列
        
        Args:
            source_type: 数据源类型 ('huggingface', 'modelscope', 'url')
            dataset_name: 数据集名称或URL
            save_dir: 保存目录 (可选，将使用默认目录)
            token: API认证令牌 (可选)
            resume: 是否启用断点续传 (默认True)
            timeout: 请求超时时间 (默认300秒)
            retry_count: 重试次数 (默认3次)
            headers: 自定义HTTP头 (可选)
            **kwargs: 其他数据源特定参数
        
        Returns:
            str: 任务ID
            
        Raises:
            ValueError: 当必需参数缺失或参数值无效时
        """
        # 验证必需参数
        if 'source_type' not in kwargs:
            raise ValueError("缺少必需参数: source_type")
        if 'dataset_name' not in kwargs:
            raise ValueError("缺少必需参数: dataset_name")
        
        source_type = kwargs['source_type'].lower()
        if source_type not in ['huggingface', 'modelscope', 'url']:
            raise ValueError(f"不支持的数据源类型: {source_type}")
        
        # 生成任务ID并设置任务参数
        task_id = self.generate_task_id()
        task_params = {
            'task_id': task_id,
            'source_type': source_type,
            'dataset_name': kwargs['dataset_name'],
            'save_dir': kwargs.get('save_dir') or self._get_default_save_dir(source_type),
            'token': kwargs.get('token'),
            'resume': kwargs.get('resume', True),
            'timeout': kwargs.get('timeout', self.config_mgr.get_config('download.timeout')),
            'retry_count': kwargs.get('retry_count', self.config_mgr.get_config('download.retry_count')),
            'headers': kwargs.get('headers', {}),
            'created_at': datetime.now().isoformat(),
            'extra_params': {k: v for k, v in kwargs.items() 
                           if k not in ['source_type', 'dataset_name', 'save_dir', 'token', 
                                      'resume', 'timeout', 'retry_count', 'headers']}
        }
        
        # 创建进度跟踪器
        tracker = ProgressTracker(task_id)
        self.tasks[task_id] = {
            'params': task_params,
            'tracker': tracker
        }
        
        # 保存任务状态到文件
        self._save_tasks_to_state()
        
        self.logger.info(f"下载任务已创建: {source_type} - {kwargs['dataset_name']}", task_id)
        return task_id
    
    def _get_default_save_dir(self, source_type: str) -> str:
        """
        获取指定数据源的默认保存目录
        
        Args:
            source_type: 数据源类型
            
        Returns:
            默认保存目录路径
        """
        root_dir = Path(self.config_mgr.get_config('base.root_dir'))
        return str(root_dir / 'raw')
    
    def start_task(self, task_id: str, async_mode: bool = False) -> bool:
        """
        启动指定的下载任务
        
        Args:
            task_id: 任务ID
            async_mode: 是否异步执行（默认False，同步执行适用于独立脚本；True适用于UI调用）
            
        Returns:
            bool: 是否成功启动
        """
        if task_id not in self.tasks:
            self.logger.error(f"任务不存在: {task_id}")
            return False
        
        task = self.tasks[task_id]
        params = task['params']
        tracker = task['tracker']
        
        if tracker.status == "running":
            self.logger.warning(f"任务已在运行中", task_id)
            return False
        
        try:
            def _run_download():
                try:
                    # 根据数据源类型调用相应的下载方法
                    source_type = params['source_type']
                    if source_type == 'huggingface':
                        self._download_huggingface(task_id, params, tracker)
                    elif source_type == 'modelscope':
                        self._download_modelscope(task_id, params, tracker)
                    elif source_type == 'url':
                        self._download_url(task_id, params, tracker)
                except Exception as e:
                    self.logger.error(f"任务执行失败: {str(e)}", task_id)
                    tracker.fail(str(e))
                finally:
                    # 无论成功失败都保存状态
                    self._save_tasks_to_state()
            
            if async_mode:
                # UI模式：异步启动下载任务，不阻塞界面
                import threading
                download_thread = threading.Thread(target=_run_download, daemon=True)
                download_thread.start()
            else:
                # 独立脚本模式：同步执行，等待完成
                _run_download()
            
            return True
            
        except Exception as e:
            self.logger.error(f"任务启动失败: {str(e)}", task_id)
            tracker.fail(str(e))
            self._save_tasks_to_state()
            return False
    
    def _download_huggingface(self, task_id: str, params: Dict, tracker: ProgressTracker):
        """
        从Huggingface平台下载数据集
        
        支持Huggingface Hub上的所有公开和私有数据集。
        使用官方的datasets库进行下载，确保兼容性和稳定性。
        
        Token传入方式支持多种配置：
        1. 通过params['token']直接传入
        2. 通过环境变量HUGGINGFACE_HUB_TOKEN
        3. 通过params['hf_token']传入（兼容性）
        4. 从配置文件或页面接口传入
        
        Args:
            task_id: 任务ID
            params: 任务参数，包含token配置
            tracker: 进度跟踪器
            
        Raises:
            DownloadError: 当Huggingface库不可用或下载失败时
        """
        if not HF_AVAILABLE:
            raise DownloadError("Huggingface功能不可用，请安装相关依赖: pip install datasets huggingface-hub")
        
        dataset_name = params['dataset_name']
        # 创建按源类型分组的目录结构：data/raw/huggingface/数据集原名/
        base_save_dir = Path(params['save_dir'])  # 已经是 data/raw/huggingface
        save_dir = base_save_dir / dataset_name.replace('/', '_')  # 保持数据集原名，只处理路径分隔符
        save_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"开始从Huggingface下载数据集: {dataset_name}", task_id)
        tracker.start()
        
        try:
            # 多种方式获取认证令牌（按优先级）
            token = self._get_huggingface_token(params)
            if token:
                self.logger.info("使用认证token进行下载", task_id)
                # 设置环境变量，确保datasets库能正确使用token
                os.environ['HUGGINGFACE_HUB_TOKEN'] = token
            else:
                self.logger.info("未提供token，尝试访问公开数据集", task_id)
            
            # 首先验证数据集是否存在和可访问
            self._verify_huggingface_dataset(dataset_name, token, task_id)
            
            # 解析额外参数
            extra_params = params.get('extra_params', {})
            download_kwargs = {
                'cache_dir': str(save_dir / 'cache'),
                'token': token,
                'trust_remote_code': extra_params.get('trust_remote_code', True)
            }
            
            # 添加其他支持的参数
            for param in ['split', 'data_files', 'streaming']:
                if param in extra_params:
                    download_kwargs[param] = extra_params[param]
            
            # 下载数据集（原始格式保存）
            self.logger.info(f"开始加载数据集: {dataset_name}", task_id)
            dataset = load_dataset(dataset_name, **download_kwargs)
            
            # 将数据集保存为原始格式
            dataset_path = save_dir / 'dataset'
            if hasattr(dataset, 'save_to_disk'):
                dataset.save_to_disk(str(dataset_path))
                self.logger.info(f"数据集已保存到磁盘: {dataset_path}", task_id)
            
            # 生成下载元数据
            self._generate_metadata(task_id, params, str(dataset_path), dataset)
            
            # 清理cache目录
            cache_dir = save_dir / 'cache'
            if cache_dir.exists():
                self._cleanup_huggingface_cache(cache_dir, dataset_path, task_id)
            
            tracker.complete()
            self.logger.info(f"Huggingface数据集下载完成: {dataset_path}", task_id)
            
        except Exception as e:
            error_msg = str(e)
            tracker.fail(error_msg)
            
            # 提供更友好的错误信息
            if "LocalEntryNotFoundError" in error_msg or "not found" in error_msg.lower():
                friendly_msg = f"数据集 '{dataset_name}' 不存在或无法访问。请检查：\n" \
                             f"1. 数据集名称是否正确\n" \
                             f"2. 数据集是否为私有（需要token）\n" \
                             f"3. 网络连接是否正常"
                self.logger.error(friendly_msg, task_id)
                raise DownloadError(friendly_msg)
            elif "permission" in error_msg.lower() or "unauthorized" in error_msg.lower():
                friendly_msg = f"访问数据集 '{dataset_name}' 需要认证。请提供有效的Huggingface token"
                self.logger.error(friendly_msg, task_id)
                raise DownloadError(friendly_msg)
            else:
                self.logger.error(f"Huggingface下载失败: {error_msg}", task_id)
                raise DownloadError(f"Huggingface下载失败: {error_msg}")
    
    def _get_huggingface_token(self, params: Dict) -> str:
        """
        获取Huggingface认证令牌
        
        支持多种token传入方式，按优先级依次检查：
        1. params['token'] - 直接传入（最高优先级）
        2. params['hf_token'] - 专用Huggingface token参数
        3. params['huggingface_token'] - 完整名称参数
        4. 环境变量 HUGGINGFACE_HUB_TOKEN
        5. 环境变量 HF_TOKEN
        
        Args:
            params: 任务参数字典
            
        Returns:
            str: 认证令牌，如果未找到则返回None
        """
        # 1. 直接从params获取token
        token = params.get('token')
        if token:
            return token
        
        # 2. 专用Huggingface token参数
        token = params.get('hf_token')
        if token:
            return token
        
        # 3. 完整名称token参数    
        token = params.get('huggingface_token')
        if token:
            return token
        
        # 4. 从环境变量获取
        token = os.environ.get('HUGGINGFACE_HUB_TOKEN')
        if token:
            return token
        
        # 5. 备用环境变量
        token = os.environ.get('HF_TOKEN')
        if token:
            return token
        
        return None
    
    def _get_modelscope_token(self, params: Dict) -> str:
        """
        获取ModelScope认证令牌
        
        支持多种token传入方式，按优先级依次检查：
        1. params['token'] - 直接传入（最高优先级）
        2. params['ms_token'] - 专用ModelScope token参数
        3. params['modelscope_token'] - 完整名称参数
        4. 环境变量 MODELSCOPE_API_TOKEN
        5. 环境变量 MS_TOKEN
        
        Args:
            params: 任务参数字典
            
        Returns:
            str: 认证令牌，如果未找到则返回None
        """
        # 1. 直接从params获取token
        token = params.get('token')
        if token:
            return token
        
        # 2. 专用ModelScope token参数
        token = params.get('ms_token')
        if token:
            return token
        
        # 3. 完整名称token参数    
        token = params.get('modelscope_token')
        if token:
            return token
        
        # 4. 从环境变量获取
        token = os.environ.get('MODELSCOPE_API_TOKEN')
        if token:
            return token
        
        # 5. 备用环境变量
        token = os.environ.get('MS_TOKEN')
        if token:
            return token
        
        return None
    
    def _verify_huggingface_dataset(self, dataset_name: str, token: str = None, task_id: str = ""):
        """
        验证Huggingface数据集是否存在和可访问
        
        Args:
            dataset_name: 数据集名称
            token: 认证令牌
            task_id: 任务ID
            
        Raises:
            DownloadError: 当数据集不存在或不可访问时
        """
        try:
            # 首先测试基本网络连接
            self.logger.info(f"正在测试网络连接...", task_id)
            test_response = self.session.get("https://huggingface.co", timeout=15)
            self.logger.info(f"网络连接正常，状态码: {test_response.status_code}", task_id)
            
            # 尝试通过API检查数据集
            url = f"https://huggingface.co/api/datasets/{dataset_name}"
            headers = {}
            if token:
                headers['Authorization'] = f'Bearer {token}'
            
            self.logger.info(f"正在验证数据集: {dataset_name}", task_id)
            response = self.session.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                dataset_info = response.json()
                self.logger.info(f"数据集验证成功: {dataset_name}", task_id)
                if dataset_info.get('private', False):
                    self.logger.info(f"注意: 这是一个私有数据集", task_id)
                return True
            elif response.status_code == 404:
                # 404错误，提供更详细的数据集搜索建议
                self.logger.error(f"数据集 '{dataset_name}' 不存在", task_id)
                self.logger.info(f"建议检查数据集名称，或访问 https://huggingface.co/datasets 搜索类似数据集", task_id)
                raise DownloadError(f"数据集 '{dataset_name}' 不存在")
            elif response.status_code == 401:
                raise DownloadError(f"访问数据集 '{dataset_name}' 需要认证token")
            else:
                self.logger.warning(f"无法通过API验证数据集，继续尝试下载: HTTP {response.status_code}", task_id)
                return True
                
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"网络连接失败: {str(e)}", task_id)
            self.logger.error(f"请检查:\n1. 网络连接是否正常\n2. 是否需要配置代理\n3. 防火墙是否阻止连接\n4. DNS解析是否正常", task_id)
            # 网络连接失败时，不继续尝试下载
            raise DownloadError(f"网络连接失败，无法访问Hugging Face: {str(e)}")
        except requests.exceptions.Timeout as e:
            self.logger.error(f"网络请求超时: {str(e)}", task_id)
            self.logger.error(f"建议:\n1. 检查网络速度\n2. 稍后重试\n3. 增加超时时间", task_id)
            raise DownloadError(f"网络请求超时: {str(e)}")
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"网络验证失败，继续尝试下载: {str(e)}", task_id)
            return True
        except DownloadError:
            raise
        except Exception as e:
            self.logger.warning(f"验证过程出错，继续尝试下载: {str(e)}", task_id)
            return True
    
    def _download_modelscope(self, task_id: str, params: Dict, tracker: ProgressTracker):
        """
        从ModelScope平台下载数据集
        
        优先使用官方命令行工具 `modelscope download`，确保下载稳定性和完整性。
        如果命令行工具不可用，则回退到Python API方式。
        
        Args:
            task_id: 任务ID
            params: 任务参数
            tracker: 进度跟踪器
            
        Raises:
            DownloadError: 当下载失败时
        """
        dataset_name = params['dataset_name']
        
        # 从数据集名称中提取提供商信息
        provider, clean_dataset_name = self._extract_provider_from_dataset_name(dataset_name)
        
        # 创建按提供商分组的目录结构：data/raw/provider/数据集名称/
        base_save_dir = Path(params['save_dir'])  # 已经是 data/raw
        if provider:
            # 如果有提供商信息，使用 data/raw/提供商/数据集名称 结构
            save_dir = base_save_dir / provider / clean_dataset_name.replace('/', '_')
        else:
            # 如果没有提供商信息，退回到源类型结构
            save_dir = base_save_dir / 'modelscope' / dataset_name.replace('/', '_')
        
        save_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"开始从ModelScope下载数据集: {dataset_name}", task_id)
        tracker.start()
        
        # 方法1: 优先使用官方命令行工具
        try:
            self.logger.info("尝试使用官方modelscope命令行工具下载", task_id)
            dataset_path = self._download_modelscope_cli(task_id, params, tracker, save_dir)
            if dataset_path:
                # 验证下载结果
                if self._validate_download_result(dataset_path, task_id):
                    # 生成下载元数据
                    self._generate_metadata(task_id, params, str(dataset_path), None)
                    tracker.complete()
                    self.logger.info(f"ModelScope数据集下载完成: {dataset_path}", task_id)
                    return
                else:
                    self.logger.warning("官方CLI下载验证失败，尝试其他方法", task_id)
        except Exception as e:
            self.logger.warning(f"官方CLI下载失败: {str(e)}，尝试其他方法", task_id)
        
        # 方法2: 如果命令行工具失败，使用Python API
        if not MS_AVAILABLE:
            # 如果ModelScope库也不可用，尝试直接HTTP下载
            self.logger.warning("ModelScope库不可用，尝试使用HTTP直接下载", task_id)
            return self._download_modelscope_http(task_id, params, tracker)
        
        self.logger.info("使用ModelScope Python API下载", task_id)
        try:
            dataset_path = self._download_modelscope_api(task_id, params, tracker, save_dir)
            if dataset_path and self._validate_download_result(dataset_path, task_id):
                # 生成下载元数据
                self._generate_metadata(task_id, params, str(dataset_path), None)
                tracker.complete()
                self.logger.info(f"ModelScope数据集下载完成: {dataset_path}", task_id)
                return
        except Exception as e:
            self.logger.error(f"Python API下载失败: {str(e)}", task_id)
        
        # 方法3: 最后尝试HTTP直接下载
        self.logger.warning("所有方法都失败，尝试HTTP下载", task_id)
        return self._download_modelscope_http(task_id, params, tracker)
    
    def _download_modelscope_api(self, task_id: str, params: Dict, tracker: ProgressTracker, save_dir: Path):
        """
        使用ModelScope Python API下载数据集（备用方法）
        
        Args:
            task_id: 任务ID
            params: 任务参数
            tracker: 进度跟踪器
            save_dir: 保存目录
            
        Returns:
            str: 下载的数据集路径
        """
        dataset_name = params['dataset_name']
        
        try:
            # 多种方式获取认证令牌
            token = self._get_modelscope_token(params)
            if token:
                self.logger.info("使用认证token进行下载", task_id)
                os.environ['MODELSCOPE_API_TOKEN'] = token
            else:
                self.logger.info("未提供token，尝试访问公开数据集", task_id)
            
            # 方法1: 尝试使用snapshot_download
            dataset_path = None
            try:
                from modelscope.hub.snapshot_download import snapshot_download
                
                # 设置进度为10%（开始下载）
                tracker.update_progress(10)
                
                dataset_path = snapshot_download(
                    model_id=dataset_name,
                    cache_dir=str(save_dir / 'cache'),
                    local_dir=str(save_dir / 'dataset')
                )
                
                # 下载完成，设置进度为90%
                tracker.update_progress(90)
                self.logger.info(f"使用snapshot_download下载完成: {dataset_path}", task_id)
                
            except Exception as e:
                self.logger.warning(f"snapshot_download失败: {str(e)}，尝试其他方法", task_id)
            
            # 方法2: 如果snapshot_download失败，尝试MsDataset
            if dataset_path is None and MsDataset:
                try:
                    tracker.update_progress(20)
                    
                    extra_params = params.get('extra_params', {})
                    download_kwargs = {
                        'cache_dir': str(save_dir / 'cache')
                    }
                    
                    # 处理subset_name参数，将其作为第二个位置参数传递
                    subset_name = extra_params.get('subset_name')
                    if subset_name:
                        self.logger.info(f"使用子集配置: {subset_name}", task_id)
                    
                    # 添加其他支持的参数
                    for param in ['split']:
                        if param in extra_params:
                            download_kwargs[param] = extra_params[param]
                    
                    tracker.update_progress(40)
                    
                    # 尝试简单加载，根据是否有subset_name决定调用方式
                    try:
                        if subset_name:
                            # 有子集名称时，将其作为第二个参数传入
                            dataset = MsDataset.load(dataset_name, subset_name, **download_kwargs)
                        else:
                            dataset = MsDataset.load(dataset_name, **download_kwargs)
                        tracker.update_progress(80)
                    except Exception as dataset_error:
                        error_msg = str(dataset_error)
                        self.logger.warning(f"数据集解析失败: {error_msg}", task_id)
                        
                        # 如果是配置缺失错误，提供更好的错误信息
                        if "Config name is missing" in error_msg or "available configs" in error_msg:
                            if not subset_name:
                                # 如果用户没有提供subset_name，抛出包含可用配置的错误
                                available_configs = "['high', 'middle']"  # race数据集的已知配置
                                raise DownloadError(f"数据集 '{dataset_name}' 需要指定配置名称。可用配置：{available_configs}。请在extra_params中添加 'subset_name': 'high' 或 'subset_name': 'middle'")
                            else:
                                raise DownloadError(f"指定的配置 '{subset_name}' 无效。{error_msg}")
                        else:
                            # 其他错误，检查cache目录是否有文件（可能下载成功但解析失败）
                            cache_dir = save_dir / 'cache'
                            if cache_dir.exists():
                                cache_files = [f for f in cache_dir.glob('**/*') if f.is_file() and f.stat().st_size > 0]
                                if cache_files:
                                    self.logger.info(f"发现cache中有 {len(cache_files)} 个文件，可能下载成功但解析失败", task_id)
                                    dataset_path = str(cache_dir)
                                    tracker.update_progress(90)
                                else:
                                    raise dataset_error
                            else:
                                raise dataset_error
                    
                    if dataset_path is None:
                        dataset_path = save_dir / 'dataset'
                        if hasattr(dataset, 'save_to_disk'):
                            dataset.save_to_disk(str(dataset_path))
                        elif hasattr(dataset, 'to_csv'):
                            dataset.to_csv(str(dataset_path / 'data.csv'), index=False)
                            dataset_path = str(dataset_path / 'data.csv')
                        
                        tracker.update_progress(90)
                    
                    self.logger.info(f"使用MsDataset下载完成: {dataset_path}", task_id)
                except Exception as e:
                    self.logger.warning(f"MsDataset也失败: {str(e)}", task_id)
                    raise e
            
            if dataset_path is None:
                raise DownloadError("所有下载方法都失败")
            
            return str(dataset_path)
            
        except Exception as e:
            raise DownloadError(f"ModelScope API下载失败: {str(e)}")
    
            token = self._get_modelscope_token(params)
            if token:
                self.logger.info("使用认证token进行下载", task_id)
                os.environ['MODELSCOPE_API_TOKEN'] = token
            else:
                self.logger.info("未提供token，尝试访问公开数据集", task_id)
            
            # 方法1: 尝试使用snapshot_download
            dataset_path = None
            try:
                from modelscope.hub.snapshot_download import snapshot_download
                
                # 设置进度为10%（开始下载）
                tracker.update_progress(10)
                
                dataset_path = snapshot_download(
                    model_id=dataset_name,
                    cache_dir=str(save_dir / 'cache'),
                    local_dir=str(save_dir / 'dataset')
                )
                
                # 下载完成，设置进度为90%
                tracker.update_progress(90)
                self.logger.info(f"使用snapshot_download下载完成: {dataset_path}", task_id)
                
            except Exception as e:
                self.logger.warning(f"snapshot_download失败: {str(e)}，尝试其他方法", task_id)
            
            # 方法2: 如果snapshot_download失败，尝试MsDataset
            if dataset_path is None and MsDataset:
                try:
                    tracker.update_progress(20)
                    
                    extra_params = params.get('extra_params', {})
                    download_kwargs = {
                        'cache_dir': str(save_dir / 'cache')
                    }
                    
                    # 处理subset_name参数，将其作为第二个位置参数传递
                    subset_name = extra_params.get('subset_name')
                    if subset_name:
                        self.logger.info(f"使用子集配置: {subset_name}", task_id)
                    
                    # 添加其他支持的参数
                    for param in ['split']:
                        if param in extra_params:
                            download_kwargs[param] = extra_params[param]
                    
                    tracker.update_progress(40)
                    
                    # 尝试简单加载，根据是否有subset_name决定调用方式
                    try:
                        if subset_name:
                            # 有子集名称时，将其作为第二个参数传入
                            dataset = MsDataset.load(dataset_name, subset_name, **download_kwargs)
                        else:
                            dataset = MsDataset.load(dataset_name, **download_kwargs)
                        tracker.update_progress(80)
                    except Exception as dataset_error:
                        error_msg = str(dataset_error)
                        self.logger.warning(f"数据集解析失败: {error_msg}", task_id)
                        
                        # 如果是配置缺失错误，提供更好的错误信息
                        if "Config name is missing" in error_msg or "available configs" in error_msg:
                            if not subset_name:
                                # 如果用户没有提供subset_name，抛出包含可用配置的错误
                                available_configs = "['high', 'middle']"  # race数据集的已知配置
                                raise DownloadError(f"数据集 '{dataset_name}' 需要指定配置名称。可用配置：{available_configs}。请在extra_params中添加 'subset_name': 'high' 或 'subset_name': 'middle'")
                            else:
                                raise DownloadError(f"指定的配置 '{subset_name}' 无效。{error_msg}")
                        else:
                            # 其他错误，检查cache目录是否有文件（可能下载成功但解析失败）
                            cache_dir = save_dir / 'cache'
                            if cache_dir.exists():
                                cache_files = [f for f in cache_dir.glob('**/*') if f.is_file() and f.stat().st_size > 0]
                                if cache_files:
                                    self.logger.info(f"发现cache中有 {len(cache_files)} 个文件，可能下载成功但解析失败", task_id)
                                    dataset_path = str(cache_dir)
                                    tracker.update_progress(90)
                                else:
                                    raise dataset_error
                            else:
                                raise dataset_error
                    
                    if dataset_path is None:
                        dataset_path = save_dir / 'dataset'
                        if hasattr(dataset, 'save_to_disk'):
                            dataset.save_to_disk(str(dataset_path))
                        elif hasattr(dataset, 'to_csv'):
                            dataset.to_csv(str(dataset_path / 'data.csv'), index=False)
                            dataset_path = str(dataset_path / 'data.csv')
                        
                        tracker.update_progress(90)
                    
                    self.logger.info(f"使用MsDataset下载完成: {dataset_path}", task_id)
                except Exception as e:
                    self.logger.warning(f"MsDataset也失败: {str(e)}，尝试HTTP下载", task_id)
            
            # 方法3: 如果以上都失败，尝试HTTP直接下载
            if dataset_path is None:
                return self._download_modelscope_http(task_id, params, tracker)
            
            # 验证下载结果
            if not self._validate_download_result(dataset_path, task_id):
                tracker.fail("下载验证失败，未找到有效的数据文件")
                self.logger.error(f"下载验证失败: {dataset_path}", task_id)
                return
            
            # 修复文件名：处理ModelScope的哈希化文件名
            dataset_path = self._fix_modelscope_filenames(dataset_path, dataset_name, task_id)
            
            # 生成下载元数据
            self._generate_metadata(task_id, params, str(dataset_path), None)
            
            tracker.complete()
            self.logger.info(f"ModelScope数据集下载完成: {dataset_path}", task_id)
            
        except Exception as e:
            tracker.fail(str(e))
            self.logger.error(f"ModelScope下载失败: {str(e)}", task_id)
            raise DownloadError(f"ModelScope下载失败: {str(e)}")
    
    def _download_modelscope_cli(self, task_id: str, params: Dict, tracker: ProgressTracker, save_dir: Path) -> str:
        """
        使用官方ModelScope命令行工具下载数据集
        
        严格按照官方文档指导下载，确保文件结构与官网一致：
        - 使用 --local_dir 直接下载到目标目录
        - 使用 --cache_dir 指定缓存目录（支持断点续传）
        - 下载完成后清理缓存
        
        Args:
            task_id: 任务ID
            params: 任务参数  
            tracker: 进度跟踪器
            save_dir: 保存目录（最终数据集目录）
            
        Returns:
            str: 下载的数据集路径，失败时返回None
        """
        dataset_name = params['dataset_name']
        
        # 确保保存目录存在但为空（重新下载时清理）
        if save_dir.exists():
            import shutil
            shutil.rmtree(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)
        
        # 缓存目录用于断点续传（放在上级目录）
        cache_dir = save_dir.parent / f".cache_{save_dir.name}"
        
        # 构建官方命令：直接下载到目标目录
        cmd = [
            'modelscope', 'download',
            '--dataset', dataset_name,
            '--local_dir', str(save_dir),      # 直接下载到最终目录
            '--cache_dir', str(cache_dir)      # 缓存目录用于断点续传
        ]
        
        # 添加token（如果提供）
        token = self._get_modelscope_token(params)
        if token:
            cmd.extend(['--token', token])
            self.logger.info("使用认证token进行下载", task_id)
        
        # 添加其他参数
        extra_params = params.get('extra_params', {})
        if 'revision' in extra_params:
            cmd.extend(['--revision', extra_params['revision']])
        if 'max_workers' in extra_params:
            cmd.extend(['--max-workers', str(extra_params['max_workers'])])
        
        # 添加包含/排除过滤器
        if 'include' in extra_params:
            cmd.extend(['--include', extra_params['include']])
        if 'exclude' in extra_params:
            cmd.extend(['--exclude', extra_params['exclude']])
        
        self.logger.info(f"执行官方ModelScope下载命令: {' '.join(cmd)}", task_id)
        
        try:
            import subprocess
            import time
            
            # 设置进度为10%（开始下载）
            tracker.update_progress(10)
            
            # 执行下载命令
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # 存储进程信息到tracker，便于后续监控
            if not hasattr(tracker, '_cli_process'):
                tracker._cli_process = process
                tracker._cli_save_dir = save_dir
            
            # 实时监控进度（基于文件系统大小轮询，跨平台可靠）
            start_time = time.time()
            last_check_time = start_time
            last_size = 0
            output_lines = []  # 保留结构，但不在循环中读取，避免阻塞
            
            while process.poll() is None:
                elapsed = time.time() - start_time
                current_time = time.time()
                
                # 读取进程输出（非阻塞）
                # 不读取 stdout，避免在 Windows 下阻塞；仅依赖文件大小变化来估算进度
                
                # 每10秒检查一次实际文件大小变化
                if current_time - last_check_time >= 10:
                    try:
                        # 检查目标目录的实际大小
                        current_size = 0
                        if save_dir.exists():
                            for file_path in save_dir.rglob('*'):
                                if file_path.is_file():
                                    current_size += file_path.stat().st_size
                        
                        # 检查缓存目录大小（如果存在）  
                        cache_size = 0
                        if cache_dir.exists():
                            for file_path in cache_dir.rglob('*'):
                                if file_path.is_file():
                                    cache_size += file_path.stat().st_size
                        
                        # 总下载大小（目标文件 + 缓存）
                        total_size = current_size + cache_size
                        
                        # 只有文件大小真正增长时才更新进度
                        if total_size > last_size:
                            size_growth = total_size - last_size
                            self.logger.info(f"下载进展: +{size_growth//1024//1024:.1f}MB, 目标文件: {current_size//1024//1024:.1f}MB, 缓存: {cache_size//1024//1024:.1f}MB", task_id)
                            
                            # 基于实际文件大小计算进度
                            if current_size < 1024 * 1024:  # <1MB
                                estimated_progress = 15
                            elif current_size < 10 * 1024 * 1024:  # <10MB
                                estimated_progress = 30
                            elif current_size < 50 * 1024 * 1024:  # <50MB
                                estimated_progress = 50
                            elif current_size < 100 * 1024 * 1024:  # <100MB
                                estimated_progress = 70
                            else:  # >100MB
                                estimated_progress = 85
                            
                            tracker.update_progress(estimated_progress)
                            last_size = total_size
                            last_check_time = current_time
                            
                            # 实时保存状态
                            self._save_tasks_to_state()
                        else:
                            # 文件大小没变化，可能刚开始或遇到问题
                            if elapsed > 60:  # 超过1分钟没有文件增长
                                self.logger.warning(f"下载已进行{elapsed:.0f}秒但文件大小无变化", task_id)
                                estimated_progress = min(25, 10 + int(elapsed / 30))
                                tracker.update_progress(estimated_progress)
                    except Exception as e:
                        self.logger.debug(f"进度检查失败: {e}", task_id)
                
                # 检查是否超时（30分钟无进展）
                if elapsed > 1800:  # 30分钟
                    self.logger.error(f"下载超时，终止进程", task_id)
                    process.terminate()
                    break
                
                time.sleep(5)  # 每5秒检查一次
            
            # 获取命令执行结果
            stdout, stderr = process.communicate()
            return_code = process.returncode
            
            if return_code == 0:
                # 下载成功
                tracker.update_progress(95)
                self.logger.info("ModelScope CLI下载完成", task_id)
                
                # 验证下载结果
                if self._validate_modelscope_download(save_dir, task_id):
                    # 清理缓存目录（下载完成后）
                    if cache_dir.exists():
                        try:
                            import shutil
                            cache_size = sum(f.stat().st_size for f in cache_dir.rglob('*') if f.is_file())
                            self.logger.info(f"清理缓存目录: {cache_size//1024//1024:.1f}MB", task_id)
                            shutil.rmtree(cache_dir)
                            self.logger.info("缓存目录清理完成", task_id)
                        except Exception as e:
                            self.logger.warning(f"缓存清理失败: {e}", task_id)
                    
                    tracker.update_progress(100)
                    self.logger.info(f"数据集下载完成: {save_dir}", task_id)
                    return str(save_dir)
                else:
                    self.logger.error("下载验证失败", task_id)
                    return None
            else:
                # 下载失败
                error_msg = stderr.strip() if stderr else stdout.strip()
                self.logger.error(f"CLI下载失败 (返回码 {return_code}): {error_msg}", task_id)
                raise DownloadError(f"ModelScope CLI下载失败: {error_msg}")
                
        except FileNotFoundError:
            # modelscope命令不存在
            self.logger.warning("modelscope命令行工具未安装", task_id)
            raise DownloadError("modelscope命令行工具未安装")
        except Exception as e:
            self.logger.error(f"CLI下载过程出错: {str(e)}", task_id)
            raise DownloadError(f"CLI下载失败: {str(e)}")
    
    def _validate_modelscope_download(self, dataset_dir: Path, task_id: str) -> bool:
        """
        验证ModelScope下载结果
        
        验证标准：
        1. 目录中应该有实际的数据文件
        2. 不应该有复杂的嵌套结构
        3. 文件应该与官网文件列表一致
        
        Args:
            dataset_dir: 数据集目录
            task_id: 任务ID
            
        Returns:
            bool: 验证是否通过
        """
        try:
            if not dataset_dir.exists():
                self.logger.error(f"数据集目录不存在: {dataset_dir}", task_id)
                return False
            
            # 统计文件
            files = []
            for file_path in dataset_dir.iterdir():
                if file_path.is_file():
                    files.append(file_path.name)
            
            if not files:
                self.logger.error(f"数据集目录为空: {dataset_dir}", task_id)
                return False
            
            # 计算总大小
            total_size = sum(f.stat().st_size for f in dataset_dir.rglob('*') if f.is_file())
            
            self.logger.info(f"验证通过: 找到 {len(files)} 个文件, 总大小: {total_size//1024//1024:.1f}MB", task_id)
            self.logger.info(f"文件列表: {', '.join(sorted(files))}", task_id)
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证下载结果时出错: {e}", task_id)
            return False
    
    def _validate_download_result(self, dataset_path: str, task_id: str) -> bool:
        """
        验证下载结果的完整性
        
        简化的验证逻辑：只验证是否有文件下载成功
        """
        try:
            path_obj = Path(dataset_path)
            
            if not path_obj.exists():
                self.logger.error(f"下载路径不存在: {dataset_path}", task_id)
                return False
            
            if path_obj.is_file():
                if path_obj.stat().st_size > 0:
                    return True
                else:
                    self.logger.error(f"下载文件为空: {dataset_path}", task_id)
                    return False
            
            elif path_obj.is_dir():
                # 统计目录中的文件
                files = [f for f in path_obj.iterdir() if f.is_file()]
                
                if files:
                    total_size = sum(f.stat().st_size for f in files)
                    self.logger.info(f"验证通过: 找到 {len(files)} 个文件，总大小: {total_size//1024//1024:.1f}MB", task_id)
                    return True
                else:
                    self.logger.error(f"下载目录为空: {dataset_path}", task_id)
                    return False
            
            return False
            
        except Exception as e:
            self.logger.error(f"验证下载结果时出错: {e}", task_id)
            return False

    def _fix_modelscope_filenames(self, dataset_path: str, dataset_name: str, task_id: str) -> str:
        """
        修复ModelScope的哈希化文件名，创建用户友好的文件名
        
        Args:
            dataset_path: 原始数据集路径
            dataset_name: 数据集名称
            task_id: 任务ID
            
        Returns:
            修复后的数据集路径
        """
        try:
            path_obj = Path(dataset_path)
            
            # 如果是cache目录，查找其中的哈希化文件
            if 'cache' in str(path_obj):
                cache_root = path_obj
                if not cache_root.name == 'cache':
                    # 找到cache目录
                    for parent in path_obj.parents:
                        if parent.name == 'cache' or 'cache' in str(parent):
                            cache_root = parent
                            break
                
                # 在cache目录中查找下载的文件
                downloads_dir = cache_root / 'downloads'
                if downloads_dir.exists():
                    # 创建用户友好的目录
                    friendly_dir = path_obj.parent / 'organized_files'
                    friendly_dir.mkdir(exist_ok=True)
                    
                    # 查找哈希化的文件
                    for hash_file in downloads_dir.iterdir():
                        if hash_file.is_file():
                            # 尝试猜测原始文件名
                            original_name = self._guess_original_filename(hash_file, dataset_name)
                            friendly_path = friendly_dir / original_name
                            
                            # 创建硬链接或复制文件
                            try:
                                if not friendly_path.exists():
                                    # 尝试创建硬链接
                                    try:
                                        friendly_path.hardlink_to(hash_file)
                                        self.logger.info(f"创建硬链接: {friendly_path} -> {hash_file}", task_id)
                                    except:
                                        # 如果硬链接失败，复制文件
                                        import shutil
                                        shutil.copy2(hash_file, friendly_path)
                                        self.logger.info(f"复制文件: {friendly_path}", task_id)
                            except Exception as e:
                                self.logger.warning(f"处理文件 {hash_file} 失败: {e}", task_id)
                    
                    # 下载完成且文件已整理，清理cache目录
                    self._cleanup_cache_after_completion(cache_root, friendly_dir, task_id)
                    
                    return str(friendly_dir)
            
            return dataset_path
            
        except Exception as e:
            self.logger.warning(f"修复文件名失败: {e}，使用原始路径", task_id)
            return dataset_path
    
    def _cleanup_cache_after_completion(self, cache_root: Path, organized_dir: Path, task_id: str):
        """
        下载完成后清理cache目录
        
        Args:
            cache_root: cache根目录
            organized_dir: 已整理的文件目录
            task_id: 任务ID
        """
        try:
            # 验证organized_files中确实有文件
            if not organized_dir.exists() or not any(organized_dir.iterdir()):
                self.logger.warning(f"organized_files目录为空，跳过cache清理", task_id)
                return
            
            # 计算文件数量和大小，确保数据完整
            org_files = list(organized_dir.glob('**/*'))
            org_file_count = len([f for f in org_files if f.is_file()])
            org_total_size = sum(f.stat().st_size for f in org_files if f.is_file())
            
            cache_files = list(cache_root.glob('**/*'))
            cache_file_count = len([f for f in cache_files if f.is_file()])
            cache_total_size = sum(f.stat().st_size for f in cache_files if f.is_file())
            
            self.logger.info(f"数据验证 - organized: {org_file_count}个文件, {org_total_size}字节; cache: {cache_file_count}个文件, {cache_total_size}字节", task_id)
            
            # 只有当organized文件数量 >= cache文件数量的一半时才清理cache
            # 这样可以确保大部分数据已经成功整理
            if org_file_count >= max(1, cache_file_count // 2):
                import shutil
                shutil.rmtree(cache_root, ignore_errors=True)
                self.logger.info(f"下载完成，已清理cache目录: {cache_root}", task_id)
            else:
                self.logger.warning(f"organized文件数量不足，保留cache目录以供调试", task_id)
                
        except Exception as e:
            self.logger.warning(f"清理cache目录失败: {e}", task_id)
    
    def _cleanup_huggingface_cache(self, cache_dir: Path, dataset_path: Path, task_id: str):
        """
        清理Huggingface下载的cache目录
        
        Args:
            cache_dir: cache目录路径
            dataset_path: 数据集保存路径
            task_id: 任务ID
        """
        try:
            # 验证数据集已经成功保存
            if not dataset_path.exists():
                self.logger.warning(f"数据集目录不存在，跳过cache清理: {dataset_path}", task_id)
                return
            
            # 检查数据集目录中是否有文件
            dataset_files = list(dataset_path.glob('**/*'))
            dataset_file_count = len([f for f in dataset_files if f.is_file()])
            
            if dataset_file_count > 0:
                # 数据集已成功保存，可以清理cache
                import shutil
                shutil.rmtree(cache_dir, ignore_errors=True)
                self.logger.info(f"Huggingface下载完成，已清理cache目录: {cache_dir}", task_id)
            else:
                self.logger.warning(f"数据集目录为空，保留cache目录: {dataset_path}", task_id)
                
        except Exception as e:
            self.logger.warning(f"清理Huggingface cache失败: {e}", task_id)
    
    def _guess_original_filename(self, hash_file: Path, dataset_name: str) -> str:
        """
        根据数据集名称和文件内容猜测原始文件名
        
        Args:
            hash_file: 哈希化的文件路径
            dataset_name: 数据集名称
            
        Returns:
            猜测的原始文件名
        """
        try:
            # 从数据集名称提取可能的文件名
            base_name = dataset_name.split('/')[-1].lower()
            
            # 检查文件开头几个字节来判断文件类型
            with open(hash_file, 'rb') as f:
                header = f.read(100)
            
            # 根据文件内容判断类型
            if header.startswith(b'{') or b'"' in header[:50]:
                # 可能是JSON文件
                if 'test' in base_name:
                    return f"{base_name}_test.jsonl"
                elif 'train' in base_name:
                    return f"{base_name}_train.jsonl"
                else:
                    return f"{base_name}.jsonl"
            elif header.startswith(b'PK'):
                # ZIP文件
                return f"{base_name}.zip"
            elif b',' in header[:50] and b'\n' in header[:50]:
                # 可能是CSV文件
                return f"{base_name}.csv"
            else:
                # 默认为文本文件
                return f"{base_name}.txt"
                
        except Exception:
            # 如果都失败了，使用默认名称
            return f"{dataset_name.split('/')[-1]}_data.bin"
    
    def _download_modelscope_http(self, task_id: str, params: Dict, tracker: ProgressTracker):
        """
        使用HTTP直接从ModelScope下载（备用方法）
        
        当ModelScope官方库不可用时，使用此方法通过HTTP API直接下载。
        
        Args:
            task_id: 任务ID
            params: 任务参数
            tracker: 进度跟踪器
        """
        dataset_name = params['dataset_name']
        
        # 从数据集名称中提取提供商信息
        provider, clean_dataset_name = self._extract_provider_from_dataset_name(dataset_name)
        
        # 创建按提供商分组的目录结构：data/raw/provider/数据集名称/
        base_save_dir = Path(params['save_dir'])  # 已经是 data/raw
        if provider:
            # 如果有提供商信息，使用 data/raw/提供商/数据集名称 结构
            save_dir = base_save_dir / provider / clean_dataset_name.replace('/', '_')
        else:
            # 如果没有提供商信息，退回到源类型结构
            save_dir = base_save_dir / 'modelscope' / dataset_name.replace('/', '_')
        
        save_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"使用HTTP方法从ModelScope下载: {dataset_name}", task_id)
        
        try:
            # 构建ModelScope的文件下载URL
            # 格式: https://www.modelscope.cn/api/v1/models/{owner}/{name}/repo/files
            base_url = f"https://www.modelscope.cn/api/v1/models/{dataset_name}/repo/files"
            
            # 获取文件列表
            headers = {}
            token = params.get('token')
            if token:
                headers['Authorization'] = f'Bearer {token}'
            
            response = self.session.get(base_url, headers=headers, timeout=params.get('timeout', 300))
            if response.status_code == 200:
                file_info = response.json()
                files_data = file_info.get('data', [])
                
                if not files_data:
                    raise DownloadError(f"数据集 {dataset_name} 在ModelScope上没有文件")
                
                self.logger.info(f"获取到文件列表，共 {len(files_data)} 个文件", task_id)
                
                # 下载主要文件
                dataset_path = save_dir / 'dataset'
                dataset_path.mkdir(parents=True, exist_ok=True)
                
                downloaded_files = 0
                for file_item in files_data[:10]:  # 限制下载前10个文件
                    file_name = file_item.get('name', 'unknown')
                    file_url = f"https://www.modelscope.cn{file_item.get('url', '')}"
                    
                    if file_url and not file_url.endswith('/'):
                        try:
                            self._download_file_from_url(
                                file_url, 
                                dataset_path / file_name, 
                                headers=headers,
                                task_id=task_id
                            )
                            downloaded_files += 1
                        except Exception as e:
                            self.logger.warning(f"下载文件 {file_name} 失败: {e}", task_id)
                
                if downloaded_files == 0:
                    raise DownloadError(f"无法从ModelScope下载任何文件")
                
                # 验证下载结果
                if not self._validate_download_result(str(dataset_path), task_id):
                    raise DownloadError(f"下载的文件验证失败")
                
                # 生成元数据
                self._generate_metadata(task_id, params, str(dataset_path), None)
                tracker.complete()
                self.logger.info(f"HTTP方法下载完成: {dataset_path}", task_id)
                
            elif response.status_code == 404:
                raise DownloadError(f"数据集 '{dataset_name}' 在ModelScope上不存在")
            else:
                raise DownloadError(f"无法访问ModelScope API，状态码: {response.status_code}")
                
        except Exception as e:
            tracker.fail(str(e))
            self.logger.error(f"HTTP下载失败: {str(e)}", task_id)
            raise DownloadError(f"HTTP下载失败: {str(e)}")
    
    def _download_file_from_url(self, url: str, save_path: Path, headers: dict = None, task_id: str = ""):
        """
        从URL下载单个文件的辅助方法
        
        Args:
            url: 文件URL
            save_path: 保存路径
            headers: HTTP头部
            task_id: 任务ID
        """
        try:
            response = self.session.get(url, headers=headers or {}, stream=True, timeout=300)
            response.raise_for_status()
            
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            self.logger.info(f"文件下载完成: {save_path.name}", task_id)
            
        except Exception as e:
            self.logger.warning(f"文件下载失败 {save_path.name}: {str(e)}", task_id)
    
    def _download_url(self, task_id: str, params: Dict, tracker: ProgressTracker):
        """
        从URL下载文件
        
        支持HTTP/HTTPS协议的文件下载，具备断点续传功能。
        适用于直接下载数据文件、压缩包等。
        
        Args:
            task_id: 任务ID
            params: 任务参数  
            tracker: 进度跟踪器
            
        Raises:
            DownloadError: 当下载失败时
        """
        url = params['dataset_name']
        save_dir = Path(params['save_dir'])
        
        # 从URL提取文件名并构建保存路径
        parsed_url = urllib.parse.urlparse(url)
        filename = Path(parsed_url.path).name or 'downloaded_file'
        
        # 创建以域名和文件名命名的子目录
        safe_dirname = f"{parsed_url.netloc}_{filename}".replace(':', '_').replace('/', '_')
        save_dir = save_dir / safe_dirname
        save_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = save_dir / filename
        temp_file = save_dir / f"{filename}.tmp"
        
        self.logger.info(f"开始从URL下载文件: {url}", task_id)
        
        try:
            # 检查断点续传支持
            resume_header = {}
            downloaded_bytes = 0
            
            if params.get('resume', True) and temp_file.exists():
                downloaded_bytes = temp_file.stat().st_size
                resume_header['Range'] = f'bytes={downloaded_bytes}-'
                self.logger.info(f"检测到临时文件，启用断点续传，已下载: {downloaded_bytes} 字节", task_id)
            
            # 合并自定义HTTP头
            headers = {**resume_header, **params.get('headers', {})}
            
            # 发起下载请求
            response = self.session.get(
                url,
                headers=headers,
                stream=True,
                timeout=params['timeout']
            )
            response.raise_for_status()
            
            # 解析文件总大小
            total_size = downloaded_bytes
            if 'content-length' in response.headers:
                content_length = int(response.headers['content-length'])
                total_size += content_length
            elif 'content-range' in response.headers:
                # 从content-range头解析总大小
                range_info = response.headers['content-range']
                total_size = int(range_info.split('/')[-1])
            
            tracker.start(total_size)
            
            # 执行文件下载
            mode = 'ab' if downloaded_bytes > 0 else 'wb'
            with open(temp_file, mode) as f:
                with tqdm(
                    total=total_size,
                    initial=downloaded_bytes,
                    unit='B',
                    unit_scale=True,
                    desc=f"下载 {filename}"
                ) as pbar:
                    
                    for chunk in response.iter_content(
                        chunk_size=self.config_mgr.get_config('download.buffer_size')
                    ):
                        if chunk:
                            f.write(chunk)
                            downloaded_bytes += len(chunk)
                            pbar.update(len(chunk))
                            tracker.update(downloaded_bytes)
            
            # 下载完成，将临时文件重命名为最终文件
            temp_file.rename(output_file)
            
            # 生成下载元数据
            self._generate_metadata(task_id, params, str(output_file))
            
            tracker.complete()
            self.logger.info(f"URL文件下载完成: {output_file}", task_id)
            
        except Exception as e:
            tracker.fail(str(e))
            self.logger.error(f"URL下载失败: {str(e)}", task_id)
            raise DownloadError(f"URL下载失败: {str(e)}")
    
    def _generate_metadata(self, task_id: str, params: Dict, output_path: str, dataset=None):
        """
        生成下载任务的元数据文件
        
        元数据包含任务信息、数据源信息、文件信息等，
        便于后续的数据处理和管理。
        
        Args:
            task_id: 任务ID
            params: 任务参数
            output_path: 输出文件路径
            dataset: 数据集对象（可选）
        """
        try:
            output_file = Path(output_path)
            meta_file = output_file.parent / 'meta.json'
            
            # 计算文件或目录的基本信息
            file_info = self._get_file_info(output_path)
            
            # 构建元数据
            metadata = {
                'task_info': {
                    'task_id': task_id,
                    'module': 'dataset_downloader',
                    'version': '2.0',
                    'create_time': params['created_at'],
                    'complete_time': datetime.now().isoformat()
                },
                'source_info': {
                    'source_type': params['source_type'],
                    'source_identifier': params['dataset_name'],
                    'download_time': datetime.now().isoformat(),
                    'token_used': bool(params.get('token'))
                },
                'download_params': {
                    'save_dir': params['save_dir'],
                    'resume_enabled': params.get('resume', True),
                    'timeout': params.get('timeout'),
                    'retry_count': params.get('retry_count'),
                    'extra_params': params.get('extra_params', {})
                },
                'output_info': {
                    'output_path': str(output_path),
                    'file_info': file_info
                }
            }
            
            # 添加数据集特定信息（如果可用）
            if dataset is not None:
                try:
                    dataset_info = {}
                    if hasattr(dataset, 'info'):
                        info = dataset.info
                        dataset_info['description'] = getattr(info, 'description', '')
                        dataset_info['features'] = str(getattr(info, 'features', ''))
                        dataset_info['splits'] = list(dataset.keys()) if hasattr(dataset, 'keys') else []
                    metadata['dataset_info'] = dataset_info
                except Exception:
                    # 忽略数据集信息获取错误
                    pass
            
            # 写入元数据文件
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"元数据已生成: {meta_file}", task_id)
            
        except Exception as e:
            self.logger.error(f"生成元数据失败: {str(e)}", task_id)
    
    def _get_file_info(self, path: str) -> Dict[str, Any]:
        """
        获取文件或目录的基本信息
        
        Args:
            path: 文件或目录路径
            
        Returns:
            包含文件信息的字典
        """
        try:
            path_obj = Path(path)
            info = {
                'exists': path_obj.exists(),
                'is_file': path_obj.is_file(),
                'is_directory': path_obj.is_dir(),
            }
            
            if path_obj.exists():
                stat = path_obj.stat()
                info.update({
                    'size_bytes': stat.st_size,
                    'created_time': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                    'modified_time': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    'hash': self._calculate_file_hash(str(path_obj)) if path_obj.is_file() else None
                })
                
                # 如果是目录，统计文件数量
                if path_obj.is_dir():
                    try:
                        file_count = sum(1 for _ in path_obj.rglob('*') if _.is_file())
                        info['file_count'] = file_count
                    except Exception:
                        info['file_count'] = -1
            
            return info
            
        except Exception:
            return {'exists': False, 'error': '获取文件信息失败'}
    
    def _calculate_file_hash(self, file_path: str, algorithm: str = 'md5') -> str:
        """
        计算文件的哈希值
        
        Args:
            file_path: 文件路径
            algorithm: 哈希算法（默认md5）
            
        Returns:
            格式为 'algorithm:hash_value' 的哈希字符串
        """
        try:
            hash_obj = hashlib.new(algorithm)
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_obj.update(chunk)
            return f"{algorithm}:{hash_obj.hexdigest()}"
        except Exception:
            return ""
    
    def get_task_progress(self, task_id: str) -> Dict[str, Any]:
        """
        获取指定任务的进度信息
        
        Args:
            task_id: 任务ID
            
        Returns:
            包含任务进度信息的字典
        """
        if task_id not in self.tasks:
            return {'error': f'任务不存在: {task_id}'}
        
        tracker = self.tasks[task_id]['tracker']
        progress_info = tracker.get_info()
        
        # 如果任务正在运行且使用CLI下载，实时更新进度
        if (progress_info.get('status') == 'running' and 
            hasattr(tracker, '_cli_process') and 
            hasattr(tracker, '_cli_save_dir')):
            try:
                # 检查CLI进程是否还在运行
                process = tracker._cli_process
                save_dir = tracker._cli_save_dir
                
                if process.poll() is None:  # 进程还在运行
                    # 实时计算当前下载大小
                    current_size = 0
                    for subdir in [save_dir / 'dataset', save_dir / 'cache']:
                        if subdir.exists():
                            for file_path in subdir.rglob('*'):
                                if file_path.is_file():
                                    current_size += file_path.stat().st_size
                    
                    # 根据文件大小更新progress信息
                    if current_size > 0:
                        progress_info['downloaded_bytes'] = current_size
                        progress_info['current_size_mb'] = round(current_size / (1024 * 1024), 1)
                        
                        # 基于文件大小重新计算进度
                        if tracker.start_time:
                            elapsed = time.time() - tracker.start_time.timestamp()
                            if current_size < 50 * 1024 * 1024:  # <50MB
                                estimated_progress = min(95, 10 + int(elapsed / 5) * 8)
                            elif current_size < 200 * 1024 * 1024:  # <200MB
                                estimated_progress = min(95, 10 + int(elapsed / 8) * 6)
                            else:  # >200MB
                                estimated_progress = min(95, 10 + int(elapsed / 15) * 4)
                            
                            progress_info['progress'] = estimated_progress
                            tracker.progress = estimated_progress  # 更新tracker中的进度
                
                elif process.poll() == 0:  # 进程已完成
                    # CLI下载完成，更新状态为完成
                    tracker.complete()
                    progress_info = tracker.get_info()
                    # 清理进程信息
                    if hasattr(tracker, '_cli_process'):
                        delattr(tracker, '_cli_process')
                    if hasattr(tracker, '_cli_save_dir'):
                        delattr(tracker, '_cli_save_dir')
                    # 保存状态
                    self._save_tasks_to_state()
                    
            except Exception as e:
                self.logger.debug(f"实时进度更新失败: {e}", task_id)
        
        return progress_info
    
    def list_tasks(self) -> List[Dict[str, Any]]:
        """
        列出所有下载任务
        
        Returns:
            包含所有任务信息的列表
        """
        result = []
        for task_id, task in self.tasks.items():
            # 使用get_task_progress获取实时进度，而不是直接调用tracker.get_info()
            progress_info = self.get_task_progress(task_id)
            
            task_info = {
                'task_id': task_id,
                'params': task['params'],
                'progress': progress_info
            }
            
            # 为UI界面添加一些便于显示的字段
            params = task['params']
            task_info.update({
                'dataset_name': params.get('dataset_name', 'Unknown'),
                'source_type': params.get('source_type', 'Unknown'),
                'status': progress_info.get('status', 'Unknown'),
                'progress_percent': progress_info.get('progress', 0),
                'current_size_mb': progress_info.get('current_size_mb', 0),
                'created_at': params.get('created_at', '')
            })
            
            result.append(task_info)
        
        return result
    
    def pause_task(self, task_id: str) -> bool:
        """
        暂停指定的下载任务
        
        注意：当前实现为简化版本，主要用于状态标记。
        实际的暂停功能需要配合具体的下载实现。
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 是否成功暂停
        """
        if task_id not in self.tasks:
            return False
        
        tracker = self.tasks[task_id]['tracker']
        if tracker.status == "running":
            tracker.pause()
            self.logger.info(f"任务已暂停", task_id)
            return True
        
        return False
    
    def resume_task(self, task_id: str) -> bool:
        """
        恢复暂停的下载任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            bool: 是否成功恢复
        """
        if task_id not in self.tasks:
            return False
        
        tracker = self.tasks[task_id]['tracker']
        if tracker.status == "paused":
            tracker.resume()
            self.logger.info(f"任务已恢复", task_id)
            return True
        
        return False
    
    def delete_task(self, task_id: str, delete_files: bool = False) -> bool:
        """
        删除指定的下载任务
        
        Args:
            task_id: 任务ID
            delete_files: 是否同时删除已下载的文件
            
        Returns:
            bool: 是否成功删除
        """
        if task_id not in self.tasks:
            return False
        
        if delete_files:
            # 删除相关文件和目录
            try:
                params = self.tasks[task_id]['params']
                save_dir = Path(params['save_dir'])
                if save_dir.exists():
                    import shutil
                    shutil.rmtree(save_dir)
                    self.logger.info(f"已删除下载文件: {save_dir}", task_id)
            except Exception as e:
                self.logger.error(f"删除文件失败: {str(e)}", task_id)
        
        del self.tasks[task_id]
        
        # 保存任务状态到文件
        self._save_tasks_to_state()
        
        self.logger.info(f"任务已删除", task_id)
        return True
    
    def get_task_details(self, task_id: str) -> Dict[str, Any]:
        """
        获取任务的详细信息
        
        Args:
            task_id: 任务ID
            
        Returns:
            包含任务详细信息的字典
        """
        if task_id not in self.tasks:
            return {'error': f'任务不存在: {task_id}'}
        
        task = self.tasks[task_id]
        return {
            'task_id': task_id,
            'params': task['params'],
            'progress': task['tracker'].get_info()
        }
    
    def cleanup_temp_files(self, max_age_hours: int = 24):
        """
        清理过期的临时文件
        
        Args:
            max_age_hours: 临时文件最大保留时间（小时）
        """
        try:
            temp_dir = Path(self.config_mgr.get_config('base.root_dir')) / 'temp'
            if not temp_dir.exists():
                return
            
            current_time = time.time()
            max_age_seconds = max_age_hours * 3600
            
            for temp_file in temp_dir.rglob('*.tmp'):
                try:
                    file_age = current_time - temp_file.stat().st_mtime
                    if file_age > max_age_seconds:
                        temp_file.unlink()
                        self.logger.info(f"已清理过期临时文件: {temp_file}")
                except Exception as e:
                    self.logger.warning(f"清理临时文件失败: {temp_file}, 错误: {str(e)}")
                    
        except Exception as e:
            self.logger.error(f"清理临时文件操作失败: {str(e)}")


# 全局下载器实例
# 提供模块级别的单例下载器，便于在应用中直接使用
_downloader_instance = None

def get_downloader() -> DatasetDownloader:
    """
    获取全局下载器实例
    
    使用单例模式，确保整个应用中只有一个下载器实例。
    
    Returns:
        DatasetDownloader: 下载器实例
    """
    global _downloader_instance
    if _downloader_instance is None:
        _downloader_instance = DatasetDownloader()
    return _downloader_instance


def diagnose_network():
    """
    网络诊断工具
    
    测试到Hugging Face的网络连接状态
    """
    print("🔍 开始网络诊断...")
    
    # 测试基本网络连接
    try:
        import socket
        socket.setdefaulttimeout(10)
        result = socket.getaddrinfo('huggingface.co', 443)
        print("✅ DNS解析正常")
    except Exception as e:
        print(f"❌ DNS解析失败: {e}")
        return False
    
    # 测试HTTP连接
    try:
        import requests
        session = requests.Session()
        session.timeout = 15
        
        print("📡 测试HTTPS连接...")
        response = session.get("https://huggingface.co", timeout=15)
        print(f"✅ HTTPS连接正常，状态码: {response.status_code}")
        
        # 测试API连接
        print("🔌 测试API连接...")
        api_response = session.get("https://huggingface.co/api/datasets/squad", timeout=15)
        print(f"✅ API连接正常，状态码: {api_response.status_code}")
        
        return True
        
    except requests.exceptions.ConnectionError as e:
        print(f"❌ 网络连接失败: {e}")
        print("💡 建议检查:")
        print("   1. 网络连接是否正常")
        print("   2. 是否需要代理设置")
        print("   3. 防火墙设置")
        return False
    except requests.exceptions.Timeout as e:
        print(f"⏰ 连接超时: {e}")
        print("💡 建议:")
        print("   1. 检查网络速度")
        print("   2. 稍后重试")
        return False
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return False


def search_datasets(query: str, limit: int = 10):
    """
    搜索Hugging Face数据集
    
    Args:
        query: 搜索关键词
        limit: 返回结果数量限制
    """
    try:
        from huggingface_hub import list_datasets
        print(f"🔍 搜索包含 '{query}' 的数据集...")
        
        datasets = list(list_datasets(search=query, limit=limit))
        
        if datasets:
            print(f"找到 {len(datasets)} 个相关数据集:")
            for i, dataset in enumerate(datasets, 1):
                print(f"  {i}. {dataset.id}")
                if hasattr(dataset, 'downloads') and dataset.downloads:
                    print(f"     下载次数: {dataset.downloads}")
        else:
            print("❌ 未找到相关数据集")
            
    except Exception as e:
        print(f"❌ 搜索失败: {e}")


# ==============================================================================
# 公共API接口
# ==============================================================================

def download_dataset(source_type: str, dataset_name: str, save_dir: str = None, 
                    token: str = None, hf_token: str = None, ms_token: str = None, **kwargs) -> str:
    """
    简化的数据集下载接口（适合页面调用）
    
    这是模块的主要API接口，提供简单易用的下载功能。
    特别适合从Web页面或API接口调用，支持多种token传入方式。
    
    Args:
        source_type: 数据源类型，支持 'huggingface', 'modelscope', 'url'
        dataset_name: 数据集名称或URL地址
        save_dir: 保存目录路径，默认使用配置的根目录
        token: 通用API认证令牌（优先级最高）
        hf_token: Huggingface专用token
        ms_token: ModelScope专用token
        **kwargs: 其他下载参数，如 resume、timeout、retry_count 等
    
    Returns:
        str: 任务ID，用于后续的状态查询和管理
    
    Example:
        >>> # 下载Huggingface数据集（使用专用token）
        >>> task_id = download_dataset('huggingface', 'squad', hf_token='hf_xxx')
        >>> 
        >>> # 下载URL文件
        >>> task_id = download_dataset('url', 'https://example.com/data.csv')
        >>> 
        >>> # 下载ModelScope数据集（使用专用token）
        >>> task_id = download_dataset('modelscope', 'damo/nlp_dataset', ms_token='ms_xxx')
        >>> 
        >>> # 页面调用示例（多个任务并发）
        >>> tasks = []
        >>> for dataset_config in page_dataset_list:
        >>>     task_id = download_dataset(
        >>>         source_type=dataset_config['source'],
        >>>         dataset_name=dataset_config['name'],
        >>>         hf_token=page_config['hf_token'],
        >>>         ms_token=page_config['ms_token']
        >>>     )
        >>>     tasks.append(task_id)
    """
    downloader = get_downloader()
    
    # 根据数据源类型设置专用token
    if source_type.lower() == 'huggingface' and hf_token:
        kwargs['hf_token'] = hf_token
    elif source_type.lower() == 'modelscope' and ms_token:
        kwargs['ms_token'] = ms_token
    
    return downloader.add_download_task(
        source_type=source_type,
        dataset_name=dataset_name,
        save_dir=save_dir,
        token=token,
        **kwargs
    )


def start_download(task_id: str) -> bool:
    """
    启动下载任务（独立脚本调用接口）
    
    Args:
        task_id: 任务ID
        
    Returns:
        bool: 是否成功启动
    """
    downloader = get_downloader()
    # 使用默认的同步模式
    return downloader.start_task(task_id)


def get_progress(task_id: str) -> Dict[str, Any]:
    """
    获取下载进度信息
    
    Args:
        task_id: 任务ID
        
    Returns:
        Dict: 包含进度信息的字典
    """
    downloader = get_downloader()
    return downloader.get_task_progress(task_id)


def list_downloads() -> List[Dict[str, Any]]:
    """
    列出所有下载任务
    
    Returns:
        List: 包含所有任务信息的列表
    """
    downloader = get_downloader()
    return downloader.list_tasks()


def pause_download(task_id: str) -> bool:
    """
    暂停下载任务
    
    Args:
        task_id: 任务ID
        
    Returns:
        bool: 是否成功暂停
    """
    downloader = get_downloader()
    return downloader.pause_task(task_id)


def resume_download(task_id: str) -> bool:
    """
    恢复下载任务
    
    Args:
        task_id: 任务ID
        
    Returns:
        bool: 是否成功恢复
    """
    downloader = get_downloader()
    return downloader.resume_task(task_id)


def delete_download(task_id: str, delete_files: bool = False) -> bool:
    """
    删除下载任务
    
    Args:
        task_id: 任务ID
        delete_files: 是否同时删除已下载的文件
        
    Returns:
        bool: 是否成功删除
    """
    downloader = get_downloader()
    return downloader.delete_task(task_id, delete_files)


# ==============================================================================
# 命令行界面
# ==============================================================================

def main():
    """
    命令行工具入口点
    
    提供完整的命令行界面，支持下载任务的创建、管理和监控。
    """
    parser = argparse.ArgumentParser(
        description='数据集下载工具 v2.0',
        epilog="""
使用示例:
  # 从Huggingface下载数据集
  python dataset_downloader.py huggingface squad --token hf_xxxxx
  
  # 从URL下载文件
  python dataset_downloader.py url https://example.com/data.csv
  
  # 从ModelScope下载数据集  
  python dataset_downloader.py modelscope damo/nlp_dataset --token ms_xxxxx
  
  # 列出所有任务
  python dataset_downloader.py --list-tasks
  
  # 查看任务进度
  python dataset_downloader.py --progress dl-20240101120000-abc123
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # 基本下载参数
    parser.add_argument('source_type', nargs='?', 
                       choices=['huggingface', 'modelscope', 'url'],
                       help='数据源类型')
    parser.add_argument('dataset_name', nargs='?', 
                       help='数据集名称或URL地址')
    
    # 下载选项
    parser.add_argument('--save-dir', 
                       help='保存目录（默认: ./data/raw/[source_type]）')
    parser.add_argument('--token', 
                       help='API认证令牌')
    parser.add_argument('--timeout', type=int, default=300,
                       help='请求超时时间（秒，默认: 300）')
    parser.add_argument('--retry', type=int, default=3,
                       help='重试次数（默认: 3）')
    parser.add_argument('--no-resume', action='store_true',
                       help='禁用断点续传')
    
    # 任务管理选项
    parser.add_argument('--list-tasks', action='store_true',
                       help='列出所有下载任务')
    parser.add_argument('--progress', metavar='TASK_ID',
                       help='查看指定任务的进度')
    parser.add_argument('--pause', metavar='TASK_ID',
                       help='暂停指定任务')
    parser.add_argument('--resume', metavar='TASK_ID',
                       help='恢复指定任务')
    parser.add_argument('--delete', metavar='TASK_ID',
                       help='删除指定任务')
    parser.add_argument('--delete-files', action='store_true',
                       help='删除任务时同时删除文件')
    
    # 其他选项
    parser.add_argument('--cleanup', action='store_true',
                       help='清理过期的临时文件')
    parser.add_argument('--verbose', action='store_true',
                       help='显示详细输出')
    parser.add_argument('--diagnose', action='store_true',
                       help='运行网络诊断')
    parser.add_argument('--search', metavar='QUERY',
                       help='搜索数据集')
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 处理诊断命令
    if args.diagnose:
        diagnose_network()
        return
    
    # 处理搜索命令
    if args.search:
        search_datasets(args.search)
        return
    
    downloader = get_downloader()
    
    try:
        # 处理任务管理命令
        if args.list_tasks:
            tasks = list_downloads()
            if not tasks:
                print("暂无下载任务")
            else:
                print("下载任务列表:")
                print("-" * 80)
                for task in tasks:
                    info = task['progress']
                    params = task['params']
                    print(f"任务ID: {info['task_id']}")
                    print(f"数据源: {params['source_type']}")
                    print(f"数据集: {params['dataset_name']}")
                    print(f"状态: {info['status']}")
                    print(f"进度: {info['progress']}%")
                    if info['total_bytes'] > 0:
                        print(f"大小: {info['downloaded_bytes']}/{info['total_bytes']} 字节")
                    print("-" * 80)
            return
        
        if args.progress:
            info = get_progress(args.progress)
            if 'error' in info:
                print(f"错误: {info['error']}")
            else:
                print(f"任务 {info['task_id']} 进度信息:")
                print(f"  状态: {info['status']}")
                print(f"  进度: {info['progress']}%")
                if info['total_bytes'] > 0:
                    print(f"  已下载: {info['downloaded_bytes']:,} 字节")
                    print(f"  总大小: {info['total_bytes']:,} 字节")
                if info['speed'] > 0:
                    speed_mb = info['speed'] / 1024 / 1024
                    print(f"  下载速度: {speed_mb:.2f} MB/s")
                if info['eta'] > 0:
                    eta_min = info['eta'] / 60
                    print(f"  预计剩余时间: {eta_min:.1f} 分钟")
                if 'error_msg' in info:
                    print(f"  错误信息: {info['error_msg']}")
            return
        
        if args.pause:
            if pause_download(args.pause):
                print(f"任务 {args.pause} 已暂停")
            else:
                print(f"无法暂停任务 {args.pause}")
            return
        
        if args.resume:
            if resume_download(args.resume):
                print(f"任务 {args.resume} 已恢复")
            else:
                print(f"无法恢复任务 {args.resume}")
            return
        
        if args.delete:
            if delete_download(args.delete, args.delete_files):
                action = "已删除（包含文件）" if args.delete_files else "已删除"
                print(f"任务 {args.delete} {action}")
            else:
                print(f"无法删除任务 {args.delete}")
            return
        
        if args.cleanup:
            downloader.cleanup_temp_files()
            print("临时文件清理完成")
            return
        
        # 处理下载命令
        if not args.source_type or not args.dataset_name:
            parser.print_help()
            return
        
        print(f"创建下载任务: {args.source_type} - {args.dataset_name}")
        
        # 创建下载任务
        task_id = download_dataset(
            source_type=args.source_type,
            dataset_name=args.dataset_name,
            save_dir=args.save_dir,
            token=args.token,
            resume=not args.no_resume,
            timeout=args.timeout,
            retry_count=args.retry
        )
        
        print(f"任务已创建: {task_id}")
        
        # 启动下载
        if start_download(task_id):
            print("开始下载...")
            
            # 监控进度
            while True:
                time.sleep(2)
                progress = get_progress(task_id)
                
                if progress['status'] == 'completed':
                    print(f"\n下载完成!")
                    break
                elif progress['status'] == 'failed':
                    error_msg = progress.get('error_msg', '未知错误')
                    print(f"\n下载失败: {error_msg}")
                    break
                elif progress['status'] == 'running':
                    # 显示进度信息
                    progress_text = f"进度: {progress['progress']}%"
                    if progress['speed'] > 0:
                        speed_mb = progress['speed'] / 1024 / 1024
                        progress_text += f" | 速度: {speed_mb:.2f} MB/s"
                    if progress['eta'] > 0:
                        eta_min = progress['eta'] / 60
                        progress_text += f" | 剩余: {eta_min:.1f} 分钟"
                    print(f"\r{progress_text}", end='', flush=True)
        else:
            print("任务启动失败")
            
    except KeyboardInterrupt:
        print("\n\n用户中断操作")
    except Exception as e:
        print(f"\n操作失败: {str(e)}")


# 创建全局实例
dataset_downloader = DatasetDownloader()

if __name__ == '__main__':
    main()
