#!/usr/bin/env python3
"""
性能优化工具模块

本模块提供性能优化相关的工具函数，包括并行处理、缓存机制等。
功能特点：
- 并行处理支持（线程池和进程池）
- 内存缓存机制
- 文件缓存机制
- 性能监控和统计

设计原则：
- 可配置的并行度
- 智能的缓存策略
- 内存使用监控
- 性能指标收集

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import os
import json
import time
import hashlib
import threading
import functools
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable, TypeVar, Tuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from collections import OrderedDict
import psutil
import sys

# 类型变量
T = TypeVar('T')
R = TypeVar('R')


class MemoryCache:
    """
    内存缓存类
    
    使用LRU（最近最少使用）策略的内存缓存。
    """
    
    def __init__(self, max_size: int = 100, ttl: Optional[int] = None):
        """
        初始化内存缓存
        
        Args:
            max_size: 最大缓存项数
            ttl: 缓存过期时间（秒），None表示不过期
        """
        self.max_size = max_size
        self.ttl = ttl
        self.cache: OrderedDict = OrderedDict()
        self.timestamps: Dict[str, float] = {}
        self.lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存值
        
        Args:
            key: 缓存键
            
        Returns:
            缓存值，如果不存在或已过期则返回None
        """
        with self.lock:
            if key not in self.cache:
                return None
            
            # 检查是否过期
            if self.ttl and key in self.timestamps:
                if time.time() - self.timestamps[key] > self.ttl:
                    del self.cache[key]
                    del self.timestamps[key]
                    return None
            
            # 移动到末尾（LRU）
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
    
    def set(self, key: str, value: Any) -> None:
        """
        设置缓存值
        
        Args:
            key: 缓存键
            value: 缓存值
        """
        with self.lock:
            if key in self.cache:
                # 更新现有项
                self.cache.pop(key)
            elif len(self.cache) >= self.max_size:
                # 删除最旧的项
                self.cache.popitem(last=False)
                if self.ttl:
                    oldest_key = list(self.timestamps.keys())[0]
                    del self.timestamps[oldest_key]
            
            self.cache[key] = value
            if self.ttl:
                self.timestamps[key] = time.time()
    
    def clear(self) -> None:
        """清空缓存"""
        with self.lock:
            self.cache.clear()
            self.timestamps.clear()
    
    def size(self) -> int:
        """获取当前缓存项数"""
        with self.lock:
            return len(self.cache)


class FileCache:
    """
    文件缓存类
    
    基于文件系统的缓存，支持持久化。
    """
    
    def __init__(self, cache_dir: str = "./data/cache", max_size_mb: int = 1000):
        """
        初始化文件缓存
        
        Args:
            cache_dir: 缓存目录
            max_size_mb: 最大缓存大小（MB）
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.lock = threading.Lock()
    
    def _get_cache_path(self, key: str) -> Path:
        """获取缓存文件路径"""
        # 使用hash避免文件名过长
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.cache"
    
    def get(self, key: str) -> Optional[Any]:
        """
        获取缓存值
        
        Args:
            key: 缓存键
            
        Returns:
            缓存值，如果不存在则返回None
        """
        cache_path = self._get_cache_path(key)
        
        if not cache_path.exists():
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 检查是否过期
                if 'expires_at' in data and time.time() > data['expires_at']:
                    cache_path.unlink()
                    return None
                return data.get('value')
        except Exception:
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        设置缓存值
        
        Args:
            key: 缓存键
            value: 缓存值
            ttl: 过期时间（秒）
        """
        cache_path = self._get_cache_path(key)
        
        try:
            data = {
                'value': value,
                'created_at': time.time()
            }
            if ttl:
                data['expires_at'] = time.time() + ttl
            
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f)
            
            # 检查缓存大小并清理
            self._cleanup_if_needed()
        except Exception:
            pass
    
    def _cleanup_if_needed(self) -> None:
        """如果缓存过大，清理最旧的文件"""
        try:
            total_size = sum(f.stat().st_size for f in self.cache_dir.glob("*.cache"))
            if total_size > self.max_size_bytes:
                # 按修改时间排序，删除最旧的文件
                files = sorted(self.cache_dir.glob("*.cache"), key=lambda f: f.stat().st_mtime)
                for f in files:
                    if total_size <= self.max_size_bytes * 0.8:  # 清理到80%
                        break
                    total_size -= f.stat().st_size
                    f.unlink()
        except Exception:
            pass
    
    def clear(self) -> None:
        """清空缓存"""
        try:
            for f in self.cache_dir.glob("*.cache"):
                f.unlink()
        except Exception:
            pass


class ParallelProcessor:
    """
    并行处理器
    
    提供线程池和进程池的并行处理功能。
    """
    
    def __init__(self, max_workers: Optional[int] = None, use_processes: bool = False):
        """
        初始化并行处理器
        
        Args:
            max_workers: 最大工作线程/进程数，None表示使用CPU核心数
            use_processes: 是否使用进程池（默认使用线程池）
        """
        if max_workers is None:
            max_workers = os.cpu_count() or 4
        
        self.max_workers = max_workers
        self.use_processes = use_processes
        self.executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    
    def map(self, func: Callable[[T], R], items: List[T], 
            chunk_size: Optional[int] = None) -> List[R]:
        """
        并行处理列表
        
        Args:
            func: 处理函数
            items: 待处理项列表
            chunk_size: 分块大小（用于进程池）
            
        Returns:
            处理结果列表
        """
        with self.executor_class(max_workers=self.max_workers) as executor:
            if self.use_processes and chunk_size:
                # 进程池使用chunk_size
                results = list(executor.map(func, items, chunksize=chunk_size))
            else:
                # 线程池直接map
                results = list(executor.map(func, items))
        return results
    
    def submit(self, func: Callable, *args, **kwargs):
        """
        提交单个任务
        
        Args:
            func: 处理函数
            *args: 位置参数
            **kwargs: 关键字参数
            
        Returns:
            Future对象
        """
        executor = self.executor_class(max_workers=self.max_workers)
        return executor.submit(func, *args, **kwargs)
    
    def process_batch(self, func: Callable[[T], R], items: List[T],
                     batch_size: Optional[int] = None) -> List[R]:
        """
        批量并行处理
        
        Args:
            func: 处理函数
            items: 待处理项列表
            batch_size: 批次大小，None表示不分组
            
        Returns:
            处理结果列表
        """
        if batch_size is None:
            return self.map(func, items)
        
        results = []
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_results = self.map(func, batch)
            results.extend(batch_results)
        return results


def parallel_map(func: Callable[[T], R], items: List[T], 
                 max_workers: Optional[int] = None,
                 use_processes: bool = False) -> List[R]:
    """
    并行处理列表的便捷函数
    
    Args:
        func: 处理函数
        items: 待处理项列表
        max_workers: 最大工作线程/进程数
        use_processes: 是否使用进程池
        
    Returns:
        处理结果列表
    """
    processor = ParallelProcessor(max_workers=max_workers, use_processes=use_processes)
    return processor.map(func, items)


def cache_result(cache: Optional[MemoryCache] = None, 
                 key_func: Optional[Callable] = None,
                 ttl: Optional[int] = None):
    """
    缓存函数结果的装饰器
    
    Args:
        cache: 缓存对象，None表示使用默认内存缓存
        key_func: 生成缓存键的函数
        ttl: 缓存过期时间（秒）
        
    Returns:
        装饰器函数
    """
    if cache is None:
        cache = MemoryCache(ttl=ttl)
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            
            # 尝试从缓存获取
            result = cache.get(cache_key)
            if result is not None:
                return result
            
            # 执行函数并缓存结果
            result = func(*args, **kwargs)
            cache.set(cache_key, result)
            return result
        
        return wrapper
    return decorator


def get_memory_usage() -> Dict[str, Any]:
    """
    获取当前内存使用情况
    
    Returns:
        包含内存使用信息的字典
    """
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    
    return {
        'rss_mb': memory_info.rss / 1024 / 1024,  # 物理内存
        'vms_mb': memory_info.vms / 1024 / 1024,  # 虚拟内存
        'percent': process.memory_percent(),
        'available_mb': psutil.virtual_memory().available / 1024 / 1024
    }


def monitor_memory(threshold_mb: float = 1000, 
                  callback: Optional[Callable] = None) -> bool:
    """
    监控内存使用，如果超过阈值则触发回调
    
    Args:
        threshold_mb: 内存阈值（MB）
        callback: 超过阈值时的回调函数
        
    Returns:
        是否超过阈值
    """
    usage = get_memory_usage()
    if usage['rss_mb'] > threshold_mb:
        if callback:
            callback(usage)
        return True
    return False


# 全局缓存实例
_default_memory_cache = MemoryCache(max_size=100)
_default_file_cache = FileCache()


def get_default_memory_cache() -> MemoryCache:
    """获取默认内存缓存实例"""
    return _default_memory_cache


def get_default_file_cache() -> FileCache:
    """获取默认文件缓存实例"""
    return _default_file_cache

