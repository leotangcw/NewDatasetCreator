#!/usr/bin/env python3
"""
日志管理模块

本模块为所有模块提供统一的日志记录功能。
功能特点：
- 按模块分类日志，便于问题定位
- 支持分级输出（DEBUG/INFO/WARN/ERROR）
- 日志轮转，避免单个文件过大
- 同时支持文件输出和控制台输出
- 任务关联日志，支持按任务ID过滤

设计原则：
- 统一的日志格式和管理策略
- 高性能的日志记录，不影响主业务
- 敏感信息过滤，保护用户隐私
- 详细的上下文信息记录

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import os
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Any, List
import threading
import re
import sys

# 导入统一异常类
try:
    from .exceptions import LogError, LogFileError
except ImportError:
    # 如果导入失败（直接运行脚本时），使用本地定义
    from exceptions import LogError, LogFileError

# 模块导出列表
__all__ = ['LogManager', 'TaskAwareLogger', 'log_manager']


class TaskAwareLogger:
    """
    任务感知的日志记录器
    
    扩展标准Logger功能，支持任务ID关联和敏感信息过滤。
    """
    
    def __init__(self, logger: logging.Logger):
        """
        初始化任务感知日志记录器
        
        Args:
            logger (logging.Logger): 标准日志记录器实例
        """
        self.logger = logger
        # 扩展的敏感信息模式（更全面的匹配）
        self._sensitive_patterns = [
            r'(api[_-]?key\s*[=:]\s*)[^\s]+',  # API密钥
            r'(token\s*[=:]\s*)[^\s]+',        # Token
            r'(password\s*[=:]\s*)[^\s]+',     # 密码
            r'(secret\s*[=:]\s*)[^\s]+',       # 密钥
            r'(auth[_-]?token\s*[=:]\s*)[^\s]+',  # 认证令牌
            r'(access[_-]?token\s*[=:]\s*)[^\s]+',  # 访问令牌
            r'hf_[a-zA-Z0-9]{34}',             # HuggingFace token
            r'sk-[a-zA-Z0-9]{48}',             # OpenAI API key
            r'ghp_[a-zA-Z0-9]{36}',            # GitHub token
            r'xox[baprs]-[0-9a-zA-Z-]{10,}',  # Slack token
        ]
        
        # 预编译敏感信息替换模式
        self._compiled_patterns = []
        patterns = [
            # 通用密钥模式
            (r'(api[_-]?key\s*[=:]\s*)([^\s]+)', r'\1****'),  # API密钥
            (r'(token\s*[=:]\s*)([^\s]+)', r'\1****'),        # Token
            (r'(password\s*[=:]\s*)([^\s]+)', r'\1****'),     # 密码
            (r'(secret\s*[=:]\s*)([^\s]+)', r'\1****'),       # 密钥
            (r'(auth[_-]?token\s*[=:]\s*)([^\s]+)', r'\1****'),  # 认证令牌
            (r'(access[_-]?token\s*[=:]\s*)([^\s]+)', r'\1****'),  # 访问令牌
            (r'(bearer\s+)([^\s]+)', r'\1****'),              # Bearer token
            
            # 特定服务token
            (r'hf_[a-zA-Z0-9]{34}', 'hf_****'),               # HuggingFace token
            (r'sk-[a-zA-Z0-9]{48}', 'sk-****'),               # OpenAI API key
            (r'ghp_[a-zA-Z0-9]{36}', 'ghp_****'),             # GitHub token
            (r'xox[baprs]-[0-9a-zA-Z-]{10,}', 'xox-****'),   # Slack token
            
            # 邮箱地址（部分脱敏）
            (r'\b([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b', r'\1@****'),
            
            # 手机号（中国格式）
            (r'\b1[3-9]\d{9}\b', '1**********'),
            
            # 身份证号（中国格式）
            (r'\b\d{17}[\dXx]\b', '******************'),
            
            # 银行卡号（部分脱敏）
            (r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b', '**** **** **** ****'),
        ]
        
        for pattern, replacement in patterns:
            self._compiled_patterns.append((re.compile(pattern, flags=re.IGNORECASE), replacement))
    
    def _filter_sensitive_info(self, message: str) -> str:
        """
        过滤敏感信息
        
        Args:
            message (str): 原始消息
            
        Returns:
            str: 过滤后的消息
        """
        filtered_message = message
        
        for pattern, replacement in self._compiled_patterns:
            filtered_message = pattern.sub(replacement, filtered_message)
        
        return filtered_message
    
    def _log_with_task(self, level: int, message: str, task_id: Optional[str] = None, 
                      **kwargs) -> None:
        """
        带任务ID的日志记录
        
        Args:
            level (int): 日志级别
            message (str): 日志消息
            task_id (str, optional): 任务ID
            **kwargs: 额外的日志上下文信息
        """
        # 过滤敏感信息
        filtered_message = self._filter_sensitive_info(message)
        
        # 添加任务ID和额外信息到日志消息
        if task_id:
            filtered_message = f"[{task_id}] {filtered_message}"
        
        # 添加额外的上下文信息
        if kwargs:
            context_info = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
            filtered_message = f"{filtered_message} [{context_info}]"
        
        # 记录日志
        self.logger.log(level, filtered_message)
    
    def debug(self, message: str, task_id: Optional[str] = None, **kwargs) -> None:
        """调试日志"""
        self._log_with_task(logging.DEBUG, message, task_id, **kwargs)
    
    def info(self, message: str, task_id: Optional[str] = None, **kwargs) -> None:
        """信息日志"""
        self._log_with_task(logging.INFO, message, task_id, **kwargs)
    
    def warning(self, message: str, task_id: Optional[str] = None, **kwargs) -> None:
        """警告日志"""
        self._log_with_task(logging.WARNING, message, task_id, **kwargs)
    
    def error(self, message: str, task_id: Optional[str] = None, **kwargs) -> None:
        """错误日志"""
        self._log_with_task(logging.ERROR, message, task_id, **kwargs)
    
    def critical(self, message: str, task_id: Optional[str] = None, **kwargs) -> None:
        """严重错误日志"""
        self._log_with_task(logging.CRITICAL, message, task_id, **kwargs)


class LogManager:
    """
    日志管理器
    
    负责创建和管理所有模块的日志记录器，提供统一的日志配置和管理。
    """
    
    def __init__(self, log_dir: str = "logs", log_level: str = "INFO"):
        """
        初始化日志管理器
        
        Args:
            log_dir (str): 日志目录
            log_level (str): 默认日志级别
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.log_level = getattr(logging, log_level.upper())
        self.loggers: Dict[str, TaskAwareLogger] = {}
        self._lock = threading.Lock()
        
        # 默认配置
        self.config = {
            'log_dir': str(self.log_dir),
            'log_level': log_level,
            'max_file_size': 10 * 1024 * 1024,  # 10MB
            'backup_count': 5,
            'console_output': True,
            'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'date_format': '%Y-%m-%d %H:%M:%S'
        }
        
        # 配置根日志记录器
        self._setup_root_logger()
    
    def _setup_root_logger(self) -> None:
        """设置根日志记录器"""
        # 避免重复配置
        if logging.getLogger().handlers:
            return
            
        # 设置根日志级别
        logging.getLogger().setLevel(self.log_level)
        
        # 创建格式化器
        formatter = logging.Formatter(
            self.config['log_format'],
            self.config['date_format']
        )
        
        # 添加控制台处理器
        if self.config['console_output']:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self.log_level)
            console_handler.setFormatter(formatter)
            logging.getLogger().addHandler(console_handler)
    
    def get_logger(self, module_name: str) -> TaskAwareLogger:
        """
        获取指定模块的日志记录器
        
        Args:
            module_name (str): 模块名称
            
        Returns:
            TaskAwareLogger: 任务感知的日志记录器
        """
        with self._lock:
            if module_name not in self.loggers:
                # 创建标准日志记录器
                logger = logging.getLogger(module_name)
                logger.setLevel(self.log_level)
                
                # 创建文件处理器（带轮转）
                log_file = self.log_dir / f"{module_name}.log"
                file_handler = logging.handlers.RotatingFileHandler(
                    log_file,
                    maxBytes=self.config['max_file_size'],
                    backupCount=self.config['backup_count'],
                    encoding='utf-8'
                )
                file_handler.setLevel(self.log_level)
                
                # 设置格式化器
                formatter = logging.Formatter(
                    self.config['log_format'],
                    self.config['date_format']
                )
                file_handler.setFormatter(formatter)
                
                # 添加处理器
                logger.addHandler(file_handler)
                
                # 避免重复输出到根日志记录器
                logger.propagate = False
                
                # 创建任务感知日志记录器
                self.loggers[module_name] = TaskAwareLogger(logger)
            
            return self.loggers[module_name]
    
    def set_log_level(self, level: str) -> None:
        """
        设置全局日志级别
        
        Args:
            level (str): 日志级别 (DEBUG/INFO/WARNING/ERROR/CRITICAL)
        """
        self.log_level = getattr(logging, level.upper())
        self.config['log_level'] = level.upper()
        
        # 更新所有现有日志记录器的级别
        with self._lock:
            for task_logger in self.loggers.values():
                task_logger.logger.setLevel(self.log_level)
                for handler in task_logger.logger.handlers:
                    handler.setLevel(self.log_level)
            
            # 更新根日志记录器
            logging.getLogger().setLevel(self.log_level)
            for handler in logging.getLogger().handlers:
                handler.setLevel(self.log_level)
    
    def enable_console_output(self, enable: bool = True) -> None:
        """
        启用/禁用控制台输出
        
        Args:
            enable (bool): 是否启用控制台输出
        """
        self.config['console_output'] = enable
        
        root_logger = logging.getLogger()
        
        if enable:
            # 检查是否已有控制台处理器
            has_console = any(isinstance(h, logging.StreamHandler) and h.stream == sys.stdout 
                            for h in root_logger.handlers)
            if not has_console:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(self.log_level)
                formatter = logging.Formatter(
                    self.config['log_format'],
                    self.config['date_format']
                )
                console_handler.setFormatter(formatter)
                root_logger.addHandler(console_handler)
        else:
            # 移除控制台处理器
            root_logger.handlers = [h for h in root_logger.handlers 
                                  if not (isinstance(h, logging.StreamHandler) and h.stream == sys.stdout)]
    
    def get_log_files(self) -> List[Path]:
        """
        获取所有日志文件列表
        
        Returns:
            List[Path]: 日志文件路径列表
        """
        return list(self.log_dir.glob("*.log"))
    
    def clean_old_logs(self, days: int = 30) -> int:
        """
        清理指定天数之前的日志文件
        
        Args:
            days (int): 保留天数
            
        Returns:
            int: 删除的文件数量
        """
        cutoff_time = datetime.now().timestamp() - (days * 24 * 3600)
        deleted_count = 0
        
        for log_file in self.get_log_files():
            if log_file.stat().st_mtime < cutoff_time:
                try:
                    log_file.unlink()
                    deleted_count += 1
                except OSError:
                    pass  # 文件可能正在使用中
        
        return deleted_count
    
    def search_logs(self, keyword: str, module_name: Optional[str] = None, 
                   date: Optional[str] = None, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        搜索日志内容
        
        Args:
            keyword (str): 搜索关键词
            module_name (str, optional): 模块名限制
            date (str, optional): 日期限制
            max_results (int): 最大结果数量
            
        Returns:
            list: 搜索结果列表，每个元素包含文件路径、行号和内容
        """
        results = []
        search_files = []
        
        if module_name:
            log_file = self.log_dir / f"{module_name}.log"
            if log_file.exists():
                search_files.append(log_file)
        else:
            search_files = self.get_log_files()
        
        for log_file in search_files:
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line_no, line in enumerate(f, 1):
                        if keyword.lower() in line.lower():
                            # 日期过滤
                            if date and date not in line:
                                continue
                            
                            results.append({
                                'file': str(log_file),
                                'line_no': line_no,
                                'content': line.strip()
                            })
                            
                            if len(results) >= max_results:
                                return results
            except (OSError, UnicodeDecodeError):
                continue
        
        return results
    
    def get_log_stats(self) -> Dict[str, Any]:
        """
        获取日志统计信息
        
        Returns:
            dict: 日志统计信息
        """
        stats = {
            'total_files': 0,
            'total_size': 0,
            'files': []
        }
        
        for log_file in self.get_log_files():
            try:
                file_stat = log_file.stat()
                file_info = {
                    'name': log_file.name,
                    'size': file_stat.st_size,
                    'modified': datetime.fromtimestamp(file_stat.st_mtime).isoformat()
                }
                stats['files'].append(file_info)
                stats['total_size'] += file_stat.st_size
                stats['total_files'] += 1
            except OSError:
                continue
        
        return stats
    
    def update_config(self, **kwargs) -> None:
        """
        更新日志配置
        
        Args:
            **kwargs: 配置项键值对
        """
        self.config.update(kwargs)
        
        # 应用新配置
        if 'log_level' in kwargs:
            self.set_log_level(kwargs['log_level'])
        
        if 'console_output' in kwargs:
            self.enable_console_output(kwargs['console_output'])
    
    def get_config(self) -> Dict[str, Any]:
        """
        获取当前日志配置
        
        Returns:
            dict: 日志配置字典
        """
        return self.config.copy()


# 全局日志管理器实例
log_manager = LogManager()


if __name__ == "__main__":
    """
    命令行入口，用于日志管理操作
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='日志管理工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # level命令
    level_parser = subparsers.add_parser('level', help='设置日志级别')
    level_parser.add_argument('level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                            help='日志级别')
    
    # stats命令
    stats_parser = subparsers.add_parser('stats', help='显示日志统计信息')
    
    # search命令
    search_parser = subparsers.add_parser('search', help='搜索日志内容')
    search_parser.add_argument('keyword', help='搜索关键词')
    search_parser.add_argument('--module', help='限制搜索的模块')
    search_parser.add_argument('--date', help='限制搜索的日期')
    search_parser.add_argument('--max-results', type=int, default=100, help='最大结果数量')
    
    # clean命令
    clean_parser = subparsers.add_parser('clean', help='清理旧日志文件')
    clean_parser.add_argument('--days', type=int, default=30, help='保留天数')
    
    # test命令
    test_parser = subparsers.add_parser('test', help='测试日志功能')
    
    args = parser.parse_args()
    
    if args.command == 'level':
        log_manager.set_log_level(args.level)
        print(f"日志级别已设置为: {args.level}")
        
    elif args.command == 'stats':
        stats = log_manager.get_log_stats()
        print(f"日志文件总数: {stats['total_files']}")
        print(f"总大小: {stats['total_size']} 字节")
        print("\n文件详情:")
        for file_info in stats['files']:
            print(f"  {file_info['name']}: {file_info['size']} 字节, "
                  f"修改时间: {file_info['modified']}")
    
    elif args.command == 'search':
        results = log_manager.search_logs(
            args.keyword, 
            args.module, 
            args.date, 
            args.max_results
        )
        print(f"找到 {len(results)} 条匹配记录:")
        for result in results:
            print(f"  {result['file']}:{result['line_no']} - {result['content']}")
    
    elif args.command == 'clean':
        deleted_count = log_manager.clean_old_logs(args.days)
        print(f"已删除 {deleted_count} 个旧日志文件")
    
    elif args.command == 'test':
        # 创建测试日志记录器
        logger = log_manager.get_logger('test_module')
        
        print("正在测试日志功能...")
        
        # 测试各种日志级别
        logger.debug("这是DEBUG级别的测试日志", task_id="test_001")
        logger.info("这是INFO级别的测试日志", task_id="test_001")
        logger.warning("这是WARNING级别的测试日志", task_id="test_001")
        logger.error("这是ERROR级别的测试日志", task_id="test_001")
        
        # 测试敏感信息过滤
        logger.info("用户配置: api_key=hf_1234567890abcdef1234567890abcdef12", task_id="test_001")
        logger.info("连接信息: token=sk_1234567890abcdef1234567890abcdef1234567890abcdef12", task_id="test_001")
        
        print("日志测试完成，请检查日志文件")
        
    else:
        parser.print_help()
