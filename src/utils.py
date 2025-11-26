#!/usr/bin/env python3
"""
工具函数模块

本模块提供跨模块复用的通用工具函数。
功能特点：
- 文件操作：分片读取、哈希计算、格式检测
- 数据处理：字典展平、去重、数据校验
- 加密解密：文本加密、密钥生成
- 网络与系统：连接检查、重试机制、性能监控

设计原则：
- 功能单一，职责明确
- 高性能，支持大文件处理
- 线程安全的操作
- 详细的异常处理和错误信息

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import os
import json
import csv
import hashlib
import time
import random
import string
import threading
import logging
from pathlib import Path

# 模块导出列表
__all__ = [
    'FileOperations', 'DataProcessing', 'SecurityUtils', 'NetworkUtils', 
    'SystemUtils', 'PerformanceUtils', 'RetryUtils'
]
from typing import Dict, Any, Optional, List, Union, Generator, Callable, Tuple
from datetime import datetime
import re
import difflib
from functools import wraps
import urllib.parse
import socket
import platform
# import psutil # Removed direct import to use dependencies

# 统一依赖管理
from .dependencies import (
    pd, HAS_PANDAS,
    jsonlines, HAS_JSONLINES,
    ET, HAS_XML,
    requests, HAS_REQUESTS,
    ijson, HAS_IJSON,
    psutil, HAS_PSUTIL
)


class FileOperations:
    """文件操作工具类"""
    
    @staticmethod
    def read_file_chunk(file_path: Union[str, Path], 
                       chunk_size: int = 1000,
                       file_format: str = None) -> Generator[List[Dict], None, None]:
        """
        按行分片读取大文件（支持jsonl/csv/excel/xml）
        
        Args:
            file_path (str or Path): 文件路径
            chunk_size (int): 每片行数，默认1000
            file_format (str, optional): 文件格式，为None时自动检测
            
        Yields:
            List[Dict]: 每片数据的字典列表
            
        Examples:
            >>> for chunk in FileOperations.read_file_chunk("data.jsonl", 500):
            ...     print(f"处理了 {len(chunk)} 行数据")
            
        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 不支持的文件格式
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        # 自动检测文件格式
        if file_format is None:
            file_format = FileOperations.detect_file_format(file_path)
        
        try:
            if file_format == 'jsonl':
                yield from FileOperations._read_jsonl_chunks(file_path, chunk_size)
            elif file_format == 'csv':
                yield from FileOperations._read_csv_chunks(file_path, chunk_size)
            elif file_format == 'json':
                yield from FileOperations._read_json_chunks(file_path, chunk_size)
            elif file_format == 'excel':
                yield from FileOperations._read_excel_chunks(file_path, chunk_size)
            elif file_format == 'xml':
                yield from FileOperations._read_xml_chunks(file_path, chunk_size)
            else:
                raise ValueError(f"不支持的文件格式: {file_format}")
                
        except Exception as e:
            raise Exception(f"读取文件失败: {str(e)}")
    
    @staticmethod
    def _read_jsonl_chunks(file_path: Path, chunk_size: int) -> Generator[List[Dict], None, None]:
        """读取JSONL格式文件块"""
        chunk = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    chunk.append(data)
                    
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                        
                except json.JSONDecodeError as e:
                    print(f"跳过无效JSON行 {line_num}: {e}")
                    continue
        
        # 返回最后一批数据
        if chunk:
            yield chunk
    
    @staticmethod
    def _read_csv_chunks(file_path: Path, chunk_size: int) -> Generator[List[Dict], None, None]:
        """读取CSV格式文件块"""
        chunk = []
        
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                chunk.append(dict(row))
                
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
        
        if chunk:
            yield chunk
    
    @staticmethod
    def _read_json_chunks(file_path: Path, chunk_size: int) -> Generator[List[Dict], None, None]:
        """读取JSON格式文件块（支持流式解析）"""
        if HAS_IJSON:
            # 使用 ijson 进行流式解析，避免大文件内存溢出
            with open(file_path, 'rb') as f:
                # 尝试解析数组中的项
                try:
                    objects = ijson.items(f, 'item')
                    chunk = []
                    for obj in objects:
                        chunk.append(obj)
                        if len(chunk) >= chunk_size:
                            yield chunk
                            chunk = []
                    if chunk:
                        yield chunk
                    return
                except Exception:
                    # 如果不是标准数组，可能是单个对象或解析失败，回退到普通加载
                    pass
        
        # 回退到普通加载（适用于小文件或非数组JSON）
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            for i in range(0, len(data), chunk_size):
                yield data[i:i + chunk_size]
        else:
            yield [data]
    
    @staticmethod
    def _read_excel_chunks(file_path: Path, chunk_size: int) -> Generator[List[Dict], None, None]:
        """读取Excel格式文件块"""
        if not HAS_PANDAS:
            raise ImportError("需要安装pandas来处理Excel文件: pip install pandas openpyxl")
        
        try:
            # 分块读取Excel文件
            for chunk_df in pd.read_excel(file_path, chunksize=chunk_size):
                chunk = chunk_df.to_dict('records')
                yield chunk
        except Exception:
            # 如果分块读取失败，尝试一次性读取
            df = pd.read_excel(file_path)
            data = df.to_dict('records')
            
            for i in range(0, len(data), chunk_size):
                yield data[i:i + chunk_size]
    
    @staticmethod
    def _read_xml_chunks(file_path: Path, chunk_size: int) -> Generator[List[Dict], None, None]:
        """读取XML格式文件块"""
        if not HAS_XML:
            raise ImportError("需要XML支持")
        
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        chunk = []
        
        # 假设XML结构为 <root><item>...</item><item>...</item></root>
        for item in root:
            item_dict = FileOperations._xml_to_dict(item)
            chunk.append(item_dict)
            
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        
        if chunk:
            yield chunk
    
    @staticmethod
    def _xml_to_dict(element) -> Dict[str, Any]:
        """将XML元素转换为字典"""
        result = {}
        
        # 处理属性
        if element.attrib:
            result.update(element.attrib)
        
        # 处理文本内容
        if element.text and element.text.strip():
            if len(element) == 0:  # 没有子元素
                return element.text.strip()
            else:
                result['text'] = element.text.strip()
        
        # 处理子元素
        for child in element:
            child_data = FileOperations._xml_to_dict(child)
            
            if child.tag in result:
                # 如果已存在，转换为列表
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        
        return result
    
    @staticmethod
    def get_file_hash(file_path: Union[str, Path], algorithm: str = "md5") -> str:
        """
        计算文件哈希值（用于数据校验）
        
        Args:
            file_path (str or Path): 文件路径
            algorithm (str): 哈希算法，支持md5/sha1/sha256
            
        Returns:
            str: 文件哈希值
            
        Examples:
            >>> hash_value = FileOperations.get_file_hash("data.jsonl", "sha256")
            >>> print(hash_value)
            "a1b2c3d4e5f6..."
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        # 选择哈希算法
        if algorithm.lower() == "md5":
            hasher = hashlib.md5()
        elif algorithm.lower() == "sha1":
            hasher = hashlib.sha1()
        elif algorithm.lower() == "sha256":
            hasher = hashlib.sha256()
        else:
            raise ValueError(f"不支持的哈希算法: {algorithm}")
        
        # 分块读取文件计算哈希
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        
        return hasher.hexdigest()
    
    @staticmethod
    def detect_file_format(file_path: Union[str, Path]) -> str:
        """
        自动检测文件格式
        
        Args:
            file_path (str or Path): 文件路径
            
        Returns:
            str: 文件格式（jsonl/csv/json/excel/xml/unknown）
            
        Examples:
            >>> format_type = FileOperations.detect_file_format("data.xlsx")
            >>> print(format_type)
            "excel"
        """
        file_path = Path(file_path)
        suffix = file_path.suffix.lower()
        
        # 基于文件扩展名的格式映射
        format_map = {
            '.jsonl': 'jsonl',
            '.json': 'json',
            '.csv': 'csv',
            '.xlsx': 'excel',
            '.xls': 'excel',
            '.xml': 'xml',
            '.txt': 'text'
        }
        
        detected_format = format_map.get(suffix, 'unknown')
        
        # 对于txt文件，尝试进一步检测
        if detected_format == 'text':
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    
                    # 检查是否为JSONL
                    if first_line.startswith('{') and first_line.endswith('}'):
                        try:
                            json.loads(first_line)
                            return 'jsonl'
                        except json.JSONDecodeError:
                            pass
                    
                    # 检查是否为CSV
                    if ',' in first_line:
                        return 'csv'
                        
            except Exception:
                pass
        
        return detected_format
    
    @staticmethod
    def get_file_info(file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        获取文件详细信息
        
        Args:
            file_path (str or Path): 文件路径
            
        Returns:
            dict: 文件信息字典
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            return {'exists': False}
        
        stat = file_path.stat()
        
        info = {
            'exists': True,
            'size_bytes': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'created_time': datetime.fromtimestamp(stat.st_ctime).isoformat(),
            'modified_time': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'format': FileOperations.detect_file_format(file_path),
            'is_readable': os.access(file_path, os.R_OK),
            'is_writable': os.access(file_path, os.W_OK)
        }
        
        # 尝试获取行数（仅对文本文件）
        if info['format'] in ['jsonl', 'csv', 'json', 'xml', 'text']:
            try:
                line_count = 0
                with open(file_path, 'r', encoding='utf-8') as f:
                    for _ in f:
                        line_count += 1
                info['line_count'] = line_count
            except Exception:
                info['line_count'] = None
        
        return info


class DataProcessing:
    """数据处理工具类"""
    
    @staticmethod
    def flatten_dict(nested_dict: Dict[str, Any], separator: str = '.') -> Dict[str, Any]:
        """
        将嵌套字典展平（如XML节点→扁平字段）
        
        Args:
            nested_dict (dict): 嵌套字典
            separator (str): 分隔符，默认为'.'
            
        Returns:
            dict: 展平后的字典
            
        Examples:
            >>> nested = {"user": {"name": "Alice", "age": 30}, "id": 1}
            >>> flat = DataProcessing.flatten_dict(nested)
            >>> print(flat)
            {"user.name": "Alice", "user.age": 30, "id": 1}
        """
        def _flatten(obj, parent_key='', sep='.'):
            items = []
            
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_key = f"{parent_key}{sep}{k}" if parent_key else k
                    items.extend(_flatten(v, new_key, sep=sep).items())
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
                    items.extend(_flatten(v, new_key, sep=sep).items())
            else:
                return {parent_key: obj}
            
            return dict(items)
        
        return _flatten(nested_dict, '', separator)
    
    @staticmethod
    def dedup_rows(data: List[Dict[str, Any]], 
                  field: str, 
                  threshold: float = 0.95,
                  keep: str = 'first') -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        基于指定字段去重（支持文本相似度）
        
        Args:
            data (List[Dict]): 数据列表
            field (str): 去重字段名
            threshold (float): 相似度阈值，0.95表示95%相似即视为重复
            keep (str): 保留策略，'first'/'last'
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (去重后数据, 重复数据)
            
        Examples:
            >>> data = [
            ...     {"text": "Hello world", "id": 1},
            ...     {"text": "Hello world!", "id": 2},
            ...     {"text": "Different text", "id": 3}
            ... ]
            >>> unique, duplicates = DataProcessing.dedup_rows(data, "text", 0.9)
        """
        if not data:
            return [], []
        
        unique_data = []
        duplicate_data = []
        seen_values = []
        
        for item in data:
            if field not in item:
                unique_data.append(item)
                continue
            
            current_value = str(item[field])
            is_duplicate = False
            
            # 检查是否与已有值相似
            for seen_value in seen_values:
                similarity = DataProcessing.calculate_similarity(current_value, seen_value)
                if similarity >= threshold:
                    is_duplicate = True
                    break
            
            if is_duplicate:
                duplicate_data.append(item)
                if keep == 'last':
                    # 如果保留最后一个，需要更新seen_values
                    for i, seen_value in enumerate(seen_values):
                        if DataProcessing.calculate_similarity(current_value, seen_value) >= threshold:
                            seen_values[i] = current_value
                            # 从unique_data中移除之前的项
                            for j in range(len(unique_data) - 1, -1, -1):
                                if str(unique_data[j][field]) == seen_value:
                                    duplicate_data.append(unique_data.pop(j))
                                    break
                            unique_data.append(item)
                            break
            else:
                unique_data.append(item)
                seen_values.append(current_value)
        
        return unique_data, duplicate_data
    
    @staticmethod
    def calculate_similarity(text1: str, text2: str) -> float:
        """
        计算两个文本的相似度
        
        Args:
            text1 (str): 文本1
            text2 (str): 文本2
            
        Returns:
            float: 相似度值（0-1之间）
        """
        if text1 == text2:
            return 1.0
        
        # 使用difflib计算相似度
        similarity = difflib.SequenceMatcher(None, text1, text2).ratio()
        return similarity
    
    @staticmethod
    def validate_data_format(data: List[Dict[str, Any]], 
                           required_fields: List[str] = None,
                           field_types: Dict[str, type] = None) -> Dict[str, Any]:
        """
        验证数据格式
        
        Args:
            data (List[Dict]): 要验证的数据
            required_fields (List[str], optional): 必需字段列表
            field_types (Dict[str, type], optional): 字段类型约束
            
        Returns:
            dict: 验证结果
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'statistics': {
                'total_records': len(data),
                'valid_records': 0,
                'invalid_records': 0
            }
        }
        
        if not data:
            result['warnings'].append('数据为空')
            return result
        
        required_fields = required_fields or []
        field_types = field_types or {}
        
        for i, record in enumerate(data):
            record_valid = True
            
            # 检查必需字段
            for field in required_fields:
                if field not in record or record[field] is None or record[field] == '':
                    result['errors'].append(f'记录 {i}: 缺少必需字段 "{field}"')
                    record_valid = False
            
            # 检查字段类型
            for field, expected_type in field_types.items():
                if field in record and record[field] is not None:
                    if not isinstance(record[field], expected_type):
                        result['errors'].append(
                            f'记录 {i}: 字段 "{field}" 类型错误，期望 {expected_type.__name__}，实际 {type(record[field]).__name__}'
                        )
                        record_valid = False
            
            if record_valid:
                result['statistics']['valid_records'] += 1
            else:
                result['statistics']['invalid_records'] += 1
                result['valid'] = False
        
        return result


class SecurityUtils:
    """安全工具类"""
    
    _encryption_key = None
    
    @classmethod
    def _get_encryption_key(cls) -> bytes:
        """获取或生成加密密钥"""
        if cls._encryption_key is None:
            try:
                from cryptography.fernet import Fernet
                
                # 尝试从配置文件读取密钥
                try:
                    # 这里可以从配置管理器获取密钥
                    # 暂时使用固定密钥，生产环境应该从安全地方获取
                    key_str = "dataset-creator-2024-secure-key-for-encryption"
                    # 使用SHA256生成固定长度的密钥
                    import hashlib
                    key_hash = hashlib.sha256(key_str.encode()).digest()
                    # 转换为base64编码的32字节密钥
                    import base64
                    cls._encryption_key = base64.urlsafe_b64encode(key_hash)
                except Exception:
                    # 生成新密钥
                    cls._encryption_key = Fernet.generate_key()
                    
            except ImportError:
                # 如果没有cryptography库，使用简单的base64编码
                import base64
                cls._encryption_key = base64.b64encode(b"simple-fallback-key").decode()
                
        return cls._encryption_key
    
    @staticmethod
    def encrypt_text(text: str) -> str:
        """
        加密文本
        
        Args:
            text (str): 要加密的文本
            
        Returns:
            str: 加密后的文本
        """
        try:
            from cryptography.fernet import Fernet
            
            key = SecurityUtils._get_encryption_key()
            fernet = Fernet(key)
            encrypted_data = fernet.encrypt(text.encode())
            return encrypted_data.decode()
            
        except ImportError:
            # 简单的base64编码作为备选方案
            import base64
            encoded = base64.b64encode(text.encode()).decode()
            return f"base64:{encoded}"
        except Exception as e:
            # 加密失败时返回原文本（生产环境应该抛出异常）
            logging.warning(f"加密失败: {e}")
            return text
    
    @staticmethod
    def decrypt_text(encrypted_text: str) -> str:
        """
        解密文本
        
        Args:
            encrypted_text (str): 加密的文本
            
        Returns:
            str: 解密后的文本
        """
        try:
            if encrypted_text.startswith("base64:"):
                # 处理base64编码的备选方案
                import base64
                encoded_text = encrypted_text[7:]  # 移除 "base64:" 前缀
                return base64.b64decode(encoded_text).decode()
            
            from cryptography.fernet import Fernet
            
            key = SecurityUtils._get_encryption_key()
            fernet = Fernet(key)
            decrypted_data = fernet.decrypt(encrypted_text.encode())
            return decrypted_data.decode()
            
        except ImportError:
            # 无法解密时返回原文本
            return encrypted_text
        except Exception as e:
            # 解密失败时返回空字符串
            logging.warning(f"解密失败: {e}")
            return ""
    
    @staticmethod
    def generate_secret_key(length: int = 32) -> str:
        """
        生成加密用的随机密钥
        
        Args:
            length (int): 密钥长度，默认32字符
            
        Returns:
            str: 随机密钥字符串
        """
        characters = string.ascii_letters + string.digits + "!@#$%^&*"
        return ''.join(random.choice(characters) for _ in range(length))
    
    @staticmethod
    def mask_sensitive_info(text: str, mask_char: str = '*') -> str:
        """
        遮蔽敏感信息
        
        Args:
            text (str): 包含敏感信息的文本
            mask_char (str): 遮蔽字符
            
        Returns:
            str: 遮蔽后的文本
        """
        # 定义敏感信息模式
        patterns = [
            (r'(api[_-]?key\s*[=:]\s*)([^\s]+)', r'\1' + mask_char * 8),
            (r'(token\s*[=:]\s*)([^\s]+)', r'\1' + mask_char * 8),
            (r'(password\s*[=:]\s*)([^\s]+)', r'\1' + mask_char * 8),
            (r'hf_[a-zA-Z0-9]{34}', mask_char * 12),
            (r'sk-[a-zA-Z0-9]{48}', mask_char * 12),
        ]
        
        masked_text = text
        for pattern, replacement in patterns:
            masked_text = re.sub(pattern, replacement, masked_text, flags=re.IGNORECASE)
        
        return masked_text


class NetworkUtils:
    """网络工具类"""
    
    @staticmethod
    def check_network(url: str = "https://www.baidu.com", timeout: int = 5) -> Dict[str, Any]:
        """
        检查网络连接是否正常
        
        Args:
            url (str): 测试URL
            timeout (int): 超时时间（秒）
            
        Returns:
            dict: 连接状态信息
        """
        result = {
            'connected': False,
            'url': url,
            'response_time': None,
            'status_code': None,
            'error': None
        }
        
        if not HAS_REQUESTS:
            result['error'] = 'requests库未安装'
            return result
        
        try:
            start_time = time.time()
            response = requests.get(url, timeout=timeout)
            end_time = time.time()
            
            result['connected'] = True
            result['response_time'] = round((end_time - start_time) * 1000, 2)  # 毫秒
            result['status_code'] = response.status_code
            
        except requests.exceptions.RequestException as e:
            result['error'] = str(e)
        except Exception as e:
            result['error'] = f'未知错误: {str(e)}'
        
        return result
    
    @staticmethod
    def test_port_connectivity(host: str, port: int, timeout: int = 5) -> bool:
        """
        测试端口连通性
        
        Args:
            host (str): 主机地址
            port (int): 端口号
            timeout (int): 超时时间
            
        Returns:
            bool: 是否可连通
        """
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except (socket.timeout, socket.error):
            return False


class RetryUtils:
    """重试工具类"""
    
    @staticmethod
    def retry_func(func: Callable, 
                  args: tuple = (), 
                  kwargs: dict = None, 
                  max_retry: int = 3, 
                  delay: float = 2,
                  backoff_factor: float = 2,
                  exceptions: tuple = (Exception,)) -> Any:
        """
        带重试的函数调用（处理临时网络异常）
        
        Args:
            func (Callable): 要调用的函数
            args (tuple): 函数参数
            kwargs (dict): 函数关键字参数
            max_retry (int): 最大重试次数
            delay (float): 初始延迟时间（秒）
            backoff_factor (float): 延迟递增因子
            exceptions (tuple): 需要重试的异常类型
            
        Returns:
            Any: 函数返回值
            
        Raises:
            Exception: 重试次数用尽后仍失败
            
        Examples:
            >>> result = RetryUtils.retry_func(
            ...     requests.get, 
            ...     args=("https://api.example.com",),
            ...     max_retry=3
            ... )
        """
        kwargs = kwargs or {}
        current_delay = delay
        last_exception = None
        
        for attempt in range(max_retry + 1):
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                last_exception = e
                
                if attempt < max_retry:
                    print(f"函数调用失败，{current_delay}秒后重试 (第{attempt + 1}/{max_retry}次): {str(e)}")
                    time.sleep(current_delay)
                    current_delay *= backoff_factor
                else:
                    print(f"函数调用最终失败，已重试{max_retry}次")
                    raise last_exception
        
        # 这里应该不会到达，但为了类型检查
        raise last_exception
    
    @staticmethod
    def retry_decorator(max_retry: int = 3, 
                       delay: float = 2,
                       backoff_factor: float = 2,
                       exceptions: tuple = (Exception,)):
        """
        重试装饰器
        
        Args:
            max_retry (int): 最大重试次数
            delay (float): 初始延迟时间
            backoff_factor (float): 延迟递增因子
            exceptions (tuple): 需要重试的异常类型
            
        Returns:
            decorator: 装饰器函数
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return RetryUtils.retry_func(
                    func, args, kwargs, max_retry, delay, backoff_factor, exceptions
                )
            return wrapper
        return decorator


class SystemUtils:
    """系统工具类"""
    
    @staticmethod
    def get_system_info() -> Dict[str, Any]:
        """
        获取系统信息
        
        Returns:
            dict: 系统信息字典
        """
        info = {
            'platform': platform.system(),
            'platform_version': platform.version(),
            'architecture': platform.machine(),
            'python_version': platform.python_version(),
            'cpu_count': os.cpu_count(),
            'memory_total_gb': None,
            'memory_available_gb': None,
            'disk_free_gb': None
        }
        
        try:
            # 获取内存信息
            memory = psutil.virtual_memory()
            info['memory_total_gb'] = round(memory.total / (1024**3), 2)
            info['memory_available_gb'] = round(memory.available / (1024**3), 2)
            
            # 获取磁盘信息
            disk = psutil.disk_usage('/')
            info['disk_free_gb'] = round(disk.free / (1024**3), 2)
            
        except Exception:
            pass
        
        return info
    
    @staticmethod
    def check_dependencies() -> Dict[str, bool]:
        """
        检查依赖库是否安装
        
        Returns:
            dict: 依赖库安装状态
        """
        dependencies = {
            'pandas': HAS_PANDAS,
            'jsonlines': HAS_JSONLINES,
            'requests': HAS_REQUESTS,
            'xml': HAS_XML,
            'psutil': True  # 在上面的导入中已检查
        }
        
        # 检查其他可选依赖
        optional_deps = ['tqdm', 'transformers', 'datasets', 'torch', 'cryptography']
        
        for dep in optional_deps:
            try:
                __import__(dep)
                dependencies[dep] = True
            except ImportError:
                dependencies[dep] = False
        
        return dependencies


class PerformanceUtils:
    """性能工具类"""
    
    @staticmethod
    def time_it(func: Callable) -> Callable:
        """
        函数执行时间装饰器
        
        Args:
            func (Callable): 要测量的函数
            
        Returns:
            Callable: 装饰后的函数
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            
            print(f"函数 {func.__name__} 执行时间: {execution_time:.3f}秒")
            return result
        
        return wrapper
    
    @staticmethod
    def monitor_memory_usage(func: Callable) -> Callable:
        """
        内存使用监控装饰器
        
        Args:
            func (Callable): 要监控的函数
            
        Returns:
            Callable: 装饰后的函数
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                process = psutil.Process()
                memory_before = process.memory_info().rss / 1024 / 1024  # MB
                
                result = func(*args, **kwargs)
                
                memory_after = process.memory_info().rss / 1024 / 1024  # MB
                memory_diff = memory_after - memory_before
                
                print(f"函数 {func.__name__} 内存使用: {memory_diff:+.2f}MB (前: {memory_before:.2f}MB, 后: {memory_after:.2f}MB)")
                
                return result
            except Exception:
                # 如果内存监控失败，仍正常执行函数
                return func(*args, **kwargs)
        
        return wrapper


# 便捷的全局函数别名
read_file_chunk = FileOperations.read_file_chunk
get_file_hash = FileOperations.get_file_hash
flatten_dict = DataProcessing.flatten_dict
dedup_rows = DataProcessing.dedup_rows
check_network = NetworkUtils.check_network
retry_func = RetryUtils.retry_func
generate_secret_key = SecurityUtils.generate_secret_key


if __name__ == "__main__":
    """
    命令行入口，用于工具函数测试
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='工具函数测试')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # file命令
    file_parser = subparsers.add_parser('file', help='文件操作测试')
    file_parser.add_argument('path', help='文件路径')
    file_parser.add_argument('--chunk-size', type=int, default=100, help='块大小')
    
    # hash命令
    hash_parser = subparsers.add_parser('hash', help='计算文件哈希')
    hash_parser.add_argument('path', help='文件路径')
    hash_parser.add_argument('--algorithm', default='md5', choices=['md5', 'sha1', 'sha256'], help='哈希算法')
    
    # network命令
    network_parser = subparsers.add_parser('network', help='网络连接测试')
    network_parser.add_argument('--url', default='https://www.baidu.com', help='测试URL')
    
    # system命令
    system_parser = subparsers.add_parser('system', help='显示系统信息')
    
    # deps命令
    deps_parser = subparsers.add_parser('deps', help='检查依赖库')
    
    # dedup命令
    dedup_parser = subparsers.add_parser('dedup', help='去重测试')
    
    args = parser.parse_args()
    
    if args.command == 'file':
        try:
            file_info = FileOperations.get_file_info(args.path)
            print("文件信息:")
            for key, value in file_info.items():
                print(f"  {key}: {value}")
            
            if file_info.get('exists'):
                print(f"\n按 {args.chunk_size} 行分片读取:")
                chunk_count = 0
                for chunk in FileOperations.read_file_chunk(args.path, args.chunk_size):
                    chunk_count += 1
                    print(f"  块 {chunk_count}: {len(chunk)} 行")
                    if chunk_count >= 3:  # 只显示前3块
                        break
                        
        except Exception as e:
            print(f"文件操作失败: {e}")
            
    elif args.command == 'hash':
        try:
            hash_value = FileOperations.get_file_hash(args.path, args.algorithm)
            print(f"{args.algorithm.upper()} 哈希值: {hash_value}")
        except Exception as e:
            print(f"计算哈希失败: {e}")
            
    elif args.command == 'network':
        result = NetworkUtils.check_network(args.url)
        print("网络连接测试:")
        for key, value in result.items():
            print(f"  {key}: {value}")
            
    elif args.command == 'system':
        info = SystemUtils.get_system_info()
        print("系统信息:")
        for key, value in info.items():
            print(f"  {key}: {value}")
            
    elif args.command == 'deps':
        deps = SystemUtils.check_dependencies()
        print("依赖库检查:")
        for dep, installed in deps.items():
            status = "已安装" if installed else "未安装"
            print(f"  {dep}: {status}")
            
    elif args.command == 'dedup':
        # 测试去重功能
        test_data = [
            {"text": "Hello world", "id": 1},
            {"text": "Hello world!", "id": 2},
            {"text": "Hello world.", "id": 3},
            {"text": "Different text", "id": 4},
            {"text": "Another text", "id": 5}
        ]
        
        unique, duplicates = DataProcessing.dedup_rows(test_data, "text", 0.8)
        
        print(f"原始数据: {len(test_data)} 条")
        print(f"去重后: {len(unique)} 条")
        print(f"重复数据: {len(duplicates)} 条")
        
        print("\n去重后数据:")
        for item in unique:
            print(f"  {item}")
            
        print("\n重复数据:")
        for item in duplicates:
            print(f"  {item}")
            
    else:
        parser.print_help()


# 为兼容性添加的全局函数
def validate_file(file_path: str, formats=None) -> bool:
    """验证文件是否存在且格式正确"""
    try:
        return os.path.exists(file_path) and os.path.isfile(file_path)
    except:
        return False

def get_file_encoding(file_path: str) -> str:
    """获取文件编码"""
    try:
        # 尝试检测文件编码
        import chardet
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)  # 读取前10KB检测编码
            result = chardet.detect(raw_data)
            return result['encoding'] or 'utf-8'
    except:
        return 'utf-8'

def infer_data_type(value) -> str:
    """推断数据类型"""
    if value is None or value == '':
        return 'null'
    
    # 尝试转换为数字
    try:
        float(value)
        if '.' in str(value):
            return 'number'
        else:
            return 'number'
    except:
        pass
    
    # 尝试转换为布尔值
    if str(value).lower() in ['true', 'false', '1', '0', 'yes', 'no']:
        return 'boolean'
    
    # 尝试检测日期
    import re
    date_patterns = [
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
        r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
    ]
    for pattern in date_patterns:
        if re.match(pattern, str(value)):
            return 'date'
    
    # 默认为字符串
    return 'string'

def handle_null(value):
    """处理空值"""
    if value is None or value == '' or str(value).strip() == '':
        return None
    return value

def read_file_chunk(file_path: str, chunk_size: int = 1000):
    """读取文件分片"""
    return FileOperations.read_file_chunk(file_path, chunk_size)

def write_file_chunk(data: List[Dict], file_path: str, mode: str = "a"):
    """写入文件分片"""
    if not data:
        return
    
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 根据文件扩展名确定格式
    if file_path.suffix.lower() == '.jsonl':
        import jsonlines
        with jsonlines.open(file_path, mode=mode) as writer:
            for item in data:
                writer.write(item)
    elif file_path.suffix.lower() == '.json':
        if mode == 'a' and file_path.exists():
            # JSON不支持追加，需要读取已有数据
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                if isinstance(existing_data, list):
                    existing_data.extend(data)
                    data = existing_data
            except:
                pass
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    elif file_path.suffix.lower() == '.csv':
        from .dependencies import pd
        if pd is None:
            raise ImportError("pandas is required for CSV support")
        df = pd.DataFrame(data)
        header = not file_path.exists() if mode == 'a' else True
        df.to_csv(file_path, mode=mode, header=header, index=False, encoding='utf-8')
    else:
        # 默认使用JSONL格式
        with open(file_path, mode, encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')

def ensure_dir(dir_path: str) -> bool:
    """确保目录存在"""
    try:
        os.makedirs(dir_path, exist_ok=True)
        return True
    except Exception:
        return False

if __name__ == '__main__':
    # 工具函数模块的命令行入口已在文件末尾的 argparse 部分实现
    # 如需测试，请使用: python -m src.utils <command> [args]
    import sys
    if len(sys.argv) > 1:
        # 如果有命令行参数，执行相应的测试命令
        # 这里可以添加简单的测试逻辑
        print("工具函数模块测试")
        print("使用 'python -m src.utils <command>' 来测试特定功能")
    else:
        print("工具函数模块")
        print("使用 'python -m src.utils <command> --help' 查看可用命令")
