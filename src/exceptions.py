#!/usr/bin/env python3
"""
统一异常类模块

本模块定义了项目中使用的所有异常类，提供统一的错误处理机制。
功能特点：
- 统一的异常基类
- 各模块专用异常类
- 错误码体系
- 详细的错误信息

设计原则：
- 异常层次清晰，便于捕获和处理
- 提供详细的错误信息，便于调试
- 支持错误码，便于国际化
- 所有异常都继承自基础异常类

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

from typing import Optional, Dict, Any


class DatasetCreatorException(Exception):
    """
    项目基础异常类
    
    所有项目异常都继承自此类，提供统一的异常处理接口。
    """
    
    def __init__(self, message: str, error_code: str = None, details: Dict[str, Any] = None):
        """
        初始化异常
        
        Args:
            message: 错误消息
            error_code: 错误码（可选）
            details: 详细信息字典（可选）
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "UNKNOWN_ERROR"
        self.details = details or {}
    
    def __str__(self) -> str:
        """返回异常字符串表示"""
        if self.error_code:
            return f"[{self.error_code}] {self.message}"
        return self.message
    
    def to_dict(self) -> Dict[str, Any]:
        """将异常转换为字典"""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details
        }


# ============== 配置管理异常 ==============

class ConfigError(DatasetCreatorException):
    """配置管理相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "CONFIG_ERROR", details)


class ConfigNotFoundError(ConfigError):
    """配置文件不存在异常"""
    
    def __init__(self, config_path: str):
        super().__init__(
            f"配置文件不存在: {config_path}",
            {"config_path": config_path}
        )


class ConfigFormatError(ConfigError):
    """配置文件格式错误异常"""
    
    def __init__(self, config_path: str, error: str):
        super().__init__(
            f"配置文件格式错误: {config_path} - {error}",
            {"config_path": config_path, "error": error}
        )


class ConfigValidationError(ConfigError):
    """配置验证失败异常"""
    
    def __init__(self, key: str, value: Any, reason: str):
        super().__init__(
            f"配置项验证失败: {key} = {value} - {reason}",
            {"key": key, "value": value, "reason": reason}
        )


# ============== 日志管理异常 ==============

class LogError(DatasetCreatorException):
    """日志管理相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "LOG_ERROR", details)


class LogFileError(LogError):
    """日志文件操作异常"""
    
    def __init__(self, log_path: str, error: str):
        super().__init__(
            f"日志文件操作失败: {log_path} - {error}",
            {"log_path": log_path, "error": error}
        )


# ============== 状态管理异常 ==============

class StateError(DatasetCreatorException):
    """状态管理相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "STATE_ERROR", details)


class TaskNotFoundError(StateError):
    """任务不存在异常"""
    
    def __init__(self, task_id: str):
        super().__init__(
            f"任务不存在: {task_id}",
            {"task_id": task_id}
        )


class TaskStateError(StateError):
    """任务状态错误异常"""
    
    def __init__(self, task_id: str, current_state: str, expected_state: str = None):
        message = f"任务状态错误: {task_id} (当前状态: {current_state})"
        if expected_state:
            message += f", 期望状态: {expected_state}"
        super().__init__(
            message,
            {"task_id": task_id, "current_state": current_state, "expected_state": expected_state}
        )


# ============== 数据下载异常 ==============

class DownloadError(DatasetCreatorException):
    """数据下载相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "DOWNLOAD_ERROR", details)


class DownloadTimeoutError(DownloadError):
    """下载超时异常"""
    
    def __init__(self, source: str, timeout: int):
        super().__init__(
            f"下载超时: {source} (超时时间: {timeout}秒)",
            {"source": source, "timeout": timeout}
        )


class DownloadFailedError(DownloadError):
    """下载失败异常"""
    
    def __init__(self, source: str, reason: str):
        super().__init__(
            f"下载失败: {source} - {reason}",
            {"source": source, "reason": reason}
        )


class UnsupportedSourceError(DownloadError):
    """不支持的数据源异常"""
    
    def __init__(self, source_type: str):
        super().__init__(
            f"不支持的数据源类型: {source_type}",
            {"source_type": source_type}
        )


# ============== 格式转换异常 ==============

class ConvertError(DatasetCreatorException):
    """格式转换相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "CONVERT_ERROR", details)


class UnsupportedFormatError(ConvertError):
    """不支持的格式异常"""
    
    def __init__(self, format_type: str, direction: str = "转换"):
        super().__init__(
            f"不支持的格式: {format_type} ({direction})",
            {"format_type": format_type, "direction": direction}
        )


class ConvertFailedError(ConvertError):
    """转换失败异常"""
    
    def __init__(self, source_path: str, target_format: str, reason: str):
        super().__init__(
            f"格式转换失败: {source_path} -> {target_format} - {reason}",
            {"source_path": source_path, "target_format": target_format, "reason": reason}
        )


# ============== 字段提取异常 ==============

class ExtractError(DatasetCreatorException):
    """字段提取相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "EXTRACT_ERROR", details)


class FieldNotFoundError(ExtractError):
    """字段不存在异常"""
    
    def __init__(self, field_name: str, available_fields: list = None):
        message = f"字段不存在: {field_name}"
        details = {"field_name": field_name}
        if available_fields:
            message += f" (可用字段: {', '.join(available_fields)})"
            details["available_fields"] = available_fields
        super().__init__(message, details)


class ExtractFailedError(ExtractError):
    """提取失败异常"""
    
    def __init__(self, source_path: str, reason: str):
        super().__init__(
            f"字段提取失败: {source_path} - {reason}",
            {"source_path": source_path, "reason": reason}
        )


# ============== 数据合并异常 ==============

class MergeError(DatasetCreatorException):
    """数据合并相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "MERGE_ERROR", details)


class SchemaMismatchError(MergeError):
    """数据结构不匹配异常"""
    
    def __init__(self, file1: str, file2: str, field: str = None):
        message = f"数据结构不匹配: {file1} 和 {file2}"
        details = {"file1": file1, "file2": file2}
        if field:
            message += f" (字段: {field})"
            details["field"] = field
        super().__init__(message, details)


class MergeFailedError(MergeError):
    """合并失败异常"""
    
    def __init__(self, reason: str, files: list = None):
        super().__init__(
            f"数据合并失败: {reason}",
            {"reason": reason, "files": files or []}
        )


# ============== 数据清洗异常 ==============

class CleanError(DatasetCreatorException):
    """数据清洗相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "CLEAN_ERROR", details)


class CleanFailedError(CleanError):
    """清洗失败异常"""
    
    def __init__(self, source_path: str, reason: str):
        super().__init__(
            f"数据清洗失败: {source_path} - {reason}",
            {"source_path": source_path, "reason": reason}
        )


# ============== 模型管理异常 ==============

class ModelError(DatasetCreatorException):
    """模型管理相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "MODEL_ERROR", details)


class ModelNotFoundError(ModelError):
    """模型不存在异常"""
    
    def __init__(self, model_name: str):
        super().__init__(
            f"模型不存在: {model_name}",
            {"model_name": model_name}
        )


class ModelConnectionError(ModelError):
    """模型连接失败异常"""
    
    def __init__(self, model_name: str, reason: str):
        super().__init__(
            f"模型连接失败: {model_name} - {reason}",
            {"model_name": model_name, "reason": reason}
        )


class ModelTimeoutError(ModelError):
    """模型调用超时异常"""
    
    def __init__(self, model_name: str, timeout: int):
        super().__init__(
            f"模型调用超时: {model_name} (超时时间: {timeout}秒)",
            {"model_name": model_name, "timeout": timeout}
        )


# ============== 蒸馏生成异常 ==============

class DistillError(DatasetCreatorException):
    """蒸馏生成相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "DISTILL_ERROR", details)


class DistillFailedError(DistillError):
    """蒸馏生成失败异常"""
    
    def __init__(self, reason: str, source_path: str = None):
        message = f"蒸馏生成失败: {reason}"
        details = {"reason": reason}
        if source_path:
            details["source_path"] = source_path
        super().__init__(message, details)


# ============== 数据管理异常 ==============

class DataManagerError(DatasetCreatorException):
    """数据管理相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "DATA_MANAGER_ERROR", details)


class DataNotFoundError(DataManagerError):
    """数据不存在异常"""
    
    def __init__(self, data_path: str):
        super().__init__(
            f"数据不存在: {data_path}",
            {"data_path": data_path}
        )


class DataAccessError(DataManagerError):
    """数据访问异常"""
    
    def __init__(self, data_path: str, reason: str):
        super().__init__(
            f"数据访问失败: {data_path} - {reason}",
            {"data_path": data_path, "reason": reason}
        )


# ============== 文件操作异常 ==============

class FileOperationError(DatasetCreatorException):
    """文件操作相关异常"""
    
    def __init__(self, message: str, details: Dict[str, Any] = None):
        super().__init__(message, "FILE_OPERATION_ERROR", details)


class FileNotFoundError(FileOperationError):
    """文件不存在异常"""
    
    def __init__(self, file_path: str):
        super().__init__(
            f"文件不存在: {file_path}",
            {"file_path": file_path}
        )


class FileReadError(FileOperationError):
    """文件读取异常"""
    
    def __init__(self, file_path: str, reason: str):
        super().__init__(
            f"文件读取失败: {file_path} - {reason}",
            {"file_path": file_path, "reason": reason}
        )


class FileWriteError(FileOperationError):
    """文件写入异常"""
    
    def __init__(self, file_path: str, reason: str):
        super().__init__(
            f"文件写入失败: {file_path} - {reason}",
            {"file_path": file_path, "reason": reason}
        )


# ============== 工具函数 ==============

def handle_exception(e: Exception) -> Dict[str, Any]:
    """
    统一处理异常，转换为字典格式
    
    Args:
        e: 异常对象
        
    Returns:
        异常信息字典
    """
    if isinstance(e, DatasetCreatorException):
        return e.to_dict()
    else:
        return {
            "error_code": "UNKNOWN_ERROR",
            "message": str(e),
            "details": {
                "exception_type": type(e).__name__
            }
        }


