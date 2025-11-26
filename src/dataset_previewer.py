#!/usr/bin/env python3
"""
数据集预览器 - 支持多种格式的数据集预览功能

支持功能：
1. 多种格式：JSON、JSONL、CSV、TSV等
2. 大文件处理：流式读取、分页预览
3. 长文本处理：智能截断、展开功能
4. 多文件数据集：目录遍历、文件组合
5. 数据质量检查：格式验证、统计信息
"""

import json
import csv
from .dependencies import pd, datasets
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
import logging
from dataclasses import dataclass
from enum import Enum
import re

# 尝试导入ijson
try:
    import ijson
    HAS_IJSON = True
except ImportError:
    HAS_IJSON = False


class DatasetFormat(Enum):
    """数据集格式枚举"""
    JSON = "json"
    JSONL = "jsonl"
    CSV = "csv"
    TSV = "tsv"
    TXT = "txt"
    ARROW = "arrow"
    PARQUET = "parquet"
    UNKNOWN = "unknown"


@dataclass
class PreviewConfig:
    """预览配置"""
    max_rows: int = 100  # 最大预览行数
    max_files: int = 10  # 最大预览文件数
    max_text_length: int = 200  # 文本截断长度
    max_file_size_mb: int = 100  # 最大单文件大小(MB)
    enable_truncation: bool = True  # 启用文本截断
    show_stats: bool = True  # 显示统计信息
    include_metadata: bool = True  # 包含元数据
    smart_columns: bool = True  # 智能列显示
    show_all_columns: bool = False  # 显示所有列
    min_column_width: int = 20  # 最小列宽
    max_column_width: int = 100  # 最大列宽


@dataclass
class FileInfo:
    """文件信息"""
    path: str
    name: str
    size: int
    format: DatasetFormat
    encoding: str = "utf-8"
    row_count: Optional[int] = None
    columns: Optional[List[str]] = None


@dataclass
class PreviewResult:
    """预览结果"""
    success: bool
    data: List[Dict[str, Any]]
    files: List[FileInfo]
    total_rows: int
    total_files: int
    format: DatasetFormat
    metadata: Dict[str, Any]
    error_message: str = ""
    truncated_fields: List[str] = None  # 被截断的字段
    hidden_columns: List[str] = None  # 被隐藏的列
    column_info: Dict[str, Any] = None  # 列信息统计


class DatasetPreviewer:
    """数据集预览器主类"""
    
    def __init__(self, config: Optional[PreviewConfig] = None):
        self.config = config or PreviewConfig()
        self.logger = logging.getLogger(__name__)
        
    def preview_dataset(self, path: Union[str, Path], 
                       max_rows: Optional[int] = None) -> PreviewResult:
        """
        预览数据集
        
        Args:
            path: 数据集路径（文件或目录）
            max_rows: 最大预览行数（覆盖配置）
            
        Returns:
            PreviewResult: 预览结果
        """
        try:
            path = Path(path)
            if not path.exists():
                return PreviewResult(
                    success=False,
                    data=[],
                    files=[],
                    total_rows=0,
                    total_files=0,
                    format=DatasetFormat.UNKNOWN,
                    metadata={},
                    error_message=f"路径不存在: {path}"
                )
            
            # 使用传入的max_rows或配置中的值
            effective_max_rows = max_rows or self.config.max_rows
            
            if path.is_file():
                return self._preview_single_file(path, effective_max_rows)
            elif path.is_dir():
                return self._preview_directory(path, effective_max_rows)
            else:
                return PreviewResult(
                    success=False,
                    data=[],
                    files=[],
                    total_rows=0,
                    total_files=0,
                    format=DatasetFormat.UNKNOWN,
                    metadata={},
                    error_message=f"未知路径类型: {path}"
                )
                
        except Exception as e:
            self.logger.error(f"预览数据集失败: {e}")
            return PreviewResult(
                success=False,
                data=[],
                files=[],
                total_rows=0,
                total_files=0,
                format=DatasetFormat.UNKNOWN,
                metadata={},
                error_message=str(e)
            )
    
    def _preview_single_file(self, file_path: Path, max_rows: int) -> PreviewResult:
        """预览单个文件"""
        try:
            # 检查文件大小
            file_size = file_path.stat().st_size
            
            # 对于Arrow和Parquet文件，增大大小限制，因为它们通常较大但读取效率高
            # 对于支持流式读取的文本格式(JSONL, CSV, TSV, TXT)，也允许更大的文件限制
            suffix = file_path.suffix.lower()
            if suffix in {'.arrow', '.parquet'}:
                max_size_limit = self.config.max_file_size_mb * 100  # 增大100倍限制 (10GB)
            elif suffix in {'.jsonl', '.csv', '.tsv', '.txt'}:
                max_size_limit = self.config.max_file_size_mb * 200 # 增大200倍限制 (20GB) - 支持流式读取
            elif suffix == '.json':
                # 如果有ijson支持，JSON也可以流式读取，放宽限制
                if HAS_IJSON:
                    max_size_limit = self.config.max_file_size_mb * 100 # 10GB
                else:
                    max_size_limit = self.config.max_file_size_mb * 5 # 500MB
            else:
                max_size_limit = self.config.max_file_size_mb
            
            if file_size > max_size_limit * 1024 * 1024:
                return PreviewResult(
                    success=False,
                    data=[],
                    files=[],
                    total_rows=0,
                    total_files=0,
                    format=DatasetFormat.UNKNOWN,
                    metadata={},
                    error_message=f"文件过大: {file_size / (1024*1024):.1f}MB，超过限制 {max_size_limit}MB"
                )
            
            # 检测文件格式
            file_format = self._detect_format(file_path)
            
            # 获取文件信息
            file_info = FileInfo(
                path=str(file_path),
                name=file_path.name,
                size=file_size,
                format=file_format
            )
            
            # 根据格式读取数据
            if file_format == DatasetFormat.JSON:
                data, total_rows = self._read_json_file(file_path, max_rows)
            elif file_format == DatasetFormat.JSONL:
                data, total_rows = self._read_jsonl_file(file_path, max_rows)
            elif file_format in [DatasetFormat.CSV, DatasetFormat.TSV]:
                data, total_rows = self._read_csv_file(file_path, max_rows, file_format)
            elif file_format == DatasetFormat.TXT:
                data, total_rows = self._read_text_file(file_path, max_rows)
            elif file_format == DatasetFormat.ARROW:
                data, total_rows = self._read_arrow_file(file_path, max_rows)
            elif file_format == DatasetFormat.PARQUET:
                data, total_rows = self._read_parquet_file(file_path, max_rows)
            else:
                return PreviewResult(
                    success=False,
                    data=[],
                    files=[file_info],
                    total_rows=0,
                    total_files=1,
                    format=file_format,
                    metadata={},
                    error_message=f"不支持的文件格式: {file_format.value}"
                )
            
            # 处理长文本截断
            truncated_fields = []
            if self.config.enable_truncation:
                data, truncated_fields = self._apply_text_truncation(data)
            
            # 处理智能列显示
            hidden_columns = []
            column_info = {}
            if self.config.smart_columns and not self.config.show_all_columns:
                data, hidden_columns, column_info = self._apply_smart_columns(data)
            
            # 更新文件信息
            file_info.row_count = total_rows
            if data:
                file_info.columns = list(data[0].keys()) if isinstance(data[0], dict) else None
            
            # 生成元数据
            metadata = self._generate_metadata(file_info, data) if self.config.include_metadata else {}
            
            return PreviewResult(
                success=True,
                data=data,
                files=[file_info],
                total_rows=total_rows,
                total_files=1,
                format=file_format,
                metadata=metadata,
                truncated_fields=truncated_fields,
                hidden_columns=hidden_columns,
                column_info=column_info
            )
            
        except Exception as e:
            self.logger.error(f"预览文件失败 {file_path}: {e}")
            return PreviewResult(
                success=False,
                data=[],
                files=[],
                total_rows=0,
                total_files=0,
                format=DatasetFormat.UNKNOWN,
                metadata={},
                error_message=str(e)
            )
    
    def _preview_directory(self, dir_path: Path, max_rows: int) -> PreviewResult:
        """预览目录中的数据集"""
        try:
            # 扫描目录中的数据文件
            data_files = self._scan_directory(dir_path)
            
            if not data_files:
                return PreviewResult(
                    success=False,
                    data=[],
                    files=[],
                    total_rows=0,
                    total_files=0,
                    format=DatasetFormat.UNKNOWN,
                    metadata={},
                    error_message=f"目录中没有找到支持的数据文件: {dir_path}"
                )
            
            # 限制文件数量（增加扫描数量以提高找到有效文件的概率）
            # 进一步增加扫描限制，因为MegaScience等数据集可能包含大量非数据文件或分片
            scan_limit = self.config.max_files * 20 
            data_files = data_files[:scan_limit]
            
            all_data = []
            file_infos = []
            total_rows = 0
            main_format = DatasetFormat.UNKNOWN
            
            # 按文件读取数据
            # 优化行数分配策略：
            # 1. 如果文件数少于最大文件数，则每个文件分配更多行数，确保总数达到 max_rows
            # 2. 动态计算剩余需要的行数
            
            effective_file_count = min(len(data_files), self.config.max_files)
            if effective_file_count > 0:
                # 基础分配：保证每个文件至少读一些，但如果文件少，就多读点
                # 例如：max_rows=100, files=1 -> rows_per_file=100
                # max_rows=100, files=2 -> rows_per_file=50
                rows_per_file = max(1, max_rows // effective_file_count)
                # 稍微多读一点以防某些文件行数不足
                rows_per_file = int(rows_per_file * 1.2)
            else:
                rows_per_file = max_rows

            successful_files_count = 0
            
            # 记录尝试过的文件和错误，以便调试
            attempted_files = 0
            errors = []
            
            for file_path in data_files:
                # 如果已经读取了足够多的文件，停止
                if successful_files_count >= self.config.max_files:
                    break
                
                # 如果已经收集了足够的数据，也可以停止（可选，但为了多样性通常继续读取）
                if len(all_data) >= max_rows * 1.5: # 收集多一点再截断
                    break

                attempted_files += 1
                try:
                    # 动态调整本次读取行数：如果之前读的不够，这次多读点
                    remaining_rows = max_rows - len(all_data)
                    if remaining_rows <= 0:
                        current_limit = rows_per_file # 即使够了也读一点，保持多样性
                    else:
                        # 如果是最后一个文件，尝试读完剩余所需
                        if successful_files_count == effective_file_count - 1:
                            current_limit = remaining_rows
                        else:
                            current_limit = rows_per_file

                    result = self._preview_single_file(file_path, max(current_limit, 10))
                    if result.success:
                        all_data.extend(result.data)
                        file_infos.extend(result.files)
                        total_rows += result.total_rows
                        successful_files_count += 1
                        if main_format == DatasetFormat.UNKNOWN:
                            main_format = result.format
                    else:
                        # 记录失败原因，但只记录前几个
                        if len(errors) < 3:
                            errors.append(f"{file_path.name}: {result.error_message}")
                except Exception as e:
                    self.logger.warning(f"跳过文件 {file_path}: {e}")
                    if len(errors) < 3:
                        errors.append(f"{file_path.name}: {str(e)}")
                    continue
            
            if not all_data and not file_infos:
                 error_detail = "; ".join(errors) if errors else "无详细错误"
                 return PreviewResult(
                    success=False,
                    data=[],
                    files=[],
                    total_rows=0,
                    total_files=0,
                    format=DatasetFormat.UNKNOWN,
                    metadata={},
                    error_message=f"无法预览目录中的任何文件 (尝试了 {attempted_files} 个文件, 扫描了 {len(data_files)} 个文件). 错误示例: {error_detail}"
                )
            
            # 限制总行数
            if len(all_data) > max_rows:
                all_data = all_data[:max_rows]
            
            # 对合并后的数据应用智能列处理
            hidden_columns = []
            column_info = {}
            truncated_fields = []
            
            if all_data:
                # 应用文本截断
                if self.config.enable_truncation:
                    all_data, truncated_fields = self._apply_text_truncation(all_data)
                
                # 应用智能列显示
                if self.config.smart_columns and not self.config.show_all_columns:
                    all_data, hidden_columns, column_info = self._apply_smart_columns(all_data)
            
            # 生成目录级元数据
            metadata = {
                'directory': str(dir_path),
                'scanned_files': len(data_files),
                'successful_files': len(file_infos),
                'formats': list(set(f.format.value for f in file_infos))
            }
            
            return PreviewResult(
                success=True,
                data=all_data,
                files=file_infos,
                total_rows=total_rows,
                total_files=len(file_infos),
                format=main_format,
                metadata=metadata,
                hidden_columns=hidden_columns,
                column_info=column_info,
                truncated_fields=truncated_fields
            )
            
        except Exception as e:
            self.logger.error(f"预览目录失败 {dir_path}: {e}")
            return PreviewResult(
                success=False,
                data=[],
                files=[],
                total_rows=0,
                total_files=0,
                format=DatasetFormat.UNKNOWN,
                metadata={},
                error_message=str(e)
            )
    
    def _detect_format(self, file_path: Path) -> DatasetFormat:
        """检测文件格式"""
        suffix = file_path.suffix.lower()
        
        if suffix == '.json':
            return DatasetFormat.JSON
        elif suffix == '.jsonl':
            return DatasetFormat.JSONL
        elif suffix == '.csv':
            return DatasetFormat.CSV
        elif suffix == '.tsv':
            return DatasetFormat.TSV
        elif suffix == '.txt':
            return DatasetFormat.TXT
        elif suffix == '.arrow':
            return DatasetFormat.ARROW
        elif suffix == '.parquet':
            return DatasetFormat.PARQUET
        else:
            # 尝试通过内容检测
            try:
                with open(file_path, 'rb') as f:
                    # 读取开头部分字节
                    chunk = f.read(1024)
                    
                    # 检查是否包含 NULL 字节，如果是则视为二进制文件，不支持
                    if b'\x00' in chunk:
                        return DatasetFormat.UNKNOWN
                        
                    # 尝试解码
                    try:
                        text = chunk.decode('utf-8').strip()
                    except UnicodeDecodeError:
                        return DatasetFormat.UNKNOWN
                        
                    first_line = text.split('\n')[0].strip()
                    if first_line.startswith('{') and first_line.endswith('}'):
                        return DatasetFormat.JSONL
                    elif ',' in first_line or '\t' in first_line:
                        return DatasetFormat.CSV
            except:
                pass
            
            return DatasetFormat.UNKNOWN
    
    def _read_json_file(self, file_path: Path, max_rows: int) -> Tuple[List[Dict], int]:
        """读取JSON文件"""
        # 如果有ijson，使用流式读取以支持大文件
        if HAS_IJSON:
            try:
                data = []
                with open(file_path, 'rb') as f:
                    # 尝试检测是否是列表
                    pos = f.tell()
                    first_char = f.read(1)
                    while first_char and first_char.isspace():
                        first_char = f.read(1)
                    f.seek(pos)
                    
                    if first_char == b'[':
                        # 列表模式：流式读取前max_rows个元素
                        items = ijson.items(f, 'item')
                        for i, item in enumerate(items):
                            if i >= max_rows:
                                break
                            data.append(item)
                        
                        # 无法轻易获取总行数，除非遍历整个文件
                        # 这里返回-1表示未知，或者如果读取完了就是当前数量
                        total_rows = len(data) if len(data) < max_rows else -1
                        return data, total_rows
                    else:
                        # 对象模式：读取整个对象
                        # 如果文件太大，这里可能会失败，但对于非列表JSON，通常不是大数据集格式
                        f.seek(0)
                        data_obj = json.load(f)
                        if isinstance(data_obj, list):
                            return data_obj[:max_rows], len(data_obj)
                        else:
                            return [data_obj], 1
            except Exception as e:
                self.logger.warning(f"使用ijson读取JSON失败，回退到普通模式: {e}")
                # 回退到普通模式
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            total_rows = len(data)
            return data[:max_rows], total_rows
        elif isinstance(data, dict):
            # 单个对象，转为列表
            return [data], 1
        else:
            # 其他类型，包装为字典
            return [{'value': data}], 1
    
    def _read_jsonl_file(self, file_path: Path, max_rows: int) -> Tuple[List[Dict], int]:
        """读取JSONL文件"""
        data = []
        total_rows = 0
        
        # 检查文件大小，如果太大则不计算总行数
        file_size = file_path.stat().st_size
        skip_count = file_size > 100 * 1024 * 1024  # 100MB
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                for line_num, line in enumerate(f):
                    # 检查是否包含 NULL 字符，如果是则跳过（可能是损坏的数据）
                    if '\x00' in line:
                        continue
                        
                    if skip_count and len(data) >= max_rows:
                        total_rows = -1 # 表示未知/未计算
                        break
                    
                    total_rows += 1
                    if len(data) >= max_rows:
                        continue  # 继续计数但不加载数据
                    
                    line = line.strip()
                    if line:
                        try:
                            item = json.loads(line)
                            data.append(item)
                        except json.JSONDecodeError as e:
                            # 仅在未达到最大行数时记录警告，避免日志爆炸
                            if len(data) < max_rows:
                                self.logger.warning(f"跳过无效JSON行 {line_num + 1}: {e}")
        except Exception as e:
            self.logger.error(f"读取JSONL文件出错: {e}")
            
        return data, total_rows
    
    def _read_csv_file(self, file_path: Path, max_rows: int, 
                      file_format: DatasetFormat) -> Tuple[List[Dict], int]:
        """读取CSV/TSV文件"""
        delimiter = '\t' if file_format == DatasetFormat.TSV else ','
        
        # 检查文件大小
        file_size = file_path.stat().st_size
        skip_count = file_size > 100 * 1024 * 1024  # 100MB
        
        total_rows = -1
        if not skip_count:
            # 先计算总行数
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    total_rows = sum(1 for _ in f) - 1  # 减去标题行
            except Exception:
                pass
        
        # 读取数据
        try:
            # 使用 on_bad_lines='skip' 避免格式错误导致崩溃
            df = pd.read_csv(file_path, delimiter=delimiter, nrows=max_rows, on_bad_lines='skip')
            # 转换为字典列表
            data = df.to_dict('records')
        except Exception as e:
            self.logger.error(f"读取CSV失败: {e}")
            data = []
        
        return data, total_rows if total_rows != -1 else -1
    
    def _read_text_file(self, file_path: Path, max_rows: int) -> Tuple[List[Dict], int]:
        """读取文本文件"""
        data = []
        total_rows = 0
        
        # 检查文件大小
        file_size = file_path.stat().st_size
        skip_count = file_size > 100 * 1024 * 1024  # 100MB
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f):
                if skip_count and len(data) >= max_rows:
                    total_rows = -1
                    break

                total_rows += 1
                if len(data) >= max_rows:
                    continue
                
                data.append({
                    'line_number': line_num + 1,
                    'content': line.rstrip('\n\r')
                })
        
        return data, total_rows
    
    def _read_arrow_file(self, file_path: Path, max_rows: int) -> Tuple[List[Dict], int]:
        """读取Arrow文件"""
        try:
            # 优先尝试使用HuggingFace datasets库（最佳方法）
            if datasets:
                dataset = datasets.Dataset.from_file(str(file_path))
                
                # 获取总行数
                total_rows = len(dataset)
                
                # 限制行数并转换为字典列表
                if max_rows > 0:
                    sample = dataset.select(range(min(max_rows, total_rows)))
                    data = []
                    for i in range(len(sample)):
                        item = {}
                        for col in dataset.column_names:
                            item[col] = sample[col][i]
                        data.append(item)
                else:
                    data = []
                    for i in range(total_rows):
                        item = {}
                        for col in dataset.column_names:
                            item[col] = dataset[col][i]
                        data.append(item)
                
                return data, total_rows
                
            # 如果没有datasets库，使用PyArrow
            from .dependencies import pa, pq
            
            if pa is None:
                 raise ImportError("PyArrow is required for Arrow files")
            
            # 尝试不同的Arrow文件读取方法
            table = None
            
            # 方法1: 标准Arrow IPC文件
            try:
                with pa.ipc.open_file(str(file_path)) as reader:
                    table = reader.read_all()
            except:
                pass
            
            # 方法2: 尝试作为Feather文件读取
            if table is None:
                try:
                    import pyarrow.feather as feather
                    table = feather.read_table(str(file_path))
                except:
                    pass
            
            # 方法3: 尝试内存映射读取
            if table is None:
                try:
                    with pa.memory_map(str(file_path), 'r') as source:
                        table = pa.ipc.open_file(source).read_all()
                except:
                    pass
            
            if table is None:
                raise ValueError("无法读取Arrow文件，建议安装datasets库以获得更好的支持")
            
            # 转换为pandas DataFrame
            df = table.to_pandas()
            
            # 获取总行数
            total_rows = len(df)
            
            # 限制行数
            if max_rows > 0:
                df = df.head(max_rows)
            
            # 转换为字典列表
            data = df.to_dict('records')
            
            # 处理特殊数据类型
            for item in data:
                for key, value in item.items():
                    # 处理pandas的特殊类型
                    if pd.isna(value):
                        item[key] = None
                    elif hasattr(value, 'item'):  # numpy类型
                        try:
                            item[key] = value.item()
                        except:
                            item[key] = str(value)
                    elif isinstance(value, (list, dict)):
                        # 保持复杂类型
                        item[key] = value
            
            return data, total_rows
            
        except ImportError:
            raise ValueError("PyArrow未安装，无法读取Arrow文件")
        except Exception as e:
            raise ValueError(f"读取Arrow文件失败: {e}")
    
    def _read_parquet_file(self, file_path: Path, max_rows: int) -> Tuple[List[Dict], int]:
        """读取Parquet文件"""
        try:
            from .dependencies import pq
            if pq is None:
                raise ImportError("pyarrow.parquet is required for Parquet files")
            
            # 读取Parquet文件
            table = pq.read_table(str(file_path))
            
            # 转换为pandas DataFrame
            df = table.to_pandas()
            
            # 获取总行数
            total_rows = len(df)
            
            # 限制行数
            if max_rows > 0:
                df = df.head(max_rows)
            
            # 转换为字典列表
            data = df.to_dict('records')
            
            # 处理特殊数据类型
            for item in data:
                for key, value in item.items():
                    # 处理pandas的特殊类型
                    if pd.isna(value):
                        item[key] = None
                    elif hasattr(value, 'item'):  # numpy类型
                        item[key] = value.item()
            
            return data, total_rows
            
        except ImportError:
            raise ValueError("PyArrow未安装，无法读取Parquet文件")
        except Exception as e:
            raise ValueError(f"读取Parquet文件失败: {e}")
    
    def _scan_directory(self, dir_path: Path) -> List[Path]:
        """扫描目录中的数据文件"""
        data_files = []
        
        # 支持的文件扩展名
        priority_extensions = {'.arrow', '.parquet'}
        regular_extensions = {'.jsonl', '.json', '.csv', '.tsv', '.txt'}
        
        # 需要排除的文件名模式
        exclude_patterns = {
            'meta.json', 'dataset_info.json', 'dataset_infos.json',
            'state.json', 'dataset_dict.json', 'merge_meta.json',
            'extract_meta.json', 'config.json', 'tokenizer.json',
            'config.json', 'added_tokens.json', 'special_tokens_map.json',
            'readme.md', 'license', 'license.txt', 'checkpoint.json',
            'checkpoint.json.tmp'
        }
        
        # 递归扫描所有文件
        try:
            # 使用rglob扫描所有文件
            all_files = list(dir_path.rglob('*'))
            
            # 过滤和分类
            priority_files = []
            regular_files = []
            
            for file_path in all_files:
                if not file_path.is_file():
                    continue
                    
                # 排除隐藏文件和目录
                if any(part.startswith('.') for part in file_path.relative_to(dir_path).parts):
                    continue
                    
                # 排除特定模式
                if file_path.name.lower() in exclude_patterns:
                    continue
                
                # 排除日志文件
                if file_path.suffix.lower() == '.log':
                    continue
                
                suffix = file_path.suffix.lower()
                if suffix in priority_extensions:
                    priority_files.append(file_path)
                elif suffix in regular_extensions:
                    regular_files.append(file_path)
            
            # 优先返回优先级高的文件
            if priority_files:
                return sorted(priority_files)
            
            # 其次返回普通文件
            return sorted(regular_files)
            
        except Exception as e:
            self.logger.error(f"扫描目录失败 {dir_path}: {e}")
            return []
    
    def _apply_text_truncation(self, data: List[Dict]) -> Tuple[List[Dict], List[str]]:
        """应用文本截断"""
        if not data or not self.config.enable_truncation:
            return data, []
        
        truncated_fields = set()
        processed_data = []
        
        for item in data:
            processed_item = {}
            for key, value in item.items():
                if isinstance(value, str) and len(value) > self.config.max_text_length:
                    processed_item[key] = value[:self.config.max_text_length] + "..."
                    processed_item[f"{key}_truncated"] = True
                    processed_item[f"{key}_full_length"] = len(value)
                    truncated_fields.add(key)
                else:
                    processed_item[key] = value
            
            processed_data.append(processed_item)
        
        return processed_data, list(truncated_fields)
    
    def _apply_smart_columns(self, data: List[Dict]) -> Tuple[List[Dict], List[str], Dict[str, Any]]:
        """应用智能列显示逻辑"""
        if not data:
            return data, [], {}
        
        # 分析所有列的数据情况
        column_stats = self._analyze_columns(data)
        
        # 确定要隐藏的列
        hidden_columns = []
        important_columns = []
        
        for column, stats in column_stats.items():
            # 隐藏条件：
            # 1. 空值率超过90%
            # 2. 所有值都相同且不是重要列名（但保留id列）
            # 3. 列名不重要且内容简单
            
            null_rate = stats['null_rate']
            unique_count = stats['unique_count']
            is_important = self._is_important_column(column)
            
            # 特殊处理：完全空的列（如input列全空）
            if null_rate >= 0.95 and not is_important:
                hidden_columns.append(column)
            # 单一值列：除非是id或重要列
            elif unique_count == 1 and not is_important and stats['total_count'] > 5 and column.lower() != 'id':
                hidden_columns.append(column)
            # 无关紧要的列
            elif not is_important and self._is_trivial_column(column, stats):
                hidden_columns.append(column)
            else:
                important_columns.append(column)
        
        # 确保至少保留一些关键列
        if len(important_columns) == 0:
            # 如果没有重要列，保留前几个非空列
            for column, stats in sorted(column_stats.items(), key=lambda x: x[1]['null_rate']):
                if len(important_columns) < 3:
                    important_columns.append(column)
                    if column in hidden_columns:
                        hidden_columns.remove(column)
        
        # 过滤数据
        filtered_data = []
        for item in data:
            filtered_item = {k: v for k, v in item.items() if k not in hidden_columns}
            filtered_data.append(filtered_item)
        
        return filtered_data, hidden_columns, column_stats
    
    def _analyze_columns(self, data: List[Dict]) -> Dict[str, Dict[str, Any]]:
        """分析列的数据特征"""
        if not data:
            return {}
        
        # 获取所有列名
        all_columns = set()
        for item in data:
            all_columns.update(item.keys())
        
        column_stats = {}
        
        for column in all_columns:
            values = []
            null_count = 0
            
            for item in data:
                value = item.get(column)
                if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
                    null_count += 1
                else:
                    values.append(value)
            
            total_count = len(data)
            null_rate = null_count / total_count if total_count > 0 else 1
            unique_values = set(str(v) for v in values)
            unique_count = len(unique_values)
            
            # 计算平均文本长度
            text_lengths = [len(str(v)) for v in values if v is not None]
            avg_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
            max_length = max(text_lengths) if text_lengths else 0
            
            column_stats[column] = {
                'total_count': total_count,
                'null_count': null_count,
                'null_rate': null_rate,
                'unique_count': unique_count,
                'avg_length': avg_length,
                'max_length': max_length,
                'sample_values': list(unique_values)[:5]  # 取前5个样本值
            }
        
        return column_stats
    
    def _is_important_column(self, column_name: str) -> bool:
        """判断是否为重要列"""
        important_keywords = {
            # 对话和问答类
            'instruction', 'input', 'output', 'response', 'answer', 'question', 'query', 'prompt',
            'text', 'content', 'message', 'conversation', 'dialogue', 'chat',
            
            # 标签和分类
            'label', 'target', 'category', 'class', 'type', 'tag', 'classification',
            
            # 标识符
            'id', 'name', 'title', 'subject', 'topic', 'key',
            
            # 中文字段
            '指令', '输入', '输出', '回答', '问题', '内容', '文本', '标签', '类别', '名称', '标题',
            '问', '答', '对话', '聊天', '回复', '响应'
        }
        
        column_lower = column_name.lower()
        return any(keyword in column_lower for keyword in important_keywords)
    
    def _is_trivial_column(self, column_name: str, stats: Dict[str, Any]) -> bool:
        """判断是否为无关紧要的列"""
        trivial_keywords = {
            'meta', 'metadata', 'version', 'timestamp', 'created', 'updated', 'modified',
            'uuid', 'hash', 'checksum', 'index', 'seq', 'sequence', 'order', 'sort',
            'temp', 'tmp', 'debug', 'test', 'example', 'sample', 'dummy',
            'source', 'origin', 'raw', 'original', 'backup', 'old', 'prev', 'previous'
        }
        
        column_lower = column_name.lower()
        
        # 列名包含无关紧要的关键词
        if any(keyword in column_lower for keyword in trivial_keywords):
            return True
        
        # 内容很短且重复率高
        if stats['avg_length'] < 5 and stats['unique_count'] < 3:
            return True
        
        # 单字符列且不重要
        if len(column_name) <= 2 and not self._is_important_column(column_name):
            return True
        
        return False
    
    def _generate_metadata(self, file_info: FileInfo, data: List[Dict]) -> Dict[str, Any]:
        """生成元数据"""
        metadata = {
            'file_info': {
                'name': file_info.name,
                'size_bytes': file_info.size,
                'size_human': self._format_size(file_info.size),
                'format': file_info.format.value,
                'total_rows': file_info.row_count
            }
        }
        
        if data and self.config.show_stats:
            # 分析数据结构
            if isinstance(data[0], dict):
                metadata['schema'] = {
                    'columns': list(data[0].keys()),
                    'column_count': len(data[0].keys())
                }
                
                # 字段类型分析
                column_types = {}
                for key in data[0].keys():
                    values = [item.get(key) for item in data[:10]]  # 取前10行分析
                    types = set(type(v).__name__ for v in values if v is not None)
                    column_types[key] = list(types)
                
                metadata['column_types'] = column_types
        
        return metadata
    
    def _format_size(self, size_bytes: int) -> str:
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}TB"
    
    def preview_to_dataframe(self, path: Union[str, Path], 
                           max_rows: Optional[int] = None) -> pd.DataFrame:
        """预览数据集并返回DataFrame格式"""
        result = self.preview_dataset(path, max_rows)
        
        if not result.success or not result.data:
            return pd.DataFrame()
        
        return pd.DataFrame(result.data)
    
    def get_dataset_summary(self, path: Union[str, Path]) -> Dict[str, Any]:
        """获取数据集摘要信息（不加载具体数据）"""
        try:
            path = Path(path)
            if not path.exists():
                return {'error': f'路径不存在: {path}'}
            
            if path.is_file():
                file_format = self._detect_format(path)
                file_size = path.stat().st_size
                
                # 快速统计行数（不加载数据）
                if file_format == DatasetFormat.JSONL:
                    with open(path, 'r') as f:
                        row_count = sum(1 for _ in f)
                elif file_format in [DatasetFormat.CSV, DatasetFormat.TSV]:
                    with open(path, 'r') as f:
                        row_count = sum(1 for _ in f) - 1  # 减去标题行
                else:
                    row_count = None
                
                return {
                    'type': 'file',
                    'name': path.name,
                    'format': file_format.value,
                    'size_bytes': file_size,
                    'size_human': self._format_size(file_size),
                    'estimated_rows': row_count
                }
            
            elif path.is_dir():
                data_files = self._scan_directory(path)
                total_size = sum(f.stat().st_size for f in data_files)
                formats = list(set(self._detect_format(f).value for f in data_files))
                
                # 改善多文件数据集的名称显示
                dataset_name = path.name
                if data_files:
                    # 检查是否有子目录结构的数据集
                    subdirs_with_data = set()
                    for f in data_files:
                        rel_path = f.relative_to(path)
                        if len(rel_path.parts) > 1:  # 文件在子目录中
                            subdirs_with_data.add(rel_path.parts[0])
                    
                    if subdirs_with_data:
                        # 如果数据文件在子目录中，显示更描述性的名称
                        subdir_names = sorted(subdirs_with_data)
                        if len(subdir_names) == 1:
                            dataset_name = f"{path.name} ({subdir_names[0]})"
                        else:
                            dataset_name = f"{path.name} ({'/'.join(subdir_names[:2])}{'...' if len(subdir_names) > 2 else ''})"
                
                return {
                    'type': 'directory',
                    'name': dataset_name,
                    'file_count': len(data_files),
                    'formats': formats,
                    'total_size_bytes': total_size,
                    'total_size_human': self._format_size(total_size)
                }
            
        except Exception as e:
            return {'error': str(e)}


# CLI接口
def main():
    """命令行接口"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据集预览工具')
    parser.add_argument('path', help='数据集路径')
    parser.add_argument('--max-rows', type=int, default=10, help='最大预览行数')
    parser.add_argument('--max-text-length', type=int, default=200, help='文本截断长度')
    parser.add_argument('--no-truncation', action='store_true', help='禁用文本截断')
    parser.add_argument('--summary-only', action='store_true', help='仅显示摘要信息')
    parser.add_argument('--output-format', choices=['table', 'json'], default='table', help='输出格式')
    
    args = parser.parse_args()
    
    # 创建配置
    config = PreviewConfig(
        max_rows=args.max_rows,
        max_text_length=args.max_text_length,
        enable_truncation=not args.no_truncation
    )
    
    # 创建预览器
    previewer = DatasetPreviewer(config)
    
    if args.summary_only:
        # 仅显示摘要
        summary = previewer.get_dataset_summary(args.path)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        # 完整预览
        result = previewer.preview_dataset(args.path)
        
        if not result.success:
            print(f"预览失败: {result.error_message}")
            return
        
        if args.output_format == 'json':
            # JSON输出
            output = {
                'success': result.success,
                'data': result.data,
                'metadata': result.metadata,
                'total_rows': result.total_rows,
                'total_files': result.total_files
            }
            print(json.dumps(output, indent=2, ensure_ascii=False))
        else:
            # 表格输出
            print(f"\n📊 数据集预览: {args.path}")
            print(f"格式: {result.format.value}")
            print(f"总行数: {result.total_rows}")
            print(f"文件数: {result.total_files}")
            
            if result.truncated_fields:
                print(f"截断字段: {', '.join(result.truncated_fields)}")
            
            print("\n" + "="*80)
            
            if result.data:
                df = pd.DataFrame(result.data)
                print(df.to_string(max_rows=args.max_rows))
            else:
                print("没有数据可显示")


if __name__ == "__main__":
    main()
