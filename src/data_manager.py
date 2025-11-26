#!/usr/bin/env python3
"""
数据管理模块

本模块负责统一管理所有数据集（原始/加工/蒸馏），提供数据预览、关键词搜索、备份/恢复、删除等功能。
功能特点：
- 数据集统一管理和分类
- 数据预览和关键词搜索
- 数据备份和恢复
- 数据删除和清理
- 数据统计和监控

设计原则：
- 统一的数据管理接口
- 支持多种数据格式
- 安全的数据操作
- 可追溯的数据历史

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import os
import json
import shutil
import fnmatch
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from .dependencies import pd

# 导入统一异常类
try:
    from .exceptions import DataManagerError, DataNotFoundError, DataAccessError
except ImportError:
    # 如果导入失败，使用本地定义（向后兼容）
    class DataManagerError(Exception):
        """数据管理相关异常类"""
        pass
    
    class DataNotFoundError(DataManagerError):
        """数据不存在异常"""
        pass
    
    class DataAccessError(DataManagerError):
        """数据访问异常"""
        pass

# 基础支撑层导入
try:
    # 作为模块导入时使用相对导入
    from .config_manager import config_manager
    from .log_manager import log_manager
    from .utils import FileOperations, DataProcessing
except ImportError:
    # 直接运行时使用绝对导入
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from config_manager import config_manager
    from log_manager import log_manager
    from utils import FileOperations, DataProcessing


class DataType:
    """数据类型常量"""
    RAW = "raw"              # 原始数据
    PROCESSED = "processed"  # 加工数据
    DISTILLED = "distilled"  # 蒸馏数据
    BACKUP = "backup"        # 备份数据


class DataManager:
    """
    数据管理器
    
    负责管理所有数据集的分类、预览、搜索、备份等功能。
    """
    
    def __init__(self):
        """初始化数据管理器"""
        self.logger = log_manager.get_logger('data_manager')
        
        # 获取配置
        self.root_dir = Path(config_manager.get_config('base.root_dir', './data'))
        self.preview_rows = config_manager.get_config('data_manager.preview_rows', 100)
        self.search_limit = config_manager.get_config('data_manager.search_limit', 1000)
        
        # 数据目录映射
        self.data_dirs = {
            DataType.RAW: self.root_dir / 'raw',
            DataType.PROCESSED: self.root_dir / 'processed', 
            DataType.DISTILLED: self.root_dir / 'distilled',
            DataType.BACKUP: self.root_dir / 'backup'
        }
        
        # 确保目录存在
        self._ensure_directories()
        
        # 支持的文件格式
        self.supported_formats = ['.jsonl', '.json', '.csv', '.xlsx', '.xml', '.md']
        
        self.logger.info('数据管理器初始化完成')
    
    def _ensure_directories(self) -> None:
        """确保所有数据目录存在"""
        try:
            for data_type, dir_path in self.data_dirs.items():
                dir_path.mkdir(parents=True, exist_ok=True)
            self.logger.debug('数据目录检查完成')
        except Exception as e:
            self.logger.error(f'创建数据目录失败: {e}')
    
    def list_datasets(self, data_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        快速列出数据集（高度优化版本）
        """
        try:
            datasets = []
            
            # 确定要扫描的目录
            if data_type and data_type in self.data_dirs:
                scan_dirs = {data_type: self.data_dirs[data_type]}
            else:
                scan_dirs = {k: v for k, v in self.data_dirs.items() if k != DataType.BACKUP}
            
            # 针对不同类型使用不同的扫描策略
            for dtype, dir_path in scan_dirs.items():
                if not dir_path.exists():
                    continue
                
                # 限制最大扫描文件数，防止内存溢出
                max_files = 2000
                
                try:
                    if dtype == 'raw':
                        # 原始数据通常在深层目录，使用递归但限制深度
                        self._scan_raw_directory(dir_path, datasets, dtype, max_files)
                    else:
                        # 处理数据通常在浅层目录，使用简单扫描
                        self._scan_processed_directory(dir_path, datasets, dtype)
                        
                except Exception as e:
                    self.logger.warning(f'扫描目录失败: {dir_path}, 错误: {e}')
                    continue
            
            # 按修改时间排序（最新的在前）
            datasets.sort(key=lambda x: x['modified_time'], reverse=True)
            
            self.logger.info(f'列出数据集完成: 类型={data_type or "all"}, 数量={len(datasets)}')
            return datasets
            
        except Exception as e:
            self.logger.error(f'列出数据集失败: {e}')
            return []
    
    def _scan_raw_directory(self, dir_path: Path, datasets: List, dtype: str, max_files: int = 2000):
        """扫描原始数据目录（使用os.walk优化性能）"""
        import os
        
        scanned_count = 0
        
        for root, dirs, files in os.walk(str(dir_path)):
            # 过滤掉以.开头的目录（如.git, ._____temp等）
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            
            for file in files:
                # 检查是否超过最大文件限制
                if len(datasets) >= max_files:
                    return

                # 检查文件扩展名
                if not file.lower().endswith(('.json', '.jsonl', '.csv', '.parquet')):
                    continue
                    
                # 跳过元数据和系统文件
                skip_files = [
                    'meta.json', 'dataset_infos.json', 'dataset_dict.json',
                    'dataset_info.json', 'state.json', 'config.json'
                ]
                if file in skip_files:
                    continue
                
                # 跳过缓存目录中的文件
                if 'cache' in root.lower() or 'downloads' in root.lower():
                    continue
                
                try:
                    file_path = Path(root) / file
                    stat_info = file_path.stat()
                    file_size_mb = stat_info.st_size / (1024 * 1024)
                    
                    # 尝试构建更友好的显示名称
                    # 如果文件在子目录中，使用 "子目录/文件名" 格式
                    # 特别针对 MegaScience/MegaScience/dataset/data/xxx.parquet 这种情况
                    try:
                        rel_path = file_path.relative_to(dir_path)
                        # 如果路径深度大于1，尝试提取有意义的部分
                        parts = rel_path.parts
                        if len(parts) > 1:
                            # 查找是否有 dataset 目录
                            if 'dataset' in parts:
                                idx = parts.index('dataset')
                                if idx > 0:
                                    # 使用 dataset 之前的目录名作为数据集名称的一部分
                                    # 例如 MegaScience/MegaScience/dataset -> MegaScience/MegaScience
                                    prefix = "/".join(parts[:idx])
                                    display_name = f"{prefix}/{file}"
                                else:
                                    display_name = str(rel_path).replace('\\', '/')
                            else:
                                # 使用相对路径作为名称
                                display_name = str(rel_path).replace('\\', '/')
                        else:
                            display_name = file
                    except Exception:
                        display_name = file

                    dataset_info = {
                        'name': display_name,
                        'path': str(file_path),
                        'relative_path': str(file_path.relative_to(self.root_dir)),
                        'type': dtype,
                        'format': file_path.suffix.lower()[1:],
                        'size': stat_info.st_size,
                        'size_mb': file_size_mb,
                        'size_human': self._format_size(stat_info.st_size),
                        'created_time': datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
                        'modified_time': datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                        'create_time': datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
                        'row_count': 0,
                        'has_meta': False
                    }
                    
                    datasets.append(dataset_info)
                    
                except Exception:
                    continue
    
    def _scan_processed_directory(self, dir_path: Path, datasets: List, dtype: str):
        """扫描处理数据目录（浅层扫描）"""
        for pattern in ['*.jsonl', '*.json', '*.csv']:
            # 扫描当前目录
            for file_path in dir_path.glob(pattern):
                # 跳过元数据和系统文件
                if file_path.name in ['meta.json', 'checkpoint.json', 'quality_report.json', 'dataset_info.json']:
                    continue
                    
                try:
                    stat_info = file_path.stat()
                    file_size_mb = stat_info.st_size / (1024 * 1024)
                    
                    dataset_info = {
                        'name': file_path.name,
                        'path': str(file_path),
                        'relative_path': str(file_path.relative_to(self.root_dir)),
                        'type': dtype,
                        'format': file_path.suffix.lower()[1:],
                        'size': stat_info.st_size,
                        'size_mb': file_size_mb,
                        'size_human': self._format_size(stat_info.st_size),
                        'created_time': datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
                        'modified_time': datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                        'create_time': datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
                        'row_count': 0,
                        'has_meta': False
                    }
                    
                    datasets.append(dataset_info)
                    
                except Exception:
                    continue
            
            # 扫描一级子目录
            for subdir in dir_path.iterdir():
                if subdir.is_dir():
                    for file_path in subdir.glob(pattern):
                        # 跳过元数据和系统文件
                        if file_path.name in ['meta.json', 'checkpoint.json', 'quality_report.json', 'dataset_info.json']:
                            continue
                            
                        try:
                            stat_info = file_path.stat()
                            file_size_mb = stat_info.st_size / (1024 * 1024)
                            
                            dataset_info = {
                                'name': file_path.name,
                                'path': str(file_path),
                                'relative_path': str(file_path.relative_to(self.root_dir)),
                                'type': dtype,
                                'format': file_path.suffix.lower()[1:],
                                'size': stat_info.st_size,
                                'size_mb': file_size_mb,
                                'size_human': self._format_size(stat_info.st_size),
                                'created_time': datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
                                'modified_time': datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                                'create_time': datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
                                'row_count': 0,
                                'has_meta': False
                            }
                            
                            datasets.append(dataset_info)
                            
                        except Exception:
                            continue

    def _format_size(self, size_bytes: int) -> str:
        """格式化文件大小"""
        if size_bytes == 0:
            return "0B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f}{size_names[i]}"
    
    def _get_row_count(self, file_path: Path) -> int:
        """获取文件行数"""
        return self._get_row_count_fast(file_path)
    
    def _get_row_count_fast(self, file_path: Path) -> int:
        """快速获取文件行数（带超时和大小限制）"""
        try:
            # 检查文件大小，超过100MB的文件跳过行数计算
            file_size = file_path.stat().st_size
            if file_size > 100 * 1024 * 1024:  # 100MB
                return -1  # 表示未计算
            
            if file_path.suffix.lower() == '.jsonl':
                # 对于JSONL文件，快速计算行数
                with open(file_path, 'r', encoding='utf-8') as f:
                    count = 0
                    for _ in f:
                        count += 1
                        # 超过10万行就停止计算，避免耗时过长
                        if count > 100000:
                            return -1
                    return count
            elif file_path.suffix.lower() == '.csv':
                try:
                    # 对于CSV文件，尝试快速读取
                    df = pd.read_csv(file_path, nrows=0)  # 只读取头部获取列信息
                    # 如果文件不大，读取完整文件
                    if file_size < 10 * 1024 * 1024:  # 10MB以下
                        df = pd.read_csv(file_path)
                        return len(df)
                    else:
                        return -1  # 大文件跳过
                except:
                    return 0
            elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                try:
                    if file_size < 5 * 1024 * 1024:  # 5MB以下的Excel文件
                        df = pd.read_excel(file_path)
                        return len(df)
                    else:
                        return -1  # 大文件跳过
                except:
                    return 0
            else:
                # 其他格式暂时返回0
                return 0
        except Exception:
            return 0
    
    def _has_meta_file(self, file_path: Path) -> bool:
        """检查是否有对应的meta.json文件"""
        meta_path = file_path.parent / 'meta.json'
        return meta_path.exists()
    
    def preview_data(self, file_path: str, rows: int = None) -> Dict[str, Any]:
        """
        预览数据（前N行）
        
        Args:
            file_path (str): 文件路径
            rows (int, optional): 预览行数，默认使用配置中的值
            
        Returns:
            dict: 预览结果，包含数据和元信息
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f'文件不存在: {file_path}')
            
            rows = rows or self.preview_rows
            format_type = file_path.suffix.lower()[1:]
            
            preview_data = []
            total_rows = 0
            columns = []
            
            if format_type == 'jsonl':
                with open(file_path, 'r', encoding='utf-8') as f:
                    for i, line in enumerate(f):
                        if i >= rows:
                            break
                        try:
                            data = json.loads(line.strip())
                            preview_data.append(data)
                            
                            # 收集所有键作为列名
                            if isinstance(data, dict):
                                columns.extend(data.keys())
                        except json.JSONDecodeError:
                            continue
                
                # 获取总行数
                total_rows = self._get_row_count(file_path)
                columns = list(set(columns))  # 去重
                
            elif format_type == 'csv':
                df = pd.read_csv(file_path, nrows=rows)
                preview_data = df.to_dict('records')
                columns = df.columns.tolist()
                
                # 获取总行数
                total_rows = self._get_row_count(file_path)
                
            elif format_type in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, nrows=rows)
                preview_data = df.to_dict('records')
                columns = df.columns.tolist()
                total_rows = self._get_row_count(file_path)
                
            elif format_type == 'json':
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, list):
                    preview_data = data[:rows]
                    total_rows = len(data)
                    if data and isinstance(data[0], dict):
                        columns = list(data[0].keys())
                else:
                    preview_data = [data]
                    total_rows = 1
                    columns = list(data.keys()) if isinstance(data, dict) else []
            
            result = {
                'success': True,
                'file_path': str(file_path),
                'format': format_type,
                'total_rows': total_rows,
                'preview_rows': len(preview_data),
                'columns': columns,
                'data': preview_data,
                'preview_limit': rows
            }
            
            self.logger.info(f'预览数据成功: {file_path.name}, 预览行数: {len(preview_data)}')
            return result
            
        except Exception as e:
            self.logger.error(f'预览数据失败: {file_path}, 错误: {e}')
            return {
                'success': False,
                'error': str(e),
                'file_path': str(file_path),
                'data': []
            }
    
    def search_data(self, keyword: str, fields: Optional[List[str]] = None, 
                   data_type: Optional[str] = None) -> Dict[str, Any]:
        """
        关键词搜索数据
        
        Args:
            keyword (str): 搜索关键词
            fields (list, optional): 指定搜索的字段
            data_type (str, optional): 指定数据类型
            
        Returns:
            dict: 搜索结果
        """
        try:
            search_results = []
            total_matches = 0
            
            # 获取要搜索的数据集
            datasets = self.list_datasets(data_type)
            
            for dataset in datasets:
                try:
                    file_path = Path(dataset['path'])
                    format_type = dataset['format']
                    
                    # 根据格式读取数据并搜索
                    matches = self._search_in_file(file_path, keyword, fields, format_type)
                    
                    if matches:
                        search_results.append({
                            'dataset': dataset,
                            'matches': matches[:50],  # 限制每个文件的匹配结果
                            'match_count': len(matches)
                        })
                        total_matches += len(matches)
                    
                    # 限制总搜索结果
                    if total_matches >= self.search_limit:
                        break
                        
                except Exception as e:
                    self.logger.warning(f'搜索文件失败: {file_path}, 错误: {e}')
            
            result = {
                'success': True,
                'keyword': keyword,
                'fields': fields,
                'data_type': data_type,
                'total_matches': total_matches,
                'datasets_count': len(search_results),
                'results': search_results,
                'search_limit': self.search_limit
            }
            
            self.logger.info(f'搜索完成: 关键词="{keyword}", 匹配={total_matches}')
            return result
            
        except Exception as e:
            self.logger.error(f'搜索数据失败: {e}')
            return {
                'success': False,
                'error': str(e),
                'keyword': keyword,
                'results': []
            }
    
    def _search_in_file(self, file_path: Path, keyword: str, 
                       fields: Optional[List[str]], format_type: str) -> List[Dict[str, Any]]:
        """在文件中搜索关键词"""
        matches = []
        keyword_lower = keyword.lower()
        
        try:
            if format_type == 'jsonl':
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        try:
                            data = json.loads(line.strip())
                            if self._match_in_record(data, keyword_lower, fields):
                                matches.append({
                                    'line_number': line_num,
                                    'data': data
                                })
                        except json.JSONDecodeError:
                            continue
                            
            elif format_type == 'csv':
                df = pd.read_csv(file_path)
                for idx, row in df.iterrows():
                    if self._match_in_record(row.to_dict(), keyword_lower, fields):
                        matches.append({
                            'row_number': idx + 1,
                            'data': row.to_dict()
                        })
                        
            elif format_type in ['xlsx', 'xls']:
                df = pd.read_excel(file_path)
                for idx, row in df.iterrows():
                    if self._match_in_record(row.to_dict(), keyword_lower, fields):
                        matches.append({
                            'row_number': idx + 1,
                            'data': row.to_dict()
                        })
                        
        except Exception as e:
            self.logger.warning(f'文件内搜索失败: {file_path}, 错误: {e}')
        
        return matches
    
    def _match_in_record(self, record: Dict[str, Any], keyword: str, 
                        fields: Optional[List[str]]) -> bool:
        """检查记录是否匹配关键词"""
        if not isinstance(record, dict):
            return False
        
        # 确定要搜索的字段
        search_fields = fields if fields else record.keys()
        
        for field in search_fields:
            if field in record:
                value = str(record[field]).lower()
                if keyword in value:
                    return True
        
        return False
    
    def backup_data(self, file_paths: List[str], backup_date: Optional[str] = None) -> str:
        """
        备份指定数据集
        
        Args:
            file_paths (list): 要备份的文件路径列表
            backup_date (str, optional): 备份日期，默认当前日期
            
        Returns:
            str: 备份路径
        """
        try:
            backup_date = backup_date or datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = self.data_dirs[DataType.BACKUP] / backup_date
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            backup_manifest = {
                'backup_date': backup_date,
                'created_time': datetime.now().isoformat(),
                'files': []
            }
            
            for file_path_str in file_paths:
                try:
                    file_path = Path(file_path_str)
                    if not file_path.exists():
                        self.logger.warning(f'备份文件不存在: {file_path}')
                        continue
                    
                    # 确定备份路径，保持相对目录结构
                    if file_path.is_relative_to(self.root_dir):
                        rel_path = file_path.relative_to(self.root_dir)
                    else:
                        rel_path = file_path.name
                    
                    backup_file_path = backup_dir / rel_path
                    backup_file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # 复制文件
                    shutil.copy2(file_path, backup_file_path)
                    
                    # 复制meta文件（如果存在）
                    meta_path = file_path.parent / 'meta.json'
                    if meta_path.exists():
                        backup_meta_path = backup_file_path.parent / 'meta.json'
                        shutil.copy2(meta_path, backup_meta_path)
                    
                    backup_manifest['files'].append({
                        'original_path': str(file_path),
                        'backup_path': str(backup_file_path),
                        'size': file_path.stat().st_size,
                        'backup_time': datetime.now().isoformat()
                    })
                    
                except Exception as e:
                    self.logger.error(f'备份单个文件失败: {file_path_str}, 错误: {e}')
            
            # 保存备份清单
            manifest_path = backup_dir / 'backup_manifest.json'
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(backup_manifest, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f'备份完成: {len(backup_manifest["files"])}个文件, 路径: {backup_dir}')
            return str(backup_dir)
            
        except Exception as e:
            self.logger.error(f'备份数据失败: {e}')
            raise
    
    def restore_data(self, backup_path: str, target_dir: Optional[str] = None) -> bool:
        """
        从备份恢复数据
        
        Args:
            backup_path (str): 备份路径
            target_dir (str, optional): 目标目录，默认为原始位置
            
        Returns:
            bool: 恢复成功返回True
        """
        try:
            backup_path = Path(backup_path)
            if not backup_path.exists():
                raise FileNotFoundError(f'备份路径不存在: {backup_path}')
            
            # 读取备份清单
            manifest_path = backup_path / 'backup_manifest.json'
            if not manifest_path.exists():
                raise FileNotFoundError(f'备份清单不存在: {manifest_path}')
            
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            
            restored_count = 0
            target_base = Path(target_dir) if target_dir else self.root_dir
            
            for file_info in manifest['files']:
                try:
                    backup_file_path = Path(file_info['backup_path'])
                    if not backup_file_path.exists():
                        self.logger.warning(f'备份文件不存在: {backup_file_path}')
                        continue
                    
                    # 确定恢复路径
                    if target_dir:
                        # 恢复到指定目录
                        relative_path = backup_file_path.relative_to(backup_path)
                        restore_path = target_base / relative_path
                    else:
                        # 恢复到原始路径
                        restore_path = Path(file_info['original_path'])
                    
                    # 创建目录
                    restore_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # 复制文件
                    shutil.copy2(backup_file_path, restore_path)
                    restored_count += 1
                    
                except Exception as e:
                    self.logger.error(f'恢复单个文件失败: {file_info}, 错误: {e}')
            
            self.logger.info(f'恢复完成: {restored_count}个文件')
            return restored_count > 0
            
        except Exception as e:
            self.logger.error(f'恢复数据失败: {e}')
            return False
    
    def delete_data(self, file_path: str) -> bool:
        """
        删除数据集（含元数据）
        支持删除单个文件或整个数据集目录
        
        Args:
            file_path (str): 文件路径或目录路径
            
        Returns:
            bool: 删除成功返回True
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                self.logger.warning(f'要删除的路径不存在: {file_path}')
                return False
            
            if file_path.is_file():
                # 删除单个文件
                self.logger.info(f'删除文件: {file_path}')
                file_path.unlink()
                
                # 删除meta文件（如果存在）
                meta_path = file_path.parent / 'meta.json'
                if meta_path.exists():
                    meta_path.unlink()
                    self.logger.info(f'删除元数据文件: {meta_path}')
                
                # 如果目录为空，删除目录
                try:
                    if not any(file_path.parent.iterdir()):
                        file_path.parent.rmdir()
                        self.logger.info(f'删除空目录: {file_path.parent}')
                except OSError:
                    pass  # 目录不为空或其他原因
                    
            elif file_path.is_dir():
                # 删除整个数据集目录
                self.logger.info(f'删除数据集目录: {file_path}')
                import shutil
                shutil.rmtree(file_path)
                
            else:
                self.logger.warning(f'未知的路径类型: {file_path}')
                return False
            
            self.logger.info(f'删除成功: {file_path}')
            return True
            
        except Exception as e:
            self.logger.error(f'删除失败: {file_path}, 错误: {e}')
            return False
    
    def get_storage_statistics(self) -> Dict[str, Any]:
        """
        获取存储统计信息
        
        Returns:
            dict: 存储统计信息
        """
        try:
            stats = {
                'total_size': 0,
                'total_files': 0,
                'by_type': {},
                'by_format': {},
                'largest_files': [],
                'recent_files': []
            }
            
            # 获取所有数据集
            all_datasets = self.list_datasets()
            
            # 统计信息
            format_stats = {}
            type_stats = {}
            
            for dataset in all_datasets:
                # 总体统计
                stats['total_size'] += dataset['size']
                stats['total_files'] += 1
                
                # 按类型统计
                data_type = dataset['type']
                if data_type not in type_stats:
                    type_stats[data_type] = {'size': 0, 'count': 0}
                type_stats[data_type]['size'] += dataset['size']
                type_stats[data_type]['count'] += 1
                
                # 按格式统计
                file_format = dataset['format']
                if file_format not in format_stats:
                    format_stats[file_format] = {'size': 0, 'count': 0}
                format_stats[file_format]['size'] += dataset['size']
                format_stats[file_format]['count'] += 1
            
            # 格式化统计信息
            for data_type, type_stat in type_stats.items():
                stats['by_type'][data_type] = {
                    'size': type_stat['size'],
                    'size_human': self._format_size(type_stat['size']),
                    'count': type_stat['count'],
                    'percentage': (type_stat['size'] / stats['total_size'] * 100) if stats['total_size'] > 0 else 0
                }
            
            for file_format, format_stat in format_stats.items():
                stats['by_format'][file_format] = {
                    'size': format_stat['size'],
                    'size_human': self._format_size(format_stat['size']),
                    'count': format_stat['count'],
                    'percentage': (format_stat['size'] / stats['total_size'] * 100) if stats['total_size'] > 0 else 0
                }
            
            # 最大文件（前10个）
            stats['largest_files'] = sorted(all_datasets, key=lambda x: x['size'], reverse=True)[:10]
            
            # 最近文件（前10个）
            stats['recent_files'] = sorted(all_datasets, key=lambda x: x['modified_time'], reverse=True)[:10]
            
            # 格式化总大小
            stats['total_size_human'] = self._format_size(stats['total_size'])
            
            return stats
            
        except Exception as e:
            self.logger.error(f'获取存储统计失败: {e}')
            return {}
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """
        列出所有备份
        
        Returns:
            list: 备份列表
        """
        try:
            backups = []
            backup_dir = self.data_dirs[DataType.BACKUP]
            
            if not backup_dir.exists():
                return backups
            
            for backup_subdir in backup_dir.iterdir():
                if backup_subdir.is_dir():
                    manifest_path = backup_subdir / 'backup_manifest.json'
                    if manifest_path.exists():
                        try:
                            with open(manifest_path, 'r', encoding='utf-8') as f:
                                manifest = json.load(f)
                            
                            # 计算备份大小
                            total_size = sum(file_info['size'] for file_info in manifest.get('files', []))
                            
                            backup_info = {
                                'backup_id': backup_subdir.name,
                                'path': str(backup_subdir),
                                'created_time': manifest.get('created_time', ''),
                                'file_count': len(manifest.get('files', [])),
                                'total_size': total_size,
                                'total_size_human': self._format_size(total_size),
                                'manifest': manifest
                            }
                            
                            backups.append(backup_info)
                            
                        except Exception as e:
                            self.logger.warning(f'读取备份清单失败: {manifest_path}, 错误: {e}')
            
            # 按创建时间排序
            backups.sort(key=lambda x: x['created_time'], reverse=True)
            
            return backups
            
        except Exception as e:
            self.logger.error(f'列出备份失败: {e}')
            return []


# 全局数据管理器实例 - 延迟初始化
_data_manager_instance = None

def get_data_manager():
    """获取数据管理器实例（单例模式）"""
    global _data_manager_instance
    if _data_manager_instance is None:
        _data_manager_instance = DataManager()
    return _data_manager_instance

# 为了兼容性，保留data_manager变量
class DataManagerProxy:
    """数据管理器代理，用于延迟初始化"""
    def __getattr__(self, name):
        return getattr(get_data_manager(), name)

data_manager = DataManagerProxy()


if __name__ == "__main__":
    """
    命令行入口，用于数据管理操作
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='数据管理工具')
    subparsers = parser.add_subparsers(dest='action', help='可用操作')
    
    # list命令
    list_parser = subparsers.add_parser('list', help='列出数据集')
    list_parser.add_argument('--type', choices=['raw', 'processed', 'distilled'], help='数据类型')
    
    # preview命令
    preview_parser = subparsers.add_parser('preview', help='预览数据')
    preview_parser.add_argument('--file', required=True, help='文件路径')
    preview_parser.add_argument('--rows', type=int, default=10, help='预览行数')
    
    # search命令
    search_parser = subparsers.add_parser('search', help='搜索数据')
    search_parser.add_argument('--keyword', required=True, help='搜索关键词')
    search_parser.add_argument('--fields', nargs='+', help='搜索字段')
    search_parser.add_argument('--type', choices=['raw', 'processed', 'distilled'], help='数据类型')
    
    # backup命令
    backup_parser = subparsers.add_parser('backup', help='备份数据')
    backup_parser.add_argument('--files', nargs='+', required=True, help='要备份的文件路径')
    backup_parser.add_argument('--date', help='备份日期标识')
    
    # restore命令
    restore_parser = subparsers.add_parser('restore', help='恢复数据')
    restore_parser.add_argument('--backup', required=True, help='备份路径')
    restore_parser.add_argument('--target', help='目标目录')
    
    # delete命令
    delete_parser = subparsers.add_parser('delete', help='删除数据')
    delete_parser.add_argument('--file', required=True, help='文件路径')
    
    # stats命令
    stats_parser = subparsers.add_parser('stats', help='存储统计')
    
    # list-backups命令
    backups_parser = subparsers.add_parser('list-backups', help='列出备份')
    
    args = parser.parse_args()
    
    if args.action == 'list':
        datasets = data_manager.list_datasets(args.type)
        print(f"数据集列表 (类型: {args.type or 'all'}):")
        for dataset in datasets:
            print(f"  📄 {dataset['name']}")
            print(f"     路径: {dataset['relative_path']}")
            print(f"     类型: {dataset['type']} | 格式: {dataset['format']}")
            print(f"     大小: {dataset['size_human']} | 行数: {dataset['row_count']}")
            print(f"     修改: {dataset['modified_time']}")
            print()
    
    elif args.action == 'preview':
        result = data_manager.preview_data(args.file, args.rows)
        if result['success']:
            print(f"文件预览: {result['file_path']}")
            print(f"格式: {result['format']} | 总行数: {result['total_rows']}")
            print(f"列: {', '.join(result['columns'])}")
            print(f"\n前 {result['preview_rows']} 行数据:")
            for i, row in enumerate(result['data'], 1):
                print(f"  {i}: {row}")
        else:
            print(f"预览失败: {result['error']}")
    
    elif args.action == 'search':
        result = data_manager.search_data(args.keyword, args.fields, args.type)
        if result['success']:
            print(f"搜索结果: '{result['keyword']}'")
            print(f"总匹配: {result['total_matches']} | 数据集: {result['datasets_count']}")
            for dataset_result in result['results'][:5]:  # 显示前5个数据集的结果
                dataset = dataset_result['dataset']
                print(f"\n📄 {dataset['name']} ({dataset_result['match_count']} 匹配)")
                for match in dataset_result['matches'][:3]:  # 每个数据集显示前3个匹配
                    print(f"   {match}")
        else:
            print(f"搜索失败: {result['error']}")
    
    elif args.action == 'backup':
        try:
            backup_path = data_manager.backup_data(args.files, args.date)
            print(f"✓ 备份成功: {backup_path}")
        except Exception as e:
            print(f"✗ 备份失败: {e}")
    
    elif args.action == 'restore':
        success = data_manager.restore_data(args.backup, args.target)
        if success:
            print(f"✓ 恢复成功")
        else:
            print(f"✗ 恢复失败")
    
    elif args.action == 'delete':
        success = data_manager.delete_data(args.file)
        if success:
            print(f"✓ 删除成功: {args.file}")
        else:
            print(f"✗ 删除失败: {args.file}")
    
    elif args.action == 'stats':
        stats = data_manager.get_storage_statistics()
        print("存储统计信息:")
        print(f"  总大小: {stats['total_size_human']}")
        print(f"  总文件: {stats['total_files']}")
        
        print("\n按类型统计:")
        for data_type, type_stats in stats['by_type'].items():
            print(f"  {data_type}: {type_stats['size_human']} ({type_stats['count']} 文件, {type_stats['percentage']:.1f}%)")
        
        print("\n按格式统计:")
        for file_format, format_stats in stats['by_format'].items():
            print(f"  {file_format}: {format_stats['size_human']} ({format_stats['count']} 文件)")
        
        print(f"\n最大文件 (前5个):")
        for dataset in stats['largest_files'][:5]:
            print(f"  {dataset['name']}: {dataset['size_human']}")
    
    elif args.action == 'list-backups':
        backups = data_manager.list_backups()
        print("备份列表:")
        for backup in backups:
            print(f"  📦 {backup['backup_id']}")
            print(f"     创建时间: {backup['created_time']}")
            print(f"     文件数量: {backup['file_count']}")
            print(f"     总大小: {backup['total_size_human']}")
            print()
    
    else:
        parser.print_help()
