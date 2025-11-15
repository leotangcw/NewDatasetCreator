#!/usr/bin/env python3
"""
数据清洗模块

本模块负责数据清洗功能，包括去除空值、字段去重、敏感词过滤、内容脱敏等操作。
功能特点：
- 多种清洗操作支持
- 大文件分片处理
- 断点续传支持
- 详细清洗报告
- 灵活的清洗配置

设计原则：
- 可配置的清洗规则
- 安全的数据处理
- 高效的批量操作
- 完整的处理记录

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import os
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, Set
from difflib import SequenceMatcher
import pandas as pd

# 基础支撑层导入
try:
    # 作为模块导入时使用相对导入
    from .config_manager import config_manager
    from .log_manager import log_manager
    from .state_manager import state_manager
    from .utils import FileOperations, DataProcessing
except ImportError:
    # 直接运行时使用绝对导入
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from config_manager import config_manager
    from log_manager import log_manager
    from state_manager import state_manager
    from utils import FileOperations, DataProcessing


class CleaningOperation:
    """清洗操作常量"""
    REMOVE_EMPTY = "remove_empty"        # 去除空值
    DEDUPLICATE = "deduplicate"          # 去重
    FILTER_SENSITIVE = "filter_sensitive" # 敏感词过滤
    PII_DESENSITIZE = "pii_desensitize"  # 个人信息脱敏
    NORMALIZE_TEXT = "normalize_text"    # 文本标准化
    # desensitize (旧版字段脱敏) 已废弃
    """清洗操作常量 (v3)"""


class DataCleaner:
    """
    数据清洗器
    
    负责执行各种数据清洗操作，生成清洗报告。
    """
    
    def __init__(self):
        """初始化数据清洗器"""
        self.logger = log_manager.get_logger('data_cleaner')
        
        # 获取配置
        self.root_dir = Path(config_manager.get_config('base.root_dir', './data'))
        self.output_dir = self.root_dir / 'processed'
        self.chunk_size = config_manager.get_config('process.chunk_size', 1000)
        self.dedup_threshold = config_manager.get_config('process.dedup_threshold', 0.95)
        
        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 支持的操作（已移除旧版 desensitize）
        self.supported_operations = [
            CleaningOperation.REMOVE_EMPTY,
            CleaningOperation.DEDUPLICATE,
            CleaningOperation.FILTER_SENSITIVE,
            CleaningOperation.PII_DESENSITIZE,
            CleaningOperation.NORMALIZE_TEXT,
        ]
        
        # 默认敏感词列表
        self.default_sensitive_words = [
            '密码', 'password', '身份证', 'idcard', '手机号', 'phone',
            '邮箱', 'email', '地址', 'address', '银行卡', 'bankcard'
        ]
        
        self.logger.info('数据清洗器初始化完成')
    
    def start_clean(self, params: Dict[str, Any]) -> str:
        """
        启动数据清洗任务
        
        Args:
            params (dict): 清洗参数
            
        Returns:
            str: 任务ID
        """
        try:
            # 生成任务ID
            task_id = f"clean_{int(time.time())}"

            # 参数预处理: operations 旧值兼容
            if 'operations' in params and isinstance(params['operations'], list):
                filtered_ops = []
                for op in params['operations']:
                    if op == 'desensitize':
                        self.logger.warning('收到已废弃操作 desensitize，已忽略。')
                        continue
                    filtered_ops.append(op)
                params['operations'] = filtered_ops

            # 验证参数
            self._validate_params(params)

            # 创建任务目录
            task_dir = self.output_dir / task_id
            task_dir.mkdir(parents=True, exist_ok=True)

            # 记录任务状态
            state_manager.add_task(
                task_type='clean',  # 与 TaskType.CLEAN 对应
                task_subtype='standard',
                params=params,
                task_id=task_id
            )

            # 更新任务状态为运行中
            state_manager.update_state(task_id, 'status', 'running')
            state_manager.update_state(task_id, 'start_time', datetime.now().isoformat())

            # 执行清洗
            self._execute_cleaning(task_id, params)

            # 更新任务状态为完成
            state_manager.update_state(task_id, 'status', 'completed')
            state_manager.update_state(task_id, 'end_time', datetime.now().isoformat())

            self.logger.info(f'数据清洗任务完成: {task_id}')
            return task_id

        except Exception as e:
            self.logger.error(f'数据清洗任务失败: {e}')
            if 'task_id' in locals():
                state_manager.update_state(task_id, 'status', 'failed')
                state_manager.update_state(task_id, 'error_msg', str(e))
            raise
    def _validate_params(self, params: Dict[str, Any]) -> None:
        """验证清洗参数（宽松策略 + 兼容旧操作名称）"""
        for field in ['source_path', 'operations']:
            if field not in params:
                raise ValueError(f'缺少必填参数: {field}')
        source_path = Path(params['source_path'])
        if not source_path.exists():
            raise FileNotFoundError(f'源文件不存在: {source_path}')
        if not isinstance(params['operations'], list):
            raise ValueError('operations必须是列表')
        # 过滤已废弃 desensitize
        params['operations'] = [op for op in params['operations'] if op != 'desensitize']
        for op in params['operations']:
            if op not in self.supported_operations:
                raise ValueError(f'不支持的清洗操作: {op}')
        if CleaningOperation.DEDUPLICATE in params['operations'] and 'dedup_field' not in params:
            self.logger.warning('deduplicate 缺少 dedup_field，将跳过该操作。')
        if CleaningOperation.REMOVE_EMPTY in params['operations'] and 'remove_empty_fields' not in params:
            self.logger.info('remove_empty 未指定字段，将对全部字段按 empty_mode 判定。')
        if 'dedup_threshold' in params:
            try:
                t = float(params['dedup_threshold'])
                if 0 < t <= 1:
                    self.dedup_threshold = t
                else:
                    self.logger.warning('dedup_threshold 不在 (0,1] 范围，使用默认。')
            except Exception:
                self.logger.warning('dedup_threshold 非法，使用默认。')
    
    def _execute_cleaning(self, task_id: str, params: Dict[str, Any]) -> None:
        """执行数据清洗"""
        source_path = Path(params['source_path'])
        operations = params['operations']
        
        # 检测文件格式
        file_format = source_path.suffix.lower()[1:]
        target_format = params.get('target_format', file_format)
        
        # 创建输出文件路径
        task_dir = self.output_dir / task_id
        output_file = task_dir / f"cleaned.{target_format}"
        
        # 初始化清洗统计
        cleaning_stats = {
            'total_rows': 0,
            'processed_rows': 0,
            'removed_empty': 0,
            'deduplicated': 0,
            'filtered_sensitive': 0,
            'desensitized': 0,
            'normalized': 0,
            'final_rows': 0,
            'sensitive_detail': {
                'field_hits': {},   # 字段 -> 命中次数
                'word_hits': {},    # 词/模式 -> 命中次数
            }
        }
        # removed_duplicates 已移除
        
        # 读取并处理数据
        if file_format == 'jsonl':
            self._clean_jsonl(source_path, output_file, operations, params, cleaning_stats, task_id)
        elif file_format == 'csv':
            self._clean_csv(source_path, output_file, operations, params, cleaning_stats, task_id)
        elif file_format in ['xlsx', 'xls']:
            self._clean_excel(source_path, output_file, operations, params, cleaning_stats, task_id)
        elif file_format == 'json':
            self._clean_json(source_path, output_file, operations, params, cleaning_stats, task_id)
        else:
            raise ValueError(f'不支持的文件格式: {file_format}')
        
        # 生成清洗报告
        self._generate_cleaning_report(task_id, params, cleaning_stats)
        
        # 生成元数据
        self._generate_metadata(task_id, params, cleaning_stats, str(output_file))
    
    def _clean_jsonl(self, source_path: Path, output_file: Path, operations: List[str],
                    params: Dict[str, Any], stats: Dict[str, int], task_id: str) -> None:
        """清洗JSONL文件"""
        seen_records = None  # remove_duplicates 已废弃
        dedup_field = params.get('dedup_field', '')
        dedup_cache = set() if CleaningOperation.DEDUPLICATE in operations else None
        
        with open(source_path, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile, 1):
                try:
                    # 更新进度
                    if line_num % self.chunk_size == 0:
                        progress = min(line_num / stats.get('estimated_total', line_num) * 100, 100)
                        state_manager.update_state(task_id, 'progress', progress)
                        state_manager.update_state(task_id, 'processed_rows', line_num)
                    
                    data = json.loads(line.strip())
                    stats['total_rows'] += 1
                    
                    # 执行清洗操作
                    cleaned_data = self._apply_operations(data, operations, params, stats)
                    
                    if cleaned_data is None:
                        continue  # 被过滤掉的记录
                    
                    # 精确重复检查已废弃
                    
                    # 字段去重检查
                    if dedup_cache is not None and dedup_field and dedup_field in cleaned_data:
                        field_value = str(cleaned_data[dedup_field])
                        if self._is_duplicate(field_value, dedup_cache, self.dedup_threshold):
                            stats['deduplicated'] += 1
                            continue
                        dedup_cache.add(field_value)
                    
                    # 写入清洗后的数据
                    outfile.write(json.dumps(cleaned_data, ensure_ascii=False) + '\n')
                    stats['processed_rows'] += 1
                    
                except json.JSONDecodeError as e:
                    self.logger.warning(f'跳过无效JSON行 {line_num}: {e}')
                    continue
                except Exception as e:
                    self.logger.error(f'处理行 {line_num} 时出错: {e}')
                    continue
        
        stats['final_rows'] = stats['processed_rows']
    
    def _clean_csv(self, source_path: Path, output_file: Path, operations: List[str],
                  params: Dict[str, Any], stats: Dict[str, int], task_id: str) -> None:
        """清洗CSV文件"""
        # 读取CSV
        df = pd.read_csv(source_path)
        stats['total_rows'] = len(df)
        
        # 转换为记录列表进行处理
        records = df.to_dict('records')
        cleaned_records = []
        seen_records = None
        dedup_field = params.get('dedup_field', '')
        dedup_cache = set() if CleaningOperation.DEDUPLICATE in operations else None
        
        for i, record in enumerate(records):
            # 更新进度
            if i % self.chunk_size == 0:
                progress = i / len(records) * 100
                state_manager.update_state(task_id, 'progress', progress)
                state_manager.update_state(task_id, 'processed_rows', i)
            
            # 执行清洗操作
            cleaned_data = self._apply_operations(record, operations, params, stats)
            
            if cleaned_data is None:
                continue
            
            # 精确重复检查已废弃
            
            # 字段去重检查
            if dedup_cache is not None and dedup_field and dedup_field in cleaned_data:
                field_value = str(cleaned_data[dedup_field])
                if self._is_duplicate(field_value, dedup_cache, self.dedup_threshold):
                    stats['deduplicated'] += 1
                    continue
                dedup_cache.add(field_value)
            
            cleaned_records.append(cleaned_data)
            stats['processed_rows'] += 1
        
        # 保存清洗后的数据
        if cleaned_records:
            cleaned_df = pd.DataFrame(cleaned_records)
            if output_file.suffix.lower() == '.csv':
                cleaned_df.to_csv(output_file, index=False, encoding='utf-8')
            elif output_file.suffix.lower() == '.jsonl':
                with open(output_file, 'w', encoding='utf-8') as f:
                    for record in cleaned_records:
                        f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        stats['final_rows'] = len(cleaned_records)
    
    def _clean_excel(self, source_path: Path, output_file: Path, operations: List[str],
                    params: Dict[str, Any], stats: Dict[str, int], task_id: str) -> None:
        """清洗Excel文件"""
        # 读取Excel
        df = pd.read_excel(source_path)
        stats['total_rows'] = len(df)
        
        # 转换为记录列表进行处理
        records = df.to_dict('records')
        cleaned_records = []
        seen_records = None
        dedup_field = params.get('dedup_field', '')
        dedup_cache = set() if CleaningOperation.DEDUPLICATE in operations else None
        
        for i, record in enumerate(records):
            # 更新进度
            if i % self.chunk_size == 0:
                progress = i / len(records) * 100
                state_manager.update_state(task_id, 'progress', progress)
                state_manager.update_state(task_id, 'processed_rows', i)
            
            # 执行清洗操作
            cleaned_data = self._apply_operations(record, operations, params, stats)
            
            if cleaned_data is None:
                continue
            
            # 精确重复检查已废弃
            
            # 字段去重检查
            if dedup_cache is not None and dedup_field and dedup_field in cleaned_data:
                field_value = str(cleaned_data[dedup_field])
                if self._is_duplicate(field_value, dedup_cache, self.dedup_threshold):
                    stats['deduplicated'] += 1
                    continue
                dedup_cache.add(field_value)
            
            cleaned_records.append(cleaned_data)
            stats['processed_rows'] += 1
        
        # 保存清洗后的数据
        if cleaned_records:
            if output_file.suffix.lower() in ['.xlsx', '.xls']:
                cleaned_df = pd.DataFrame(cleaned_records)
                cleaned_df.to_excel(output_file, index=False)
            elif output_file.suffix.lower() == '.jsonl':
                with open(output_file, 'w', encoding='utf-8') as f:
                    for record in cleaned_records:
                        f.write(json.dumps(record, ensure_ascii=False, default=str) + '\n')
        
        stats['final_rows'] = len(cleaned_records)

    def _clean_json(self, source_path: Path, output_file: Path, operations: List[str],
                    params: Dict[str, Any], stats: Dict[str, int], task_id: str) -> None:
        """清洗标准 JSON 文件 (顶层为 list[dict] 或 dict 中包含 list[dict])"""
        try:
            with open(source_path, 'r', encoding='utf-8') as f:
                obj = json.load(f)
        except Exception as e:
            raise ValueError(f'读取 JSON 失败: {e}')

        records = None
        if isinstance(obj, list):
            if all(isinstance(x, dict) for x in obj):
                records = obj
        elif isinstance(obj, dict):
            # 尝试发现第一个 list[dict]
            for v in obj.values():
                if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
                    records = v
                    break
        if records is None:
            raise ValueError('JSON 顶层需为对象数组或包含对象数组的字典')

        stats['total_rows'] = len(records)
        cleaned_records: List[Dict[str, Any]] = []
        dedup_field = params.get('dedup_field', '')
        dedup_cache = set() if CleaningOperation.DEDUPLICATE in operations else None

        for i, record in enumerate(records):
            if i % self.chunk_size == 0:
                progress = i / max(len(records), 1) * 100
                state_manager.update_state(task_id, 'progress', progress)
                state_manager.update_state(task_id, 'processed_rows', i)

            cleaned = self._apply_operations(record, operations, params, stats)
            if cleaned is None:
                continue
            if dedup_cache is not None and dedup_field and dedup_field in cleaned:
                field_value = str(cleaned[dedup_field])
                if self._is_duplicate(field_value, dedup_cache, self.dedup_threshold):
                    stats['deduplicated'] += 1
                    continue
                dedup_cache.add(field_value)
            cleaned_records.append(cleaned)
            stats['processed_rows'] += 1

        # 输出：保持 json / 或用户指定 target_format
        if output_file.suffix.lower() == '.jsonl':
            with open(output_file, 'w', encoding='utf-8') as f:
                for rec in cleaned_records:
                    f.write(json.dumps(rec, ensure_ascii=False) + '\n')
        else:
            # 默认写成 JSON 数组
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(cleaned_records, f, ensure_ascii=False, indent=2)
        stats['final_rows'] = len(cleaned_records)
    
    def _apply_operations(self, data: Dict[str, Any], operations: List[str],
                         params: Dict[str, Any], stats: Dict[str, int]) -> Optional[Dict[str, Any]]:
        """应用清洗操作"""
        if not isinstance(data, dict):
            return None
        
        result = data.copy()
        
        # 去除空值 (支持 any/all)
        if CleaningOperation.REMOVE_EMPTY in operations:
            if self._should_drop_empty(result, params):
                stats['removed_empty'] += 1
                return None

        # 敏感词
        if CleaningOperation.FILTER_SENSITIVE in operations:
            sensitive_words = params.get('sensitive_words', self.default_sensitive_words)
            action = params.get('sensitive_action', 'drop_record')
            replacement = params.get('sensitive_replacement', '***')
            allowed_fields = params.get('sensitive_fields')  # 白名单
            exclude_fields = params.get('sensitive_exclude_fields')  # 黑名单
            field_policy_map = params.get('sensitive_field_policies_parsed')  # 字段策略: field -> (action, replacement)
            use_regex = bool(params.get('sensitive_use_regex'))
            case_sensitive = bool(params.get('sensitive_case_sensitive'))
            hit, modified, dropped = self._process_sensitive(
                result,
                sensitive_words,
                action,
                replacement,
                allowed_fields,
                exclude_fields,
                field_policy_map,
                use_regex,
                case_sensitive,
                stats
            )
            if hit:
                if dropped:
                    stats['filtered_sensitive'] += 1
                    return None
                if modified:
                    stats['filtered_sensitive'] += 1

        # PII 脱敏
        if CleaningOperation.PII_DESENSITIZE in operations:
            categories = params.get('pii_enable', []) or []
            repl_map = params.get('pii_replacements', {}) or {}
            if self._pii_desensitize(result, categories, repl_map):
                stats['desensitized'] += 1

        # 文本标准化
        if CleaningOperation.NORMALIZE_TEXT in operations:
            modes = params.get('normalize_modes', []) or []
            if self._normalize_text(result, modes):
                stats['normalized'] += 1
        
        return result
    
    # 新版辅助方法
    def _should_drop_empty(self, data: Dict[str, Any], params: Dict[str, Any]) -> bool:
        fields = params.get('remove_empty_fields') or list(data.keys())
        mode = (params.get('empty_mode') or 'any').lower()
        empties = 0
        total = 0
        for f in fields:
            if f not in data:
                continue
            total += 1
            v = data.get(f)
            is_empty = (v is None) or (isinstance(v, str) and not v.strip())
            if is_empty:
                empties += 1
                if mode == 'any':
                    return True
        if mode == 'all' and total > 0 and empties == total:
            return True
        return False

    def _process_sensitive(
        self,
        data: Dict[str, Any],
        words: List[str],
        global_action: str,
        global_replacement: str,
        allowed_fields: Optional[List[str]] = None,
        exclude_fields: Optional[List[str]] = None,
        field_policy_map: Optional[Dict[str, Any]] = None,
        use_regex: bool = False,
        case_sensitive: bool = False,
        stats: Optional[Dict[str, Any]] = None
    ):
        """敏感词处理 (增强版)

        支持:
          - allowed_fields / exclude_fields
          - 字段级策略 (action, replacement)
          - 正则模式 / 大小写开关
          - 统计字段与词命中次数
        """
        if not words:
            return (False, False, False)
        # 预构建 regex 或普通匹配资源
        flags = 0 if case_sensitive else re.IGNORECASE
        compiled_patterns: List[Tuple[str, Any]] = []
        if use_regex:
            for w in words:
                try:
                    compiled_patterns.append((w, re.compile(w, flags)))
                except re.error:
                    self.logger.warning(f'无效正则敏感词跳过: {w}')
        else:
            # 普通词，转义后用于 re.sub
            for w in words:
                pattern = re.compile(re.escape(w), flags)
                compiled_patterns.append((w, pattern))

        allowed_set = set(allowed_fields) if allowed_fields else None
        exclude_set = set(exclude_fields) if exclude_fields else None
        field_policy_map = field_policy_map or {}
        field_hits = stats['sensitive_detail']['field_hits'] if stats else None
        word_hits = stats['sensitive_detail']['word_hits'] if stats else None

        hit_any = False
        modified_any = False

        for field, value in list(data.items()):
            if not isinstance(value, str):
                continue
            if allowed_set is not None and field not in allowed_set:
                continue
            if exclude_set is not None and field in exclude_set:
                continue

            # 字段策略优先
            action = global_action
            replacement = global_replacement
            if field in field_policy_map:
                fa = field_policy_map[field]
                # fa: (action, replacement or None)
                if isinstance(fa, (list, tuple)):
                    if len(fa) >= 1 and fa[0]:
                        action = fa[0]
                    if len(fa) >= 2 and fa[1] is not None:
                        replacement = fa[1]

            original = value
            new_val = value

            for raw_pat, comp_pat in compiled_patterns:
                # 快速包含检查（普通模式时可以先做）
                if not use_regex and (case_sensitive and raw_pat not in new_val) and (not case_sensitive and raw_pat.lower() not in new_val.lower()):
                    continue
                if action == 'drop_record':
                    # 只要任一命中即可丢弃整条
                    if comp_pat.search(new_val):
                        if field_hits is not None:
                            field_hits[field] = field_hits.get(field, 0) + 1
                        if word_hits is not None:
                            word_hits[raw_pat] = word_hits.get(raw_pat, 0) + 1
                        return (True, False, True)
                elif action in ('remove_word', 'replace_word'):
                    rep_text = '' if action == 'remove_word' else replacement
                    new_val, count = comp_pat.subn(rep_text, new_val)
                    if count:
                        hit_any = True
                        modified_any = True
                        if field_hits is not None:
                            field_hits[field] = field_hits.get(field, 0) + count
                        if word_hits is not None:
                            word_hits[raw_pat] = word_hits.get(raw_pat, 0) + count
                else:
                    # 未知动作: 视为 replace_word 使用全局 replacement
                    new_val, count = comp_pat.subn(replacement, new_val)
                    if count:
                        hit_any = True
                        modified_any = True
                        if field_hits is not None:
                            field_hits[field] = field_hits.get(field, 0) + count
                        if word_hits is not None:
                            word_hits[raw_pat] = word_hits.get(raw_pat, 0) + count

            if new_val != original:
                data[field] = new_val

        return (hit_any, modified_any, False)

    # _legacy_desensitize_fields 已移除（v3 之后不再支持）

    _PII_PATTERNS = {
        # 身份证号: 去掉首尾\b，改用前后断言，避免前面是中文导致 \b 不匹配
        'id_card': re.compile(r'(?<![0-9A-Za-z])\d{6}(19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{3}[0-9Xx](?![0-9A-Za-z])'),
        'phone': re.compile(r'\b1[3-9]\d{9}\b'),
        'email': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'),
        'bank_card': re.compile(r'\b\d{13,19}\b'),
        'ip': re.compile(r'\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d?\d)\b'),
        'passport': re.compile(r'\b(?:[EePpKkSsGgDd]\d{8})\b')
    }

    def _pii_desensitize(self, data: Dict[str, Any], categories: List[str], repl_map: Dict[str, str]) -> bool:
        modified = False
        active = [c for c in categories if c in self._PII_PATTERNS]
        if not active:
            return False
        for field, value in data.items():
            if not isinstance(value, str):
                continue
            new_val = value
            for cat in active:
                patt = self._PII_PATTERNS[cat]
                rep_default = repl_map.get(cat) or repl_map.get('default') or f'<{cat}>'
                if cat == 'email':
                    # 仅替换本地部分，保留域名
                    def _sub_email(m):
                        full = m.group(0)
                        local, domain = full.split('@', 1)
                        return (rep_default if rep_default != f'<{cat}>' else '<EMAIL>') + '@' + domain
                    new_val, count = patt.subn(_sub_email, new_val)
                else:
                    new_val, count = patt.subn(rep_default, new_val)
                if count:
                    modified = True
            if new_val != value:
                data[field] = new_val
        return modified

    def _normalize_text(self, data: Dict[str, Any], modes: List[str]) -> bool:
        import unicodedata
        modes = set(modes or [])
        modified = False
        def fullwidth_to_halfwidth(s: str) -> str:
            res = []
            for ch in s:
                code = ord(ch)
                if 0xFF01 <= code <= 0xFF5E:
                    res.append(chr(code - 0xFEE0))
                elif code == 0x3000:
                    res.append(' ')
                else:
                    res.append(ch)
            return ''.join(res)
        for field, value in data.items():
            if not isinstance(value, str):
                continue
            orig = value
            text = value.strip()
            text = re.sub(r'\s+', ' ', text)
            if 'collapse_newlines' in modes:
                text = re.sub(r'(\n\s*){2,}', '\n', text)
            if 'unicode_nfc' in modes:
                text = unicodedata.normalize('NFC', text)
            if 'fullwidth' in modes:
                text = fullwidth_to_halfwidth(text)
            if 'lowercase' in modes:
                text = text.lower()
            if text != orig:
                data[field] = text
                modified = True
        return modified
    
    def _is_duplicate(self, text: str, cache: Set[str], threshold: float) -> bool:
        """检查文本是否重复（基于相似度）"""
        for cached_text in cache:
            similarity = SequenceMatcher(None, text, cached_text).ratio()
            if similarity >= threshold:
                return True
        return False
    
    def _generate_cleaning_report(self, task_id: str, params: Dict[str, Any], 
                                 stats: Dict[str, int]) -> None:
        """生成清洗报告"""
        try:
            task_dir = self.output_dir / task_id
            report_path = task_dir / 'clean_report.json'
            # 计算已使用与未使用参数 (用于排查 UI 透传但未生效的配置)
            param_keys = {k for k in params.keys() if k not in ['operations', 'source_path']}
            op_used: Dict[str, Set[str]] = {
                'remove_empty': {'remove_empty_fields', 'empty_mode'},
                'deduplicate': {'dedup_field', 'dedup_threshold'},
                'filter_sensitive': {'sensitive_words', 'sensitive_action', 'sensitive_replacement', 'sensitive_fields',
                                      'sensitive_exclude_fields', 'sensitive_field_policies', 'sensitive_field_policies_parsed',
                                      'sensitive_use_regex', 'sensitive_case_sensitive'},
                'pii_desensitize': {'pii_enable', 'pii_replacements'},
                'normalize_text': {'normalize_modes'},
                'desensitize': {'desensitize_fields'}
            }
            used_params: Set[str] = set()
            for op in params.get('operations', []):
                used_params.update(op_used.get(op, set()))
            # 只保留真实存在于参数中的
            used_params = {k for k in used_params if k in param_keys}
            unused_params = sorted(list(param_keys - used_params))
            
            report = {
                'task_id': task_id,
                'source_file': params['source_path'],
                'operations': params['operations'],
                'parameters': {k: v for k, v in params.items() if k not in ['operations', 'source_path']},
                'used_parameters': sorted(list(used_params)),
                'unused_parameters': unused_params,
                'parameter_explain': {
                    'remove_empty': '按 remove_empty_fields + empty_mode 过滤空值记录 (any=任一为空剔除 / all=全部为空剔除)',
                    'deduplicate': '基于相似度的模糊去重(占位); 参数: dedup_field, dedup_threshold',
                    'filter_sensitive': '敏感词动作: drop_record|remove_word|replace_word; 参数: sensitive_words, sensitive_action, sensitive_replacement',
                    'pii_desensitize': '按正则匹配 PII 并替换; 参数: pii_enable, pii_replacements(default+分类)',
                    'normalize_text': '文本规范化: 基础空白折叠 + 可选 unicode_nfc/fullwidth/lowercase/collapse_newlines',
                    'desensitize': '旧版字段脱敏 (字段级 hide/mask/phone/email)' 
                },
                'statistics': stats,
                'summary': {
                    'total_input_rows': stats['total_rows'],
                    'total_output_rows': stats['final_rows'],
                    'reduction_rate': (1 - stats['final_rows'] / stats['total_rows']) * 100 if stats['total_rows'] > 0 else 0,
                    'operations_applied': len(params['operations'])
                },
                'sensitive_detail': stats.get('sensitive_detail'),
                'generated_time': datetime.now().isoformat()
            }
            
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f'清洗报告已生成: {report_path}')
            
        except Exception as e:
            self.logger.error(f'生成清洗报告失败: {e}')
    
    def _generate_metadata(self, task_id: str, params: Dict[str, Any], 
                          stats: Dict[str, int], output_file: str) -> None:
        """生成元数据文件"""
        try:
            task_dir = self.output_dir / task_id
            meta_path = task_dir / 'meta.json'
            
            metadata = {
                'task_id': task_id,
                'task_type': 'data_clean',
                'source_path': params['source_path'],
                'output_path': output_file,
                'params': params,
                'start_time': state_manager.get_task_state(task_id).get('start_time', ''),
                'end_time': state_manager.get_task_state(task_id).get('end_time', ''),
                'input_row_count': stats['total_rows'],
                'output_row_count': stats['final_rows'],
                'file_size': Path(output_file).stat().st_size if Path(output_file).exists() else 0,
                'operations_summary': {
                    'removed_empty': stats['removed_empty'],
                    'deduplicated': stats['deduplicated'],
                    'filtered_sensitive': stats['filtered_sensitive'],
                    'desensitized': stats['desensitized'],
                    'normalized': stats['normalized'],
                    # removed_duplicates 项已移除
                }
            }
            
            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f'元数据已生成: {meta_path}')
            
        except Exception as e:
            self.logger.error(f'生成元数据失败: {e}')
    
    def get_clean_progress(self, task_id: str) -> Dict[str, Any]:
        """
        获取清洗进度
        
        Args:
            task_id (str): 任务ID
            
        Returns:
            dict: 进度信息
        """
        try:
            task_state = state_manager.get_task_state(task_id)
            if not task_state:
                return {'error': '任务不存在'}
            
            progress_info = {
                'task_id': task_id,
                'status': task_state.get('status', 'unknown'),
                'progress': task_state.get('progress', 0),
                'processed_rows': task_state.get('processed_rows', 0),
                'start_time': task_state.get('start_time', ''),
                'error_msg': task_state.get('error_msg', ''),
                'params': task_state.get('params', {})
            }
            
            return progress_info
            
        except Exception as e:
            self.logger.error(f'获取清洗进度失败: {e}')
            return {'error': str(e)}
    
    def get_clean_report(self, task_id: str) -> Dict[str, Any]:
        """
        获取清洗报告
        
        Args:
            task_id (str): 任务ID
            
        Returns:
            dict: 清洗报告
        """
        try:
            task_dir = self.output_dir / task_id
            report_path = task_dir / 'clean_report.json'
            
            if not report_path.exists():
                return {'error': '清洗报告不存在'}
            
            with open(report_path, 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            return report
            
        except Exception as e:
            self.logger.error(f'获取清洗报告失败: {e}')
            return {'error': str(e)}


# 全局数据清洗器实例
data_cleaner = DataCleaner()


if __name__ == "__main__":
    """
    命令行入口，用于数据清洗操作
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='数据清洗工具')
    subparsers = parser.add_subparsers(dest='action', help='可用操作')
    
    # clean命令
    clean_parser = subparsers.add_parser('clean', help='执行数据清洗')
    clean_parser.add_argument('--source', required=True, help='源文件路径')
    clean_parser.add_argument('--operations', nargs='+', required=True,
                 choices=['remove_empty', 'deduplicate', 'filter_sensitive',
                       'desensitize', 'pii_desensitize', 'normalize_text'],
                 help='清洗操作 (可多选)')
    clean_parser.add_argument('--remove-empty-fields', nargs='+', help='去空字段 (可选, 默认全部字段)')
    clean_parser.add_argument('--empty-mode', choices=['any', 'all'], default='any', help='空值判定策略 any=任一为空丢弃 / all=全部为空丢弃')
    clean_parser.add_argument('--dedup-field', help='去重字段 (模糊去重占位)')
    clean_parser.add_argument('--dedup-threshold', type=float, help='模糊去重相似度阈值 (0-1)')
    clean_parser.add_argument('--sensitive-words', nargs='+', help='敏感词列表')
    clean_parser.add_argument('--sensitive-action', choices=['remove_word', 'replace_word', 'drop_record'], default='drop_record', help='敏感词处理动作')
    clean_parser.add_argument('--sensitive-replacement', default='***', help='动作=replace_word 时的替换文本')
    clean_parser.add_argument('--pii-enable', nargs='+', help='启用的 PII 类型: id_card phone email bank_card ip passport')
    clean_parser.add_argument('--pii-replacement-default', help='PII 默认替换文本 (默认 <类型>)')
    clean_parser.add_argument('--pii-replacement', nargs='*', help='单独替换 形式: 类型:文本 例如 id_card:<ID> phone:<TEL>')
    clean_parser.add_argument('--normalize-modes', nargs='+', help='文本标准化模式: unicode_nfc fullwidth lowercase collapse_newlines')
    clean_parser.add_argument('--target-format', help='目标格式 (默认与源一致)')
    clean_parser.add_argument('--output-dir', help='输出目录')
    
    # progress命令
    progress_parser = subparsers.add_parser('progress', help='查看清洗进度')
    progress_parser.add_argument('--task-id', required=True, help='任务ID')
    
    # report命令
    report_parser = subparsers.add_parser('report', help='查看清洗报告')
    report_parser.add_argument('--task-id', required=True, help='任务ID')
    
    args = parser.parse_args()
    
    if args.action == 'clean':
        # 构建清洗参数
        params = {
            'source_path': args.source,
            'operations': args.operations
        }
        
        if args.remove_empty_fields:
            params['remove_empty_fields'] = args.remove_empty_fields
        
        if args.dedup_field:
            params['dedup_field'] = args.dedup_field
        if args.dedup_threshold is not None:
            params['dedup_threshold'] = args.dedup_threshold
        
        if args.sensitive_words:
            params['sensitive_words'] = args.sensitive_words
            params['sensitive_action'] = args.sensitive_action
            params['sensitive_replacement'] = args.sensitive_replacement

        if args.pii_enable:
            params['pii_enable'] = args.pii_enable
            repl_map = {}
            if args.pii_replacement_default:
                repl_map['default'] = args.pii_replacement_default
            if args.pii_replacement:
                for item in args.pii_replacement:
                    if ':' in item:
                        k, v = item.split(':', 1)
                        repl_map[k] = v
            if repl_map:
                params['pii_replacements'] = repl_map

        if args.normalize_modes:
            params['normalize_modes'] = args.normalize_modes
        if args.empty_mode:
            params['empty_mode'] = args.empty_mode
        
        if args.target_format:
            params['target_format'] = args.target_format
        
        if args.output_dir:
            params['output_dir'] = args.output_dir
        
        try:
            task_id = data_cleaner.start_clean(params)
            print(f"✓ 清洗任务已启动: {task_id}")
        except Exception as e:
            print(f"✗ 清洗任务启动失败: {e}")
    
    elif args.action == 'progress':
        progress = data_cleaner.get_clean_progress(args.task_id)
        if 'error' in progress:
            print(f"✗ 获取进度失败: {progress['error']}")
        else:
            print(f"任务 {args.task_id} 进度:")
            print(f"  状态: {progress['status']}")
            print(f"  进度: {progress['progress']:.1f}%")
            print(f"  已处理行数: {progress['processed_rows']}")
            if progress.get('error_msg'):
                print(f"  错误: {progress['error_msg']}")
    
    elif args.action == 'report':
        report = data_cleaner.get_clean_report(args.task_id)
        if 'error' in report:
            print(f"✗ 获取报告失败: {report['error']}")
        else:
            print(f"任务 {args.task_id} 清洗报告:")
            print(f"  源文件: {report['source_file']}")
            print(f"  操作: {', '.join(report['operations'])}")
            
            stats = report['statistics']
            print(f"  统计信息:")
            print(f"    输入行数: {stats['total_rows']}")
            print(f"    输出行数: {stats['final_rows']}")
            print(f"    去空记录: {stats['removed_empty']}")
            print(f"    去重记录: {stats['deduplicated']}")
            print(f"    过滤敏感: {stats['filtered_sensitive']}")
            print(f"    脱敏记录: {stats['desensitized']}")
            print(f"    标准化: {stats['normalized']}")
            # removed_duplicates 已移除，不再输出
            
            summary = report['summary']
            print(f"  汇总:")
            print(f"    数据减少率: {summary['reduction_rate']:.1f}%")
            print(f"    应用操作数: {summary['operations_applied']}")
    
    else:
        parser.print_help()
