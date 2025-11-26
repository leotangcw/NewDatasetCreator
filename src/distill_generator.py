#!/usr/bin/env python3
"""
蒸馏生成模块

本模块负责基于模型的数据蒸馏生成功能，支持多种生成策略和模型类型。
功能特点：
- 多模型支持（OpenAI、本地LLM等）
- 多种生成策略（扩充、转换、增强）
- 智能提示工程
- 质量评估和过滤
- 大规模并发生成

设计原则：
- 高质量数据生成
- 可配置的生成策略
- 智能提示管理
- 完整的生成记录

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import os
import json
import time
import asyncio
import random
import string
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import threading
from collections import deque

# 可选：JSON数组流式解析（大 JSON 支持）
from .dependencies import ijson

# 导入统一异常类
try:
    from .exceptions import DistillError, DistillFailedError
except ImportError:
    # 如果导入失败，使用本地定义（向后兼容）
    class DistillError(Exception):
        """蒸馏生成相关异常类"""
        pass
    
    class DistillFailedError(DistillError):
        """蒸馏生成失败异常"""
        pass

# 基础支撑层导入
try:
    # 作为模块导入时使用相对导入
    from .config_manager import config_manager
    from .log_manager import log_manager
    from .state_manager import state_manager, TaskType
    from .utils import DataProcessing
    from .model_manager import model_manager
except ImportError:
    # 直接运行时使用绝对导入
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from config_manager import config_manager
    from log_manager import log_manager
    from state_manager import state_manager, TaskType
    from utils import DataProcessing
    from model_manager import model_manager


class GenerationStrategy:
    """生成策略常量"""
    EXPAND = "expand"               # 数据扩充
    ENHANCE = "enhance"             # 数据增强
    PARAPHRASE = "paraphrase"       # 文本改写
    CLASSIFY_LABEL = "classify_label"  # 分类标注
    Q_TO_A = "q_to_a"               # 从Q生A
    CUSTOM = "custom"               # 自定义


class QualityMetric:
    """质量评估指标"""
    RELEVANCE = "relevance"       # 相关性
    COHERENCE = "coherence"       # 连贯性
    UNIQUENESS = "uniqueness"     # 独特性
    COMPLETENESS = "completeness" # 完整性
    ACCURACY = "accuracy"         # 准确性


class DistillGenerator:
    """
    蒸馏生成器
    
    负责基于模型的数据生成，支持多种生成策略和质量控制。
    """
    
    def __init__(self):
        """初始化蒸馏生成器"""
        self.logger = log_manager.get_logger('distill_generator')
        
        # 获取配置
        self.root_dir = Path(config_manager.get_config('base.root_dir', './data'))
        self.output_dir = self.root_dir / 'distilled'
        self.max_workers = config_manager.get_config('generation.max_workers', 4)
        self.batch_size = config_manager.get_config('generation.batch_size', 10)
        self.max_retries = config_manager.get_config('generation.max_retries', 3)
        self.quality_threshold = config_manager.get_config('generation.quality_threshold', 0.7)
        
        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 支持的生成策略
        self.supported_strategies = [
            GenerationStrategy.EXPAND,
            GenerationStrategy.ENHANCE,
            GenerationStrategy.PARAPHRASE,
            GenerationStrategy.CLASSIFY_LABEL,
            GenerationStrategy.Q_TO_A,
            GenerationStrategy.CUSTOM
        ]
        
        # 提示模板
        self.prompt_templates = self._load_prompt_templates()
        
        # 线程锁
        self._lock = threading.Lock()
        # 简单的请求速率限制器（线程共享，窗口为1秒）
        self._rl_lock = threading.Lock()
        self._rl_window_start = 0.0
        self._rl_count = 0
        
        # 扫描本地任务以恢复状态
        # 注释掉自动扫描，避免已删除但保留文件的任务被意外恢复
        # self.scan_local_tasks()
        
        self.logger.info('蒸馏生成器初始化完成')

    def _acquire_rate_limit(self, rps: Optional[float]):
        """粗粒度限流：每秒最多 rps 次进入。rps<=0 表示不限速。"""
        try:
            if not rps or float(rps) <= 0:
                return
        except Exception:
            return
        now = time.time()
        with self._rl_lock:
            if now - self._rl_window_start >= 1.0:
                self._rl_window_start = now
                self._rl_count = 0
            if self._rl_count >= int(rps):
                sleep_time = 1.0 - (now - self._rl_window_start)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                # 新窗口
                self._rl_window_start = time.time()
                self._rl_count = 0
            self._rl_count += 1
    
    def _load_prompt_templates(self) -> Dict[str, str]:
        """加载提示模板"""
        templates = {
        GenerationStrategy.EXPAND: """
{sys_prefix}
原始样本：
{sample}

任务：参考上述样本，生成{count}个新的数据样本。
{field_hint}

格式要求：
1. 必须返回标准的JSON格式数据列表。
2. 保持数据结构与原始样本一致。

请严格遵循系统提示词中的风格和内容要求进行生成。
""",
            
        GenerationStrategy.ENHANCE: """
{sys_prefix}
原始数据：
{data}

任务：对上述数据进行增强。

格式要求：
1. 返回增强后的JSON格式数据。
2. 保持原有信息的准确性。

请严格遵循系统提示词中的风格和内容要求进行增强。
""",
            
        GenerationStrategy.PARAPHRASE: """
{sys_prefix}
原文：
{text}

任务：对上述文本进行改写，生成{count}个版本。

格式要求：
1. 必须返回改写后的文本列表。
2. 仅返回文本内容，不要包含任何解释或序号。

请严格遵循系统提示词中的风格和内容要求进行改写。
""",
            
        GenerationStrategy.CLASSIFY_LABEL: """
{sys_prefix}
数据：
{data}

任务：为上述数据生成分类标签。{label_hint}

格式要求：
1. 请将最终的分类标签放在 \\boxed{{}} 中，例如 \\boxed{{标签名}}。
2. 不要输出任何其他解释或JSON格式，仅输出包含在boxed中的标签。

请严格遵循系统提示词中的要求。
""",
            
    GenerationStrategy.Q_TO_A: """
{sys_prefix}
问题：
{question}

任务：请回答上述问题。
""",
            
        GenerationStrategy.CUSTOM: """{sys_prefix}
{custom_body}
""",
        }

        return templates
    
    def start_generation(self, params: Dict[str, Any]) -> str:
        """
        启动数据生成任务
        
        Args:
            params (dict): 生成参数
            
        Returns:
            str: 任务ID
        """
        try:
            # 生成任务ID
            task_id = f"distill_{int(time.time())}"
            
            # 验证参数
            self._validate_params(params)
            
            # 创建任务目录
            task_dir = self.output_dir / task_id
            task_dir.mkdir(parents=True, exist_ok=True)
            
            # 记录任务状态
            # 使用标准任务类型与子类型（子类型用策略名便于过滤/统计）
            state_manager.add_task(
                TaskType.DISTILL,
                params.get('strategy', 'data_distill'),
                params,
                task_id=task_id
            )
            
            # 更新任务状态为运行中
            state_manager.update_state(task_id, 'status', 'running')
            state_manager.update_state(task_id, 'start_time', datetime.now().isoformat())
            
            # 启动后台生成任务
            threading.Thread(
                target=self._execute_generation,
                args=(task_id, params),
                daemon=True
            ).start()
            
            self.logger.info(f'数据生成任务启动: {task_id}')
            return task_id
            
        except Exception as e:
            self.logger.error(f'数据生成任务启动失败: {e}')
            if 'task_id' in locals():
                state_manager.update_state(task_id, 'status', 'failed')
                state_manager.update_state(task_id, 'error_msg', str(e))
            raise
    
    def _validate_params(self, params: Dict[str, Any]) -> None:
        """验证生成参数"""
        required_fields = ['strategy', 'model_id']
        for field in required_fields:
            if field not in params:
                raise ValueError(f'缺少必填参数: {field}')
        
        # 验证生成策略
        strategy = params['strategy']
        if strategy not in self.supported_strategies:
            raise ValueError(f'不支持的生成策略: {strategy}')
        
        # 验证模型
        model_id = params['model_id']
        active_models = model_manager.get_active_models()
        # active_models 是包含字典的列表，需提取 name 字段进行匹配
        active_names = {m.get('name') for m in active_models if isinstance(m, dict)}
        if model_id not in active_names:
            # 兼容某些情况下传入的是具体后端模型名，尝试允许跳过严格校验
            self.logger.warning(f"模型 '{model_id}' 不在在线列表中，将尝试仍然调用。请确认模型已配置并可用。")
        
        # 统一参数：目标字段与选择字段
        if 'target_field' not in params or not params.get('target_field'):
            params['target_field'] = 'output'
        if 'selected_fields' in params and isinstance(params['selected_fields'], str):
            # 支持逗号分隔
            params['selected_fields'] = [f.strip() for f in params['selected_fields'].split(',') if f.strip()]

        # 验证特定策略的参数
        if strategy in [GenerationStrategy.EXPAND, GenerationStrategy.PARAPHRASE, GenerationStrategy.Q_TO_A]:
            if 'generation_count' not in params:
                params['generation_count'] = 5  # 默认生成5个

        if strategy == GenerationStrategy.CLASSIFY_LABEL:
            # 可选：标签集合，逗号分隔（支持中英文逗号）
            if 'label_set' in params and isinstance(params['label_set'], str):
                raw_labels = params['label_set'].replace('，', ',')
                params['label_set'] = [s.strip() for s in raw_labels.split(',') if s.strip()]
        
        # 验证输入数据
        if 'input_data' not in params and 'input_file' not in params:
            raise ValueError('需要指定input_data或input_file')
        
        if 'input_file' in params:
            input_file = Path(params['input_file'])
            if not input_file.exists():
                raise FileNotFoundError(f'输入文件不存在: {input_file}')
    
    def _get_processed_signatures(self, output_file: Path, params: Dict[str, Any]) -> set:
        """获取已处理的数据签名集合"""
        signatures = set()
        if not output_file.exists():
            return signatures
            
        q_field = params.get('q_field_name', 'instruction')
        
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                        # 优先使用 id
                        if 'id' in item:
                            signatures.add(str(item['id']))
                            continue
                            
                        # 其次尝试提取文本特征
                        q_text = item.get(q_field) or item.get('instruction') or item.get('question') or item.get('input')
                        if q_text:
                            signatures.add(hashlib.md5(str(q_text).encode('utf-8')).hexdigest())
                        else:
                            # 最后使用整个内容的哈希
                            signatures.add(hashlib.md5(line.encode('utf-8')).hexdigest())
                    except Exception:
                        continue
        except Exception as e:
            self.logger.warning(f'读取已生成文件失败: {e}')
            
        return signatures

    def _execute_generation(self, task_id: str, params: Dict[str, Any]) -> None:
        """执行数据生成"""
        try:
            # 若为JSONL输入，启用流式并发与断点续跑
            if 'input_file' in params and str(params['input_file']).lower().endswith('.jsonl'):
                return self._execute_generation_streaming_jsonl(task_id, params)

            # 可选：对超大 JSON 数组启用流式解析，转为临时 JSONL 后复用管线
            if 'input_file' in params and str(params['input_file']).lower().endswith('.json'):
                if bool(params.get('stream_json_array', False)):
                    tmp_jsonl = self._json_array_to_temp_jsonl(task_id, Path(params['input_file']))
                    if tmp_jsonl:
                        new_params = dict(params)
                        new_params['input_file'] = str(tmp_jsonl)
                        return self._execute_generation_streaming_jsonl(task_id, new_params)

            # 非JSONL走内存模式（适用于较小数据集）
            input_data = self._prepare_input_data(params)
            
            # 获取生成参数
            strategy = params['strategy']
            model_id = params['model_id']
            # 兼容旧参数名 generations_per_item
            generation_count = params.get('generation_count', params.get('generations_per_item', 1))
            
            # 初始化生成统计
            generation_stats = {
                'total_input': len(input_data),
                'total_generated': 0,
                'successful_generations': 0,
                'failed_generations': 0,
                'quality_passed': 0,
                'quality_failed': 0
            }
            
            # 创建输出文件
            task_dir = self.output_dir / task_id
            output_file = task_dir / 'generated_data.jsonl'
            
            # 获取已处理的签名 (用于断点续传)
            processed_signatures = self._get_processed_signatures(output_file, params)
            
            # 执行生成 (使用追加模式)
            with open(output_file, 'a', encoding='utf-8') as f:
                if strategy in [GenerationStrategy.EXPAND, GenerationStrategy.PARAPHRASE, GenerationStrategy.Q_TO_A, GenerationStrategy.CUSTOM, GenerationStrategy.CLASSIFY_LABEL]:
                    self._generate_multiple(task_id, input_data, strategy, model_id, 
                                          generation_count, f, generation_stats, params, processed_signatures)
                else:
                    self._generate_single(task_id, input_data, strategy, model_id, 
                                        f, generation_stats, params, processed_signatures)
            
            # 若任务被暂停，则生成阶段性报告但不标记完成
            current_state = state_manager.get_task_state(task_id) or {}
            if current_state.get('status') == 'paused':
                try:
                    self._generate_quality_report(task_id, params, generation_stats, is_partial=True)
                except Exception as _:
                    pass
                self.logger.info(f'任务已暂停，保留部分结果并输出阶段性报告: {task_id}')
                return

            # 生成质量报告
            self._generate_quality_report(task_id, params, generation_stats)
            
            # 生成元数据
            self._generate_metadata(task_id, params, generation_stats, str(output_file))
            
            # 更新任务状态为完成
            state_manager.update_state(task_id, 'status', 'completed')
            state_manager.update_state(task_id, 'end_time', datetime.now().isoformat())
            
            self.logger.info(f'数据生成任务完成: {task_id}')
            
        except Exception as e:
            self.logger.error(f'数据生成任务执行失败: {e}')
            state_manager.update_state(task_id, 'status', 'failed')
            state_manager.update_state(task_id, 'error_msg', str(e))
    
    def _load_checkpoint(self, task_dir: Path) -> Dict[str, Any]:
        """加载checkpoint（增强版：支持备份恢复）"""
        ckpt_path = task_dir / 'checkpoint.json'
        tmp_path = task_dir / 'checkpoint.json.tmp'
        
        # 尝试加载主checkpoint
        if ckpt_path.exists():
            try:
                with open(ckpt_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 简单验证数据完整性
                    if 'task_id' in data and 'line_index' in data:
                        return data
            except Exception as e:
                self.logger.warning(f'加载checkpoint失败: {e}，尝试加载备份')
        
        # 尝试加载备份
        if tmp_path.exists():
            try:
                with open(tmp_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'task_id' in data and 'line_index' in data:
                        self.logger.info('成功从备份恢复checkpoint')
                        return data
            except Exception:
                pass
                
        return {}

    def _save_checkpoint(self, task_dir: Path, ckpt: Dict[str, Any]) -> None:
        """保存checkpoint（原子写）"""
        try:
            ckpt_path = task_dir / 'checkpoint.json'
            tmp_path = task_dir / 'checkpoint.json.tmp'
            ckpt['updated_at'] = datetime.now().isoformat()
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(ckpt, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, ckpt_path)
        except Exception as e:
            self.logger.warning(f'保存checkpoint失败: {e}')

    def _count_lines(self, file_path: Path) -> int:
        """统计文件行数（用于进度估计）"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return sum(1 for _ in f)
        except Exception:
            return 0

    def _json_array_to_temp_jsonl(self, task_id: str, json_path: Path) -> Optional[Path]:
        """将超大JSON数组以流式方式转写到任务目录的临时JSONL文件。
        需要 ijson；若不可用或解析失败则返回None。
        """
        try:
            if not json_path.exists():
                return None
            task_dir = self.output_dir / task_id
            task_dir.mkdir(parents=True, exist_ok=True)
            out_path = task_dir / 'input_stream.jsonl'

            # 若 ijson 不可用，回退为 None
            if ijson is None:
                self.logger.warning('ijson 未安装，无法启用 JSON 数组流式解析，回退为常规加载。')
                return None

            count = 0
            with open(json_path, 'rb') as f_in, open(out_path, 'w', encoding='utf-8') as f_out:
                for item in ijson.items(f_in, 'item'):
                    try:
                        f_out.write(json.dumps(item, ensure_ascii=False) + "\n")
                        count += 1
                        if count % 10000 == 0:
                            self.logger.info(f'JSON数组转写进度: {count} 行 -> {out_path.name}')
                    except Exception as ie:
                        self.logger.warning(f'写入临时JSONL失败，跳过一项: {ie}')
                        continue
            self.logger.info(f'JSON数组已转写为临时JSONL: {out_path}（共 {count} 行）')
            return out_path
        except Exception as e:
            self.logger.warning(f'JSON数组转写JSONL失败: {e}')
            return None

    def _execute_generation_streaming_jsonl(self, task_id: str, params: Dict[str, Any]) -> None:
        """JSONL 流式并发执行，保序写入 + 断点续跑"""
        strategy = params['strategy']
        model_id = params['model_id']
        # 兼容旧参数名 generations_per_item
        generation_count = params.get('generation_count', params.get('generations_per_item', 1))
        input_file = Path(params['input_file'])
        task_dir = self.output_dir / task_id
        task_dir.mkdir(parents=True, exist_ok=True)

        # 初始化/加载 checkpoint
        ckpt = self._load_checkpoint(task_dir) or {}
        
        # 确保状态同步：强制更新为运行中
        state_manager.update_state(task_id, 'status', 'running')
        
        if not ckpt:
            # 首次创建
            total_lines = self._count_lines(input_file)
            ckpt = {
                'task_id': task_id,
                'input_file': str(input_file),
                'format': 'jsonl',
                'line_index': 0,           # 已顺序写入的最后一行编号
                'written_count': 0,        # 已写入的条目数
                'total_lines': total_lines,
                'status': 'running',
                'params': {k: v for k, v in params.items() if k != 'input_data'},
                'started_at': datetime.now().isoformat(),
            }
            self._save_checkpoint(task_dir, ckpt)
        else:
            # 恢复时也更新状态
            ckpt['status'] = 'running'
            self._save_checkpoint(task_dir, ckpt)

        # 若有覆盖参数（例如更换模型），以 params 为准，合并存回 ckpt
        for k, v in params.items():
            if k != 'input_data':
                ckpt.setdefault('params', {})[k] = v
        model_id = ckpt['params'].get('model_id', model_id)
        # 兼容旧参数名 generations_per_item
        generation_count = int(ckpt['params'].get('generation_count', ckpt['params'].get('generations_per_item', generation_count)))
        self._save_checkpoint(task_dir, ckpt)

        # 输出文件：若 line_index > 0 则追加写
        output_path = task_dir / 'generated_data.jsonl'
        mode = 'a' if ckpt.get('line_index', 0) > 0 and output_path.exists() else 'w'

        stats = {
            'total_input': ckpt.get('total_lines', 0),
            'total_generated': 0,
            'successful_generations': 0,
            'failed_generations': 0,
            'quality_passed': 0,
            'quality_failed': 0
        }

        fsync_interval = int(params.get('fsync_interval', 50))
        write_counter = 0
        checkpoint_interval = int(params.get('checkpoint_interval', 100))

        unordered = bool(params.get('unordered_write', False))
        if not unordered:
            next_to_write = ckpt.get('line_index', 0) + 1
            pending_results: Dict[int, List[Dict[str, Any]]] = {}
        else:
            next_to_write = None  # 乱序模式不使用
            pending_results = {}

        inflight: Dict[Any, int] = {}
        local_max_workers = int(params.get('max_workers', self.max_workers))
        inflight_multiplier = int(params.get('inflight_multiplier', 4))
        max_inflight = max(local_max_workers * inflight_multiplier, local_max_workers)

        try:
            with open(output_path, mode, encoding='utf-8') as out_f:
                with open(input_file, 'r', encoding='utf-8') as in_f:
                    line_no = 0
                    executor = ThreadPoolExecutor(max_workers=local_max_workers)
                    try:
                        # 读取并提交
                        for raw_line in in_f:
                            line_no += 1
                            # 跳过已完成的部分
                            if line_no <= ckpt.get('line_index', 0):
                                continue

                            # 检查暂停
                            state = state_manager.get_task_state(task_id) or {}
                            if state.get('status') == 'paused':
                                self.logger.info(f'检测到暂停，停止读取: {task_id}')
                                break

                            if not raw_line.strip():
                                if not unordered:
                                    pending_results[line_no] = []
                                else:
                                    pass
                            else:
                                try:
                                    data_item = json.loads(raw_line.strip())
                                except Exception as e:
                                    self.logger.warning(f'第{line_no}行解析失败，跳过: {e}')
                                    if not unordered:
                                        pending_results[line_no] = []
                                else:
                                    # 控制并发窗口
                                    while len(inflight) >= max_inflight:
                                        # 使用 wait 替代 as_completed 以支持超时监控
                                        done, _ = wait(list(inflight.keys()), timeout=30, return_when=FIRST_COMPLETED)
                                        if not done:
                                            self.logger.warning(f"等待任务完成超时 (30s)，当前并发: {len(inflight)}，可能模型响应较慢")
                                            continue
                                        
                                        # 处理已完成的任务（取出一个）
                                        done_fut = list(done)[0]
                                        done_line = inflight.pop(done_fut)
                                        try:
                                            gen_items = done_fut.result()
                                        except Exception as e:
                                            self.logger.error(f'第{done_line}行生成失败: {e}')
                                            gen_items = []
                                            stats['failed_generations'] += 1
                                        else:
                                            stats['successful_generations'] += 1
                                            if not unordered:
                                                pending_results[done_line] = gen_items or []
                                                # 有序推进
                                                while next_to_write in pending_results:
                                                    items = pending_results.pop(next_to_write)
                                                    for gi in items:
                                                        if self._evaluate_quality(gi, params):
                                                            out_f.write(json.dumps(gi, ensure_ascii=False) + '\n')
                                                            out_f.flush()
                                                            write_counter += 1
                                                            if write_counter % fsync_interval == 0:
                                                                try:
                                                                    os.fsync(out_f.fileno())
                                                                except Exception:
                                                                    pass
                                                            stats['quality_passed'] += 1
                                                        else:
                                                            stats['quality_failed'] += 1
                                                        stats['total_generated'] += 1
                                                    # 推进检查点
                                                    ckpt['line_index'] = next_to_write
                                                    ckpt['written_count'] = stats['quality_passed']
                                                    if (ckpt.get('total_lines', 0)):
                                                        progress = ckpt['line_index'] / max(ckpt['total_lines'], 1) * 100
                                                        state_manager.update_state(task_id, 'progress', progress)
                                                    if next_to_write % checkpoint_interval == 0:
                                                        self._save_checkpoint(task_dir, ckpt)
                                                    next_to_write += 1
                                            else:
                                                # 乱序：完成即写
                                                for gi in (gen_items or []):
                                                    if self._evaluate_quality(gi, params):
                                                        out_f.write(json.dumps(gi, ensure_ascii=False) + '\n')
                                                        out_f.flush()
                                                        write_counter += 1
                                                        if write_counter % fsync_interval == 0:
                                                            try:
                                                                os.fsync(out_f.fileno())
                                                            except Exception:
                                                                pass
                                                        stats['quality_passed'] += 1
                                                    else:
                                                        stats['quality_failed'] += 1
                                                    stats['total_generated'] += 1
                                                if checkpoint_interval > 0 and (write_counter % checkpoint_interval == 0):
                                                    ckpt['written_count'] = stats['quality_passed']
                                                    self._save_checkpoint(task_dir, ckpt)

                                    # 提交任务
                                    fut = executor.submit(
                                        self._generate_for_item,
                                        task_id, data_item, strategy, model_id, generation_count, params
                                    )
                                    inflight[fut] = line_no

                        # 读取结束后，收尾所有 in-flight
                        while inflight:
                            done_fut = next(as_completed(list(inflight.keys())))
                            done_line = inflight.pop(done_fut)
                            try:
                                gen_items = done_fut.result()
                            except Exception as e:
                                self.logger.error(f'第{done_line}行生成失败: {e}')
                                gen_items = []
                                stats['failed_generations'] += 1
                            else:
                                stats['successful_generations'] += 1
                                if not unordered:
                                    pending_results[done_line] = gen_items or []
                                    while next_to_write in pending_results:
                                        items = pending_results.pop(next_to_write)
                                        for gi in items:
                                            if self._evaluate_quality(gi, params):
                                                out_f.write(json.dumps(gi, ensure_ascii=False) + '\n')
                                                out_f.flush()
                                                write_counter += 1
                                                if write_counter % fsync_interval == 0:
                                                    try:
                                                        os.fsync(out_f.fileno())
                                                    except Exception:
                                                        pass
                                                stats['quality_passed'] += 1
                                            else:
                                                stats['quality_failed'] += 1
                                            stats['total_generated'] += 1
                                        ckpt['line_index'] = next_to_write
                                        ckpt['written_count'] = stats['quality_passed']
                                        if (ckpt.get('total_lines', 0)):
                                            progress = ckpt['line_index'] / max(ckpt['total_lines'], 1) * 100
                                            state_manager.update_state(task_id, 'progress', progress)
                                        if next_to_write % checkpoint_interval == 0:
                                            self._save_checkpoint(task_dir, ckpt)
                                        next_to_write += 1
                                else:
                                    for gi in (gen_items or []):
                                        if self._evaluate_quality(gi, params):
                                            out_f.write(json.dumps(gi, ensure_ascii=False) + '\n')
                                            out_f.flush()
                                            write_counter += 1
                                            if write_counter % fsync_interval == 0:
                                                try:
                                                    os.fsync(out_f.fileno())
                                                except Exception:
                                                    pass
                                            stats['quality_passed'] += 1
                                        else:
                                            stats['quality_failed'] += 1
                                    if checkpoint_interval > 0 and (write_counter % checkpoint_interval == 0):
                                        ckpt['written_count'] = stats['quality_passed']
                                        self._save_checkpoint(task_dir, ckpt)
                    finally:
                        executor.shutdown(wait=True, cancel_futures=False)

                # 检查暂停与完成
                state = state_manager.get_task_state(task_id) or {}
                if state.get('status') == 'paused':
                    self._save_checkpoint(task_dir, ckpt)
                    try:
                        self._generate_quality_report(task_id, params, stats, is_partial=True)
                    except Exception:
                        pass
                    self.logger.info(f'任务已暂停，阶段性保存完成: {task_id}')
                    return

            # 正常完成
            ckpt['status'] = 'completed'
            self._save_checkpoint(task_dir, ckpt)

            # 质量报告与元数据
            try:
                self._generate_quality_report(task_id, params, stats, is_partial=False)
            except Exception:
                pass
            self._generate_metadata(task_id, params, stats, str(output_path))
            state_manager.update_state(task_id, 'status', 'completed')
            state_manager.update_state(task_id, 'end_time', datetime.now().isoformat())
            self.logger.info(f'数据生成任务完成(流式): {task_id}')

        except Exception as e:
            self.logger.error(f'流式生成失败: {e}')
            state_manager.update_state(task_id, 'status', 'failed')
            state_manager.update_state(task_id, 'error_msg', str(e))

    def resume_generation(self, task_id: str, params_override: Optional[Dict[str, Any]] = None) -> str:
        """恢复已存在的任务（可覆盖模型与参数）"""
        try:
            task_dir = self.output_dir / task_id
            if not task_dir.exists():
                raise FileNotFoundError(f'任务目录不存在: {task_dir}')

            ckpt = self._load_checkpoint(task_dir)
            if not ckpt:
                raise RuntimeError('找不到可用的checkpoint，无法恢复')

            # 合并覆盖参数
            base_params = ckpt.get('params', {}).copy()
            if params_override:
                base_params.update({k: v for k, v in params_override.items() if v is not None})

            # 是否另起新任务目录恢复
            resume_as_new = bool(base_params.pop('resume_as_new', False) or base_params.pop('new_task', False))

            # 确保必要字段
            for key in ['strategy', 'model_id']:
                if key not in base_params:
                    raise ValueError(f'checkpoint 缺少必要参数: {key}')

            if not resume_as_new:
                # 更新/登记任务（原任务继续）
                # 检查任务是否已在运行，避免重复启动
                current_state = state_manager.get_task_state(task_id)
                if current_state and current_state.get('status') == 'running':
                    self.logger.warning(f'任务 {task_id} 已在运行中，忽略恢复请求')
                    return task_id

                # 关键修复：不要调用 add_task，否则会重置进度为0。直接更新状态即可。
                if not current_state:
                    # 只有当状态管理器中完全没有该任务时（例如重启后），才重新add
                    state_manager.add_task(TaskType.DISTILL, base_params.get('strategy'), base_params, task_id=task_id)
                else:
                    # 仅更新参数
                    current_state['params'] = base_params
                
                state_manager.update_state(task_id, 'status', 'running')
                # 不重置 start_time，保留原始开始时间
                
                threading.Thread(
                    target=self._execute_generation,
                    args=(task_id, base_params),
                    daemon=True
                ).start()

                self.logger.info(f'任务恢复启动: {task_id}')
                return task_id
            else:
                # 创建新任务ID与目录
                new_task_id = f"distill_{int(time.time())}"
                new_task_dir = self.output_dir / new_task_id
                new_task_dir.mkdir(parents=True, exist_ok=True)
                # 复制并更新checkpoint（不复制已生成文件，新任务只继续后续）
                new_ckpt = dict(ckpt)
                new_ckpt['task_id'] = new_task_id
                new_ckpt['status'] = 'running'
                self._save_checkpoint(new_task_dir, new_ckpt)

                # 登记新任务
                state_manager.add_task(TaskType.DISTILL, base_params.get('strategy'), base_params, task_id=new_task_id)
                state_manager.update_state(new_task_id, 'status', 'running')
                state_manager.update_state(new_task_id, 'start_time', datetime.now().isoformat())

                threading.Thread(
                    target=self._execute_generation,
                    args=(new_task_id, base_params),
                    daemon=True
                ).start()

                self.logger.info(f'任务克隆并恢复启动: {new_task_id} (from {task_id})')
                return new_task_id
        except Exception as e:
            self.logger.error(f'任务恢复失败: {e}')
            raise
    def _prepare_input_data(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """准备输入数据"""
        if 'input_data' in params:
            input_data = params['input_data']
            if isinstance(input_data, dict):
                return [input_data]
            elif isinstance(input_data, list):
                return input_data
            else:
                raise ValueError('input_data必须是字典或字典列表')
        
        elif 'input_file' in params:
            input_file = Path(params['input_file'])
            file_format = input_file.suffix.lower()[1:]
            
            if file_format == 'jsonl':
                data = []
                with open(input_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            data.append(json.loads(line.strip()))
                return data
            
            elif file_format == 'json':
                with open(input_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return [data]
                    elif isinstance(data, list):
                        return data
                    else:
                        raise ValueError('JSON文件格式错误')
            
            else:
                raise ValueError(f'不支持的输入文件格式: {file_format}')
        
        else:
            raise ValueError('需要指定输入数据')
    
    def _generate_multiple(self, task_id: str, input_data: List[Dict[str, Any]], 
                          strategy: str, model_id: str, generation_count: int, output_file,
                          stats: Dict[str, int], params: Dict[str, Any], processed_signatures: set = None) -> None:
        """生成多个变体（优化版：分组写入 + 流量控制）"""
        total_tasks = len(input_data)
        completed_tasks = 0
        fsync_interval = int(params.get('fsync_interval', 50))
        write_counter = 0
        local_max_workers = int(params.get('max_workers', self.max_workers))
        processed_signatures = processed_signatures or set()
        
        # 初始化进度：已处理的签名数量
        # 注意：这里简单用签名数量近似已完成任务数，用于恢复进度条
        completed_tasks = len(processed_signatures)
        
        # 流量控制：限制最大在途任务数，防止内存爆炸
        inflight_multiplier = int(params.get('inflight_multiplier', 4))
        max_inflight = local_max_workers * inflight_multiplier
        
        # 使用线程池并发生成
        with ThreadPoolExecutor(max_workers=local_max_workers) as executor:
            active_futures = {} # {future: (index, data_item)}
            input_iter = enumerate(input_data)
            
            while True:
                # 1. 提交新任务（直到达到最大在途数或无更多数据）
                while len(active_futures) < max_inflight:
                    try:
                        i, data_item = next(input_iter)
                    except StopIteration:
                        break
                        
                    # 检查是否已处理
                    sig = None
                    if 'id' in data_item:
                        sig = str(data_item['id'])
                    else:
                        q_text = self._extract_question_text(data_item, params)
                        if q_text:
                            sig = hashlib.md5(str(q_text).encode('utf-8')).hexdigest()
                    
                    if sig and sig in processed_signatures:
                        completed_tasks += 1
                        continue

                    # 检查是否已暂停
                    state = state_manager.get_task_state(task_id) or {}
                    if state.get('status') == 'paused':
                        self.logger.info(f'检测到暂停，停止提交新任务: {task_id}')
                        break
                        
                    future = executor.submit(
                        self._generate_for_item,
                        task_id, data_item, strategy, model_id, generation_count, params
                    )
                    active_futures[future] = (i, data_item)
                
                # 如果没有活动任务且输入已耗尽，则退出循环
                if not active_futures:
                    break
                
                # 2. 等待至少一个任务完成
                # 注意：as_completed 返回的是迭代器，我们这里只取第一个完成的
                done_futures = []
                try:
                    # 设置超时，避免无限等待，并提供状态反馈
                    for f in as_completed(active_futures, timeout=10):
                        done_futures.append(f)
                        break # 获取到一个就处理，以便腾出槽位提交新任务
                except TimeoutError:
                    self.logger.info(f"等待任务完成超时 (10s)，当前并发: {len(active_futures)}，可能模型响应较慢或正在重试")
                    continue
                
                # 3. 处理完成的任务
                for future in done_futures:
                    item_index, original_data = active_futures.pop(future)
                    try:
                        generated_items = future.result()
                        
                        # 分组写入：将同一输入的所有生成结果一次性写入
                        lines_to_write = []
                        for generated_item in generated_items:
                            if self._evaluate_quality(generated_item, params):
                                lines_to_write.append(json.dumps(generated_item, ensure_ascii=False))
                                stats['quality_passed'] += 1
                            else:
                                stats['quality_failed'] += 1
                            stats['total_generated'] += 1
                        
                        if lines_to_write:
                            # 原子性写入整组数据
                            output_file.write('\n'.join(lines_to_write) + '\n')
                            output_file.flush()
                            
                            write_counter += 1
                            if write_counter % fsync_interval == 0:
                                try:
                                    os.fsync(output_file.fileno())
                                except Exception:
                                    pass
                        
                        stats['successful_generations'] += 1
                        
                    except Exception as e:
                        self.logger.error(f'生成任务失败: {e}')
                        stats['failed_generations'] += 1
                    
                    finally:
                        completed_tasks += 1
                        progress = completed_tasks / total_tasks * 100
                        state_manager.update_state(task_id, 'progress', progress)
                
                # 再次检查暂停状态以退出外层循环
                state = state_manager.get_task_state(task_id) or {}
                if state.get('status') == 'paused':
                    # 等待所有在途任务完成（可选，或者直接放弃）
                    # 这里选择不再提交新任务，但允许在途任务完成
                    if not active_futures:
                        break
    
    def _generate_single(self, task_id: str, input_data: List[Dict[str, Any]], 
                        strategy: str, model_id: str, output_file,
                        stats: Dict[str, int], params: Dict[str, Any], processed_signatures: set = None) -> None:
        """生成单个结果（优化版：流量控制）"""
        total_tasks = len(input_data)
        completed_tasks = 0
        fsync_interval = int(params.get('fsync_interval', 50))
        write_counter = 0
        local_max_workers = int(params.get('max_workers', self.max_workers))
        processed_signatures = processed_signatures or set()
        
        # 初始化进度：已完成的任务数 = 已处理的签名数
        # 注意：这里不再重置 completed_tasks 为 0，而是继承已有的进度
        completed_tasks = len(processed_signatures)
        
        # 流量控制
        inflight_multiplier = int(params.get('inflight_multiplier', 4))
        max_inflight = local_max_workers * inflight_multiplier
        
        # 使用线程池并发生成
        with ThreadPoolExecutor(max_workers=local_max_workers) as executor:
            active_futures = {}
            input_iter = enumerate(input_data)
            
            while True:
                # 1. 提交新任务
                while len(active_futures) < max_inflight:
                    try:
                        i, data_item = next(input_iter)
                    except StopIteration:
                        break
                        
                    # 检查是否已处理
                    sig = None
                    if 'id' in data_item:
                        sig = str(data_item['id'])
                    else:
                        q_text = self._extract_question_text(data_item, params)
                        if q_text:
                            sig = hashlib.md5(str(q_text).encode('utf-8')).hexdigest()
                    
                    if sig and sig in processed_signatures:
                        # 如果已处理，跳过，但不要重复增加 completed_tasks（因为初始化时已经算过了）
                        # 除非我们想在日志里看到跳过的过程，否则直接 continue
                        continue

                    # 检查是否已暂停
                    state = state_manager.get_task_state(task_id) or {}
                    if state.get('status') == 'paused':
                        self.logger.info(f'检测到暂停，停止提交新任务: {task_id}')
                        break
                        
                    future = executor.submit(
                        self._generate_for_item,
                        task_id, data_item, strategy, model_id, 1, params
                    )
                    active_futures[future] = (i, data_item)
                
                if not active_futures:
                    break
                
                # 2. 等待至少一个任务完成
                done_futures = []
                try:
                    # 设置超时，避免无限等待，并提供状态反馈
                    for f in as_completed(active_futures, timeout=10):
                        done_futures.append(f)
                        break
                except TimeoutError:
                    self.logger.info(f"等待任务完成超时 (10s)，当前并发: {len(active_futures)}，可能模型响应较慢或正在重试")
                    continue
                
                # 3. 处理完成的任务
                for future in done_futures:
                    item_index, original_data = active_futures.pop(future)
                    try:
                        generated_items = future.result()
                        
                        # 写入生成的数据
                        lines_to_write = []
                        for generated_item in generated_items:
                            if self._evaluate_quality(generated_item, params):
                                lines_to_write.append(json.dumps(generated_item, ensure_ascii=False))
                                stats['quality_passed'] += 1
                            else:
                                stats['quality_failed'] += 1
                            stats['total_generated'] += 1
                        
                        if lines_to_write:
                            output_file.write('\n'.join(lines_to_write) + '\n')
                            output_file.flush()
                            write_counter += 1
                            if write_counter % fsync_interval == 0:
                                try:
                                    os.fsync(output_file.fileno())
                                except Exception:
                                    pass
                        
                        stats['successful_generations'] += 1
                        
                    except Exception as e:
                        self.logger.error(f'生成任务失败: {e}')
                        stats['failed_generations'] += 1
                    
                    finally:
                        completed_tasks += 1
                        progress = completed_tasks / total_tasks * 100
                        state_manager.update_state(task_id, 'progress', progress)
                
                # 再次检查暂停
                state = state_manager.get_task_state(task_id) or {}
                if state.get('status') == 'paused':
                    if not active_futures:
                        break
    
    def _extract_label_from_boxed(self, text: str) -> Optional[str]:
        """从 \\boxed{...} 中提取标签"""
        import re
        match = re.search(r'\\boxed\{(.*?)\}', text)
        if match:
            return match.group(1).strip()
        return None

    def _generate_for_item(self, task_id: str, data_item: Dict[str, Any], strategy: str, 
                          model_id: str, count: int, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """为单个数据项生成内容"""
        # 若任务被暂停，尽早返回空
        state = state_manager.get_task_state(task_id) or {}
        if state.get('status') == 'paused':
            return []

        # 特殊处理 Q_TO_A 的多生成需求：循环调用以保证生成数量
        if strategy == GenerationStrategy.Q_TO_A and count > 1:
            all_results = []
            for i in range(count):
                # 检查暂停
                state = state_manager.get_task_state(task_id) or {}
                if state.get('status') == 'paused':
                    break
                
                # 构建提示 (count=1 因为每次只生成一个)
                prompt = self._build_prompt(data_item, strategy, 1, params)
                
                # 单次生成的重试逻辑
                for attempt in range(self.max_retries):
                    # 每次尝试前检查暂停
                    state = state_manager.get_task_state(task_id) or {}
                    if state.get('status') == 'paused':
                        return all_results
                    
                    # 全局速率限制
                    self._acquire_rate_limit((params or {}).get('rate_limit_rps'))
                    
                    try:
                        # 为了增加多样性，可以微调 temperature (可选，这里暂不处理)
                        response = self._call_model(model_id, prompt, params)
                        generated_items = self._parse_response(response, strategy, data_item, params.get('target_field'), params)
                        
                        if generated_items:
                            all_results.extend(generated_items)
                            break # 成功则跳出重试循环
                        else:
                            self.logger.warning(f'模型返回空结果，重试第{attempt + 1}次')
                            
                    except Exception as e:
                        self.logger.error(f'模型调用失败（尝试{attempt + 1}）: {e}')
                        
                        # 检测是否为速率限制 (429)
                        is_rate_limit = "HTTP 429" in str(e)

                        if attempt == self.max_retries - 1:
                            pass # 最后一次失败也不抛出，继续下一个 count
                        
                        # 指数退避
                        try:
                            if is_rate_limit:
                                # 针对限流进行更激进的退避 (5s, 10s, 20s...)
                                backoff = min(5.0 * (2 ** attempt), 60.0)
                            else:
                                backoff = min(1.0 * (2 ** attempt), float((params or {}).get('max_backoff', 8.0)))
                        except Exception:
                            backoff = min(1.0 * (2 ** attempt), 8.0)
                        jitter = random.random() * 0.3
                        time.sleep(backoff + jitter)
            
            return all_results

        # 其他策略或 count=1 的情况
        prompt = self._build_prompt(data_item, strategy, count, params)
        
        # 调用模型生成
        for attempt in range(self.max_retries):
            # 每次尝试前检查暂停
            state = state_manager.get_task_state(task_id) or {}
            if state.get('status') == 'paused':
                return []
            # 全局速率限制（每秒请求数）
            self._acquire_rate_limit((params or {}).get('rate_limit_rps'))
            try:
                response = self._call_model(model_id, prompt, params)
                
                # 特殊处理 CLASSIFY_LABEL 的验证逻辑
                if strategy == GenerationStrategy.CLASSIFY_LABEL:
                    label = self._extract_label_from_boxed(response)
                    valid_labels = params.get('label_set', [])
                    
                    # 如果提取失败或不在标签集中，视为失败
                    if not label:
                        self.logger.warning(f'未能从响应中提取到boxed标签，重试第{attempt + 1}次。响应片段: {response[:50]}...')
                        if attempt == self.max_retries - 1:
                            # 最后一次尝试失败，使用特殊标记
                            if params.get('selected_fields'):
                                fallback_item = {k: v for k, v in data_item.items() if k in params['selected_fields']}
                            else:
                                fallback_item = dict(data_item)
                            fallback_item[params.get('target_field', 'label')] = "##**分类失败**##"
                            return [fallback_item]
                        continue # 触发重试
                        
                    if valid_labels and label not in valid_labels:
                        self.logger.warning(f'提取的标签 "{label}" 不在预设集合中，重试第{attempt + 1}次')
                        if attempt == self.max_retries - 1:
                            # 最后一次尝试失败，使用特殊标记
                            if params.get('selected_fields'):
                                fallback_item = {k: v for k, v in data_item.items() if k in params['selected_fields']}
                            else:
                                fallback_item = dict(data_item)
                            fallback_item[params.get('target_field', 'label')] = "##**分类失败**##"
                            return [fallback_item]
                        continue # 触发重试
                    
                    # 验证通过，构造结果
                    # 仅保留 selected_fields 指定的字段 + 标签字段
                    if params.get('selected_fields'):
                        item = {k: v for k, v in data_item.items() if k in params['selected_fields']}
                    else:
                        item = dict(data_item)
                    
                    item[params.get('target_field', 'label')] = label
                    return [item]

                generated_items = self._parse_response(response, strategy, data_item, params.get('target_field'), params)
                
                if generated_items:
                    return generated_items
                else:
                    self.logger.warning(f'模型返回空结果，重试第{attempt + 1}次')
                    
            except Exception as e:
                self.logger.error(f'模型调用失败（尝试{attempt + 1}）: {e}')
                
                # 检测是否为速率限制 (429)
                is_rate_limit = "HTTP 429" in str(e)

                if attempt == self.max_retries - 1:
                    # 如果是分类任务，最后一次异常也返回失败标记
                    if strategy == GenerationStrategy.CLASSIFY_LABEL:
                        if params.get('selected_fields'):
                            fallback_item = {k: v for k, v in data_item.items() if k in params['selected_fields']}
                        else:
                            fallback_item = dict(data_item)
                        fallback_item[params.get('target_field', 'label')] = "##**分类失败**##"
                        return [fallback_item]
                    raise
                # 指数退避 + 抖动，避免持续撞限流
                try:
                    if is_rate_limit:
                        # 针对限流进行更激进的退避 (5s, 10s, 20s...)
                        backoff = min(5.0 * (2 ** attempt), 60.0)
                    else:
                        backoff = min(1.0 * (2 ** attempt), float((params or {}).get('max_backoff', 8.0)))
                except Exception:
                    backoff = min(1.0 * (2 ** attempt), 8.0)
                jitter = random.random() * 0.3
                time.sleep(backoff + jitter)
        
        return []
    
    def _build_prompt(self, data_item: Dict[str, Any], strategy: str, 
                     count: int, params: Dict[str, Any]) -> str:
        """构建提示（合并 system/q/a 三类提示到单条提示文本）"""
        template = self.prompt_templates.get(strategy, '')
        
        # 优化提示词拼接方案：
        # 1. 将前端传递的 system_prompt 显式拼接到 User Prompt 的开头，作为核心指令。
        #    这能确保即使模型对 System Role 支持不佳，也能明确接收到指令。
        # 2. 同时保留 _call_model 中对 System Role 的传递（双重保障）。
        
        sys_prefix = ''
        user_sys_prompt = params.get('system_prompt', '').strip()
        if user_sys_prompt:
            sys_prefix = f"核心指令：\n{user_sys_prompt}\n\n"
        
        field_hint = ''
        if params.get('selected_fields'):
            field_hint = f"仅对以下字段进行生成/改写：{', '.join(params['selected_fields'])}。"
        label_hint = ''
        if strategy == GenerationStrategy.CLASSIFY_LABEL and params.get('label_set'):
            label_hint = f"（可选标签集合：{', '.join(params['label_set'])}）"

        if strategy == GenerationStrategy.EXPAND:
            source_val = None
            if params.get('source_field'):
                sf = params['source_field']
                if sf in data_item:
                    source_val = data_item[sf]
            
            if source_val:
                sample_text = str(source_val)
                hint = f"基于字段 '{params['source_field']}' 的内容进行扩写。请生成包含字段 '{params['source_field']}' 的JSON对象列表。"
            else:
                sample_text = json.dumps(data_item, ensure_ascii=False, indent=2)
                hint = field_hint or '（未指定字段约束）'

            return template.format(
                sys_prefix=sys_prefix,
                count=count,
                sample=sample_text,
                field_hint=hint
            )

        elif strategy == GenerationStrategy.ENHANCE:
            return template.format(
                sys_prefix=sys_prefix,
                data=json.dumps(data_item, ensure_ascii=False, indent=2)
            )

        elif strategy == GenerationStrategy.PARAPHRASE:
            # 若指定字段，则取第一个字段文本，否则回退 text 或整条文本
            text = ''
            if params.get('selected_fields'):
                for f in params['selected_fields']:
                    if f in data_item and isinstance(data_item[f], (str, int, float)):
                        text = str(data_item[f])
                        break
            if not text:
                text = data_item.get('text', '') or str(data_item)
            return template.format(sys_prefix=sys_prefix, count=count, text=text)

        elif strategy == GenerationStrategy.CLASSIFY_LABEL:
            # 过滤数据：仅保留 selected_fields 指定的字段
            filtered_data = data_item
            if params.get('selected_fields'):
                filtered_data = {k: v for k, v in data_item.items() if k in params['selected_fields']}
            
            return template.format(
                sys_prefix=sys_prefix,
                data=json.dumps(filtered_data, ensure_ascii=False, indent=2),
                label_hint=label_hint
            )

        elif strategy == GenerationStrategy.Q_TO_A:
            # 使用统一的提取逻辑，确保与 _map_generated_item 一致
            q_text = self._extract_question_text(data_item, params)
            
            q_prompt = (params.get('q_prompt') or '').strip()
            a_prompt = (params.get('a_prompt') or '').strip()
            composed = template.format(sys_prefix=sys_prefix, count=count, question=q_text)
            if q_prompt:
                composed = f"{sys_prefix}[Q提示]\n{q_prompt}\n\n" + composed
            if a_prompt:
                composed = composed + f"\n\n[答案风格提示]\n{a_prompt}"
            return composed

        elif strategy == GenerationStrategy.CUSTOM:
            custom_parts = []
            # CUSTOM 策略下，system prompt 仍可能需要显示在 body 中，视用户需求而定
            # 但为了统一，这里也移除 sys_prefix 的自动拼接，完全依赖 params 传递
            if params.get('q_prompt'):
                custom_parts.append(f"[Q]\n{params['q_prompt']}")
            if params.get('a_prompt'):
                custom_parts.append(f"[A]\n{params['a_prompt']}")
            custom_body = "\n\n".join(custom_parts) or json.dumps(data_item, ensure_ascii=False, indent=2)
            return template.format(sys_prefix='', custom_body=custom_body)

        else:
            # 兜底
            return f"请处理以下数据：\n{json.dumps(data_item, ensure_ascii=False, indent=2)}"
    
    def _call_model(self, model_id: str, prompt: str, params: Dict[str, Any]) -> str:
        """调用模型生成"""
        generation_params = {
            'max_tokens': params.get('max_tokens', 2048),
            'temperature': params.get('temperature', 0.7),
            'top_p': params.get('top_p', 0.9),
            'top_k': params.get('top_k', None),
            'system_prompt': params.get('system_prompt'), # 传递系统提示词
            'timeout': params.get('timeout', 120) # 默认 120秒超时，防止无限等待
        }
        
        # 调用模型管理器
        response = model_manager.generate_text(model_id, prompt, generation_params)
        
        if response.get('success'):
            return response['content']
        else:
            raise Exception(f"模型生成失败: {response.get('error', 'Unknown error')}")
    
    def _parse_response(self, response: str, strategy: str, 
                       original_data: Dict[str, Any], target_field: Optional[str] = None,
                       params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """解析模型响应并统一映射到目标字段"""
        try:
            # 对于 Q_TO_A 策略，直接走文本处理，保留模型原始输出（不做任何清理/替换）
            if strategy == GenerationStrategy.Q_TO_A:
                return self._handle_text_response(response, strategy, original_data, target_field, params)
            # 尝试解析JSON
            if response.strip().startswith('['):
                # 响应是JSON数组
                parsed_data = json.loads(response.strip())
                if isinstance(parsed_data, list):
                    return [self._map_generated_item(item, original_data, strategy, target_field, params) for item in parsed_data]
            elif response.strip().startswith('{'):
                # 响应是单个JSON对象
                parsed_data = json.loads(response.strip())
                return [self._map_generated_item(parsed_data, original_data, strategy, target_field, params)]
            else:
                # 响应是文本，尝试提取JSON
                json_match = self._extract_json_from_text(response)
                if json_match:
                    return [self._map_generated_item(item, original_data, strategy, target_field, params) for item in json_match]
                # 没有可解析的JSON，则按纯文本处理（保留 Q_TO_A 的原始响应在后续处理）
                return self._handle_text_response(response, strategy, original_data, target_field, params)
        except json.JSONDecodeError:
            # JSON解析失败，尝试其他方法
            return self._handle_text_response(response, strategy, original_data, target_field, params)
    
    def _extract_json_from_text(self, text: str) -> Optional[List[Dict[str, Any]]]:
        """从文本中提取JSON，优先提取列表"""
        import re
        
        # 优先查找JSON数组 [...]
        list_pattern = r'(\[.*\])'
        list_matches = re.findall(list_pattern, text, re.DOTALL)
        
        results = []
        for match in list_matches:
            try:
                parsed = json.loads(match)
                if isinstance(parsed, list):
                    results.extend(parsed)
            except json.JSONDecodeError:
                continue
        
        if results:
            return results

        # 其次查找JSON对象 {...}
        obj_pattern = r'(\{.*\})'
        obj_matches = re.findall(obj_pattern, text, re.DOTALL)
        
        for match in obj_matches:
            try:
                parsed = json.loads(match)
                if isinstance(parsed, dict):
                    results.append(parsed)
            except json.JSONDecodeError:
                continue
        
        return results if results else None

    # 注：按照当前需求，Q_TO_A 策略应原样保留模型输出文本，不做任何清洗或格式化处理。
    
    def _handle_text_response(self, response: str, strategy: str, 
                             original_data: Dict[str, Any], target_field: Optional[str],
                             params: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """处理文本响应并放入目标字段"""
        lines = [line.strip() for line in response.split('\n') if line.strip()]
        if not lines:
            return []
        results: List[Dict[str, Any]] = []
        tgt = target_field or 'output'
        q_out_name = (params or {}).get('q_field_name', 'instruction')

        # Helper: obtain the original field value to store in the `original` column
        def _get_original_value(orig: Dict[str, Any], p: Optional[Dict[str, Any]]) -> Any:
            if isinstance(p, dict) and p.get('source_field'):
                sf = p.get('source_field')
                if sf in orig:
                    return orig.get(sf)
            # fallback to selected_fields first item
            if isinstance(p, dict) and p.get('selected_fields'):
                sf = p.get('selected_fields')
                if isinstance(sf, list) and len(sf) > 0:
                    f = sf[0]
                    if f in orig:
                        return orig.get(f)
            # final fallback: try common question fields or full object
            for f in ['question', 'instruction', 'input', 'query', 'prompt']:
                if f in orig and orig.get(f) is not None:
                    return orig.get(f)
            return orig

        # EXPAND: always return two-field records {original, <tgt>} where
        # `original` is the source field value and `<tgt>` is the generated text/json.
        if strategy == GenerationStrategy.EXPAND:
            original_val = _get_original_value(original_data, params)
            # For text responses we join lines into one string
            generated_text = "\n".join(lines)
            return [{
                'original': original_val,
                tgt: generated_text
            }]

        if strategy == GenerationStrategy.PARAPHRASE:
            for line in lines:
                item = dict(original_data)
                item[tgt] = line
                results.append(item)
            return results
        elif strategy == GenerationStrategy.CLASSIFY_LABEL:
            item = dict(original_data)
            item[tgt] = lines[0]
            return [item]
        elif strategy == GenerationStrategy.Q_TO_A:
            # 仅输出问答两列：<q_out_name>: 问题文本, <tgt>: 模型原始输出（含思维链与答案）
            # 保留原始响应的换行与格式，不进行裁剪
            answer_text = response
            q_text = self._extract_question_text(original_data, params)
            return [{q_out_name: q_text, tgt: answer_text}]
        else:
            item = dict(original_data)
            item[tgt] = "\n".join(lines)
            return [item]

    def _map_generated_item(self, gen: Any, original_data: Dict[str, Any], strategy: str, target_field: Optional[str],
                            params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """将模型生成的对象映射到统一结构（目标字段）

        - 若 gen 为 dict 且策略为 EXPAND：优先与原始数据合并（gen 覆盖原字段）
        - 其他策略：将文本或对象序列化后写入 target_field
        """
        tgt = target_field or 'output'
        # For EXPAND we emit only two fields: 'original' and <target_field>
        if strategy == GenerationStrategy.EXPAND:
            # determine original value similar to text handler
            original_val = None
            if isinstance(params, dict) and params.get('source_field'):
                sf = params.get('source_field')
                if sf in original_data:
                    original_val = original_data.get(sf)
            if original_val is None and isinstance(params, dict) and params.get('selected_fields'):
                sf = params.get('selected_fields')
                if isinstance(sf, list) and len(sf) > 0:
                    f = sf[0]
                    if f in original_data:
                        original_val = original_data.get(f)
            if original_val is None:
                for f in ['question', 'instruction', 'input', 'query', 'prompt']:
                    if f in original_data and original_data.get(f) is not None:
                        original_val = original_data.get(f)
                        break
            if original_val is None:
                original_val = original_data

            # generated target: extract content if wrapped in single key, else serialize
            if isinstance(gen, dict):
                # Try to extract content if it's wrapped in a common key
                keys = list(gen.keys())
                if len(keys) == 1 and isinstance(gen[keys[0]], str):
                    tgt_val = gen[keys[0]]
                elif 'query' in gen:
                    tgt_val = gen['query']
                elif 'question' in gen:
                    tgt_val = gen['question']
                elif 'instruction' in gen:
                    tgt_val = gen['instruction']
                elif 'text' in gen:
                    tgt_val = gen['text']
                elif 'content' in gen:
                    tgt_val = gen['content']
                else:
                    tgt_val = json.dumps(gen, ensure_ascii=False)
            else:
                tgt_val = str(gen)
            return {'original': original_val, (target_field or 'output'): tgt_val}

        elif strategy == GenerationStrategy.Q_TO_A:
            # 仅保留问答两列
            q_out_name = (params or {}).get('q_field_name', 'instruction')
            q_text = self._extract_question_text(original_data, params)
            if isinstance(gen, dict):
                a_text = json.dumps(gen, ensure_ascii=False)
            else:
                a_text = str(gen)
            return {q_out_name: q_text, tgt: a_text}
        else:
            item = dict(original_data)
            if isinstance(gen, dict):
                item[tgt] = json.dumps(gen, ensure_ascii=False)
            else:
                item[tgt] = str(gen)
            return item

    def _extract_question_text(self, original_data: Dict[str, Any], params: Optional[Dict[str, Any]]) -> str:
        """按照优先级从原始数据中抽取问题文本。
        优先 selected_fields 指定的第一个存在字段；否则从常见字段中按顺序查找。
        若均无，则返回原始数据的字符串化形式。
        """
        # 首选用户指定字段
        sel_fields = None
        if isinstance(params, dict):
            sf = params.get('selected_fields')
            if isinstance(sf, list):
                sel_fields = sf
            elif isinstance(sf, str):
                sel_fields = [s.strip() for s in sf.split(',') if s.strip()]
        if sel_fields:
            for f in sel_fields:
                if f in original_data and original_data.get(f) is not None:
                    return str(original_data.get(f))
        # 常见问字段顺序 (增加首字母大写支持)
        common_fields = [
            'question', 'instruction', 'input', 'query', 'prompt',
            'Question', 'Instruction', 'Input', 'Query', 'Prompt'
        ]
        for f in common_fields:
            if f in original_data and original_data.get(f) is not None:
                return str(original_data.get(f))
        return str(original_data)
    
    def _evaluate_quality(self, generated_item: Dict[str, Any], params: Dict[str, Any]) -> bool:
        """评估生成质量"""
        # 基本质量检查
        if not generated_item or not isinstance(generated_item, dict):
            return False
        
        # 检查是否有实际内容
        # 排除 'original' 字段，检查其他字段是否为空或仅包含空白字符
        # 修复：增加对 NULL 字符 (\x00) 的检查，防止写入无效数据
        has_content = False
        for key, value in generated_item.items():
            if key == 'original':
                continue
            
            val_str = str(value)
            if not val_str.strip():
                continue
                
            # 检查是否包含大量 NULL 字符 (可能是模型输出异常或编码问题)
            if '\x00' in val_str:
                self.logger.warning(f"检测到包含 NULL 字符的生成结果，视为无效: {key}")
                return False
                
            has_content = True
            
        if not has_content:
            return False
        
        # 长度检查
        min_length = params.get('min_length', 10)
        # 放宽上限以容纳含 CoT 的长输出
        max_length = params.get('max_length', 200000)
        
        total_length = sum(
            len(str(value)) 
            for value in generated_item.values()
            if isinstance(value, (str, int, float))
        )
        
        if total_length < min_length or total_length > max_length:
            return False
        
        # 质量分数检查（如果有配置）
        quality_score = self._calculate_quality_score(generated_item, params)
        return quality_score >= self.quality_threshold
    
    def _calculate_quality_score(self, generated_item: Dict[str, Any], params: Dict[str, Any]) -> float:
        """计算质量分数"""
        # 简单的质量评估算法
        score = 1.0
        
        # 检查完整性
        if len(generated_item) < 2:
            score *= 0.8
        
        # 检查内容丰富度
        total_chars = sum(len(str(v)) for v in generated_item.values())
        if total_chars < 50:
            score *= 0.7
        elif total_chars > 1000:
            score *= 1.1  # 奖励丰富的内容
        
        # 检查是否有重复内容
        values = [str(v).lower() for v in generated_item.values()]
        unique_values = set(values)
        if len(unique_values) < len(values):
            score *= 0.9
        
        return min(score, 1.0)
    
    def _generate_quality_report(self, task_id: str, params: Dict[str, Any], 
                                stats: Dict[str, int], is_partial: bool = False) -> None:
        """生成质量报告"""
        try:
            task_dir = self.output_dir / task_id
            report_path = task_dir / 'quality_report.json'
            
            # 计算质量指标
            total_generated = stats['total_generated']
            quality_rate = (stats['quality_passed'] / total_generated * 100) if total_generated > 0 else 0
            success_rate = (stats['successful_generations'] / stats['total_input'] * 100) if stats['total_input'] > 0 else 0
            
            report = {
                'task_id': task_id,
                'generation_strategy': params['strategy'],
                'model_id': params['model_id'],
                'statistics': stats,
                'quality_metrics': {
                    'total_input_items': stats['total_input'],
                    'total_generated_items': stats['total_generated'],
                    'quality_passed_items': stats['quality_passed'],
                    'quality_failed_items': stats['quality_failed'],
                    'quality_pass_rate': quality_rate,
                    'generation_success_rate': success_rate,
                    'average_generations_per_input': total_generated / stats['total_input'] if stats['total_input'] > 0 else 0
                },
                'parameters': {k: v for k, v in params.items() if k not in ['input_data']},
                'generated_time': datetime.now().isoformat(),
                'is_partial': bool(is_partial)
            }
            
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f'质量报告已生成: {report_path}')
            
        except Exception as e:
            self.logger.error(f'生成质量报告失败: {e}')
    
    def _generate_metadata(self, task_id: str, params: Dict[str, Any], 
                          stats: Dict[str, int], output_file: str) -> None:
        """生成元数据文件"""
        try:
            task_dir = self.output_dir / task_id
            meta_path = task_dir / 'meta.json'
            
            metadata = {
                'task_id': task_id,
                'task_type': 'data_distill',
                'strategy': params['strategy'],
                'model_id': params['model_id'],
                'output_path': output_file,
                'params': {k: v for k, v in params.items() if k not in ['input_data']},
                'start_time': state_manager.get_task_state(task_id).get('start_time', ''),
                'end_time': state_manager.get_task_state(task_id).get('end_time', ''),
                'input_item_count': stats['total_input'],
                'output_item_count': stats['quality_passed'],
                'file_size': Path(output_file).stat().st_size if Path(output_file).exists() else 0,
                'generation_summary': {
                    'total_generated': stats['total_generated'],
                    'successful_generations': stats['successful_generations'],
                    'failed_generations': stats['failed_generations'],
                    'quality_passed': stats['quality_passed'],
                    'quality_failed': stats['quality_failed']
                }
            }
            
            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f'元数据已生成: {meta_path}')
            
        except Exception as e:
            self.logger.error(f'生成元数据失败: {e}')
    
    def get_generation_progress(self, task_id: str) -> Dict[str, Any]:
        """
        获取生成进度
        
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
                'start_time': task_state.get('start_time', ''),
                'error_msg': task_state.get('error_msg', ''),
                'params': task_state.get('params', {})
            }
            
            return progress_info
            
        except Exception as e:
            self.logger.error(f'获取生成进度失败: {e}')
            return {'error': str(e)}
    
    def get_quality_report(self, task_id: str) -> Dict[str, Any]:
        """
        获取质量报告
        
        Args:
            task_id (str): 任务ID
            
        Returns:
            dict: 质量报告
        """
        try:
            task_dir = self.output_dir / task_id
            report_path = task_dir / 'quality_report.json'
            
            if not report_path.exists():
                return {'error': '质量报告不存在'}
            
            with open(report_path, 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            return report
            
        except Exception as e:
            self.logger.error(f'获取质量报告失败: {e}')
            return {'error': str(e)}
    
    def list_generation_strategies(self) -> List[str]:
        """
        获取支持的生成策略列表
        
        Returns:
            list: 生成策略列表
        """
        return self.supported_strategies.copy()
    
    def get_strategy_description(self, strategy: str) -> Dict[str, Any]:
        """
        获取生成策略描述
        
        Args:
            strategy (str): 生成策略
            
        Returns:
            dict: 策略描述
        """
        descriptions = {
            GenerationStrategy.EXPAND: {
                'name': '数据扩充',
                'description': '基于现有数据生成类似但不重复的新数据（可限制字段，默认写入 target_field=output）',
                'required_params': ['generation_count'],
                'optional_params': ['selected_fields', 'target_field', 'system_prompt', 'max_tokens', 'temperature', 'top_p', 'top_k']
            },
            GenerationStrategy.ENHANCE: {
                'name': '内容增强',
                'description': '对选定字段进行内容扩写或补充（结果写入 target_field）',
                'required_params': [],
                'optional_params': ['selected_fields', 'target_field', 'system_prompt', 'max_tokens', 'temperature', 'top_p', 'top_k']
            },
            GenerationStrategy.PARAPHRASE: {
                'name': '文本改写',
                'description': '对选定字段进行风格化改写，保持语义不变（结果写入 target_field）',
                'required_params': ['generation_count'],
                'optional_params': ['selected_fields', 'target_field', 'system_prompt', 'max_tokens', 'temperature', 'top_p', 'top_k']
            },
            GenerationStrategy.CLASSIFY_LABEL: {
                'name': '分类标注',
                'description': '根据选定字段打语义标签，可指定标签集合（label_set），结果写入 target_field',
                'required_params': [],
                'optional_params': ['selected_fields', 'target_field', 'label_set', 'system_prompt', 'max_tokens', 'temperature', 'top_p', 'top_k']
            },
            GenerationStrategy.Q_TO_A: {
                'name': '从Q生A',
                'description': '从选定问题字段生成答案（支持 q_prompt/a_prompt 引导），结果写入 target_field',
                'required_params': ['generation_count'],
                'optional_params': ['selected_fields', 'q_field_name', 'target_field', 'system_prompt', 'q_prompt', 'a_prompt', 'max_tokens', 'temperature', 'top_p', 'top_k']
            },
            GenerationStrategy.CUSTOM: {
                'name': '自定义',
                'description': '自定义 system/q/a 提示词与字段选择，完全按需加工，结果写入 target_field',
                'required_params': [],
                'optional_params': ['selected_fields', 'target_field', 'system_prompt', 'q_prompt', 'a_prompt', 'max_tokens', 'temperature', 'top_p', 'top_k']
            }
        }
        
        return descriptions.get(strategy, {'error': '未知策略'})

    def scan_local_tasks(self) -> int:
        """
        扫描本地蒸馏任务目录，恢复丢失的任务状态
        
        Returns:
            int: 恢复的任务数量
        """
        restored_count = 0
        try:
            if not self.output_dir.exists():
                return 0
                
            # 遍历蒸馏目录下的所有子目录
            for task_dir in self.output_dir.iterdir():
                if not task_dir.is_dir():
                    continue
                    
                task_id = task_dir.name
                
                # 检查任务是否已存在于状态管理器
                if state_manager.get_task_state(task_id):
                    continue
                
                # 尝试从 checkpoint.json 恢复（运行中/暂停/失败的任务）
                ckpt_path = task_dir / 'checkpoint.json'
                if ckpt_path.exists():
                    try:
                        with open(ckpt_path, 'r', encoding='utf-8') as f:
                            ckpt = json.load(f)
                        
                        # 恢复任务状态
                        params = ckpt.get('params', {})
                        status = ckpt.get('status', 'unknown')
                        
                        # 如果状态是 running，但进程已重启，应重置为 paused
                        if status == 'running':
                            status = 'paused'
                            
                        state_manager.add_task(
                            TaskType.DISTILL,
                            params.get('strategy', 'data_distill'),
                            params,
                            task_id=task_id
                        )
                        
                        # 更新详细状态
                        state_manager.update_state(task_id, 'status', status)
                        state_manager.update_state(task_id, 'progress', ckpt.get('progress', 0))
                        state_manager.update_state(task_id, 'start_time', ckpt.get('started_at', ''))
                        
                        if 'written_count' in ckpt:
                            state_manager.update_state(task_id, 'statistics.processed_items', ckpt['written_count'])
                            
                        restored_count += 1
                        continue
                    except Exception as e:
                        self.logger.warning(f'从checkpoint恢复任务 {task_id} 失败: {e}')
                
                # 尝试从 meta.json 恢复（已完成的任务）
                meta_path = task_dir / 'meta.json'
                if meta_path.exists():
                    try:
                        with open(meta_path, 'r', encoding='utf-8') as f:
                            meta = json.load(f)
                            
                        params = meta.get('params', {})
                        
                        state_manager.add_task(
                            TaskType.DISTILL,
                            params.get('strategy', 'data_distill'),
                            params,
                            task_id=task_id
                        )
                        
                        state_manager.update_state(task_id, 'status', 'completed')
                        state_manager.update_state(task_id, 'start_time', meta.get('start_time', ''))
                        state_manager.update_state(task_id, 'end_time', meta.get('end_time', ''))
                        state_manager.update_state(task_id, 'progress', 100)
                        
                        stats = meta.get('generation_summary', {})
                        if stats:
                            state_manager.update_state(task_id, 'statistics.total_items', meta.get('input_item_count', 0))
                            state_manager.update_state(task_id, 'statistics.processed_items', stats.get('quality_passed', 0))
                            
                        restored_count += 1
                    except Exception as e:
                        self.logger.warning(f'从meta恢复任务 {task_id} 失败: {e}')
            
            if restored_count > 0:
                self.logger.info(f'已从本地恢复 {restored_count} 个蒸馏任务')
                # 立即保存状态
                state_manager.save_state()
                
            return restored_count
            
        except Exception as e:
            self.logger.error(f'扫描本地任务失败: {e}')
            return 0


# 全局蒸馏生成器实例
distill_generator = DistillGenerator()


if __name__ == "__main__":
    """
    命令行入口，用于数据蒸馏生成操作
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='数据蒸馏生成工具')
    subparsers = parser.add_subparsers(dest='action', help='可用操作')
    
    # generate命令
    generate_parser = subparsers.add_parser('generate', help='执行数据生成')
    generate_parser.add_argument('--strategy', required=True,
                                choices=['expand', 'enhance', 'paraphrase', 'classify_label', 'q_to_a', 'custom'],
                                help='生成策略')
    generate_parser.add_argument('--model-id', required=True, help='模型ID')
    generate_parser.add_argument('--input-file', help='输入文件路径')
    generate_parser.add_argument('--input-data', help='输入数据（JSON格式）')
    generate_parser.add_argument('--generation-count', type=int, default=5, help='生成数量')
    generate_parser.add_argument('--max-tokens', type=int, default=2048, help='最大token数')
    generate_parser.add_argument('--temperature', type=float, default=0.7, help='温度参数')
    generate_parser.add_argument('--top-p', type=float, default=0.9, help='top_p')
    generate_parser.add_argument('--top-k', type=int, help='top_k')
    generate_parser.add_argument('--selected-fields', help='逗号分隔的字段列表')
    generate_parser.add_argument('--target-field', default='output', help='生成写入的目标字段，默认 output')
    generate_parser.add_argument('--label-set', help='用于分类标注的标签集合，逗号分隔')
    generate_parser.add_argument('--system-prompt', help='系统提示词')
    generate_parser.add_argument('--q-prompt', help='Q 提示词')
    generate_parser.add_argument('--a-prompt', help='A 提示词')
    generate_parser.add_argument('--q-field-name', default='instruction', help='输出中问题字段名，默认 instruction')
    # 并发与性能
    generate_parser.add_argument('--max-workers', type=int, default=8, help='并发度（同时请求数）')
    generate_parser.add_argument('--fsync-interval', type=int, default=50, help='写盘同步间隔（行）')
    generate_parser.add_argument('--checkpoint-interval', type=int, default=100, help='检查点保存间隔（行）')
    generate_parser.add_argument('--inflight-multiplier', type=int, default=4, help='在途任务倍数（流式JSONL）')
    generate_parser.add_argument('--unordered-write', action='store_true', help='无序写入（完成即写，牺牲严格保序）')
    # 限流与退避
    generate_parser.add_argument('--rate-limit-rps', type=float, help='每秒最大请求数（0或缺省表示不限速）')
    generate_parser.add_argument('--max-backoff', type=float, default=8.0, help='最大退避秒数（指数退避上限）')
    # 大JSON数组流式解析
    generate_parser.add_argument('--stream-json-array', action='store_true', help='将JSON数组流式转换为临时JSONL再处理（需要 ijson）')
    
    # progress命令
    progress_parser = subparsers.add_parser('progress', help='查看生成进度')
    progress_parser.add_argument('--task-id', required=True, help='任务ID')
    
    # report命令
    report_parser = subparsers.add_parser('report', help='查看质量报告')
    report_parser.add_argument('--task-id', required=True, help='任务ID')
    
    # strategies命令
    strategies_parser = subparsers.add_parser('strategies', help='列出支持的生成策略')
    
    # describe命令
    describe_parser = subparsers.add_parser('describe', help='查看策略描述')
    describe_parser.add_argument('--strategy', required=True, help='策略名称')

    # resume命令
    resume_parser = subparsers.add_parser('resume', help='恢复中断任务')
    resume_parser.add_argument('--task-id', required=True, help='任务ID')
    resume_parser.add_argument('--model-id', help='覆盖模型ID')
    resume_parser.add_argument('--generation-count', type=int, help='覆盖生成数量')
    resume_parser.add_argument('--max-tokens', type=int, help='覆盖最大token数')
    resume_parser.add_argument('--temperature', type=float, help='覆盖温度参数')
    resume_parser.add_argument('--top-p', type=float, help='覆盖top_p')
    resume_parser.add_argument('--top-k', type=int, help='覆盖top_k')
    # 新增：并发与鲁棒性覆盖
    resume_parser.add_argument('--max-workers', type=int, help='覆盖并发度')
    resume_parser.add_argument('--rate-limit-rps', type=float, help='覆盖限流每秒请求数（0或缺省表示不限）')
    resume_parser.add_argument('--max-backoff', type=float, help='覆盖最大退避秒数')
    resume_parser.add_argument('--resume-as-new', action='store_true', help='克隆为新任务继续执行')
    
    args = parser.parse_args()

    if args.action == 'generate':
        # 构建生成参数
        params = {
            'strategy': args.strategy,
            'model_id': args.model_id,
            'generation_count': args.generation_count,
            'max_tokens': args.max_tokens,
            'temperature': args.temperature,
 'top_p': args.top_p,
        }
        if args.top_k is not None:
            params['top_k'] = args.top_k

        # 新增：性能与鲁棒性参数
        if getattr(args, 'max_workers', None) is not None:
            params['max_workers'] = int(args.max_workers)
        if getattr(args, 'fsync_interval', None) is not None:
            params['fsync_interval'] = int(args.fsync_interval)
        if getattr(args, 'checkpoint_interval', None) is not None:
            params['checkpoint_interval'] = int(args.checkpoint_interval)
        if getattr(args, 'inflight_multiplier', None) is not None:
            params['inflight_multiplier'] = int(args.inflight_multiplier)
        if getattr(args, 'unordered_write', False):
            params['unordered_write'] = True
        if getattr(args, 'rate_limit_rps', None) is not None:
            params['rate_limit_rps'] = float(args.rate_limit_rps)
        if getattr(args, 'max_backoff', None) is not None:
            params['max_backoff'] = float(args.max_backoff)
        if getattr(args, 'stream_json_array', None):
            params['stream_json_array'] = bool(args.stream_json_array)

        if args.input_file:
            params['input_file'] = args.input_file
        elif args.input_data:
                       params['input_data'] = json.loads(args.input_data)
        else:
            print("✗ 需要指定 --input-file 或 --input-data")
            exit(1)

        if args.selected_fields:
            params['selected_fields'] = [s.strip() for s in args.selected_fields.split(',') if s.strip()]
        if args.target_field:
            params['target_field'] = args.target_field
        if args.label_set:
            params['label_set'] = [s.strip() for s in args.label_set.split(',') if s.strip()]
        if args.system_prompt:
            params['system_prompt'] = args.system_prompt
        if args.q_prompt:
            params['q_prompt'] = args.q_prompt
        if args.a_prompt:
            params['a_prompt'] = args.a_prompt
        if args.q_field_name:
            params['q_field_name'] = args.q_field_name

        try:
            task_id = distill_generator.start_generation(params)
            print(f"✓ 生成任务已启动: {task_id}")
        except Exception as e:
            print(f"✗ 生成任务启动失败: {e}")

    elif args.action == 'progress':
        progress = distill_generator.get_generation_progress(args.task_id)
        if 'error' in progress:
            print(f"✗ 获取进度失败: {progress['error']}")
        else:
            print(f"任务 {args.task_id} 进度:")
            print(f"  状态: {progress['status']}")
            print(f"  进度: {progress['progress']:.1f}%")
            if progress.get('error_msg'):
                print(f"  错误: {progress['error_msg']}")

    elif args.action == 'report':
        report = distill_generator.get_quality_report(args.task_id)
        if 'error' in report:
            print(f"✗ 获取报告失败: {report['error']}")
        else:
            print(f"任务 {args.task_id} 质量报告:")
            print(f"  策略: {report['generation_strategy']}")
            print(f"  模型: {report['model_id']}")
            
            metrics = report['quality_metrics']
            print(f"  质量指标:")
            print(f"    输入项目数: {metrics['total_input_items']}")
            print(f"    生成项目数: {metrics['total_generated_items']}")
            print(f"    质量通过数: {metrics['quality_passed_items']}")
            print(f"    质量通过率: {metrics['quality_pass_rate']:.1f}%")
            print(f"    生成成功率: {metrics['generation_success_rate']:.1f}%")

    elif args.action == 'strategies':
        strategies = distill_generator.list_generation_strategies()
        print("支持的生成策略:")
        for strategy in strategies:
            desc = distill_generator.get_strategy_description(strategy)
            print(f"  {strategy}: {desc['description']}")

    elif args.action == 'describe':
        desc = distill_generator.get_strategy_description(args.strategy)
        if 'error' in desc:
            print(f"✗ {desc['error']}")
        else:
            print(f"策略: {args.strategy}")
            print(f"名称: {desc['name']}")
            print(f"描述: {desc['description']}")
            print(f"必需参数: {', '.join(desc['required_params']) if desc['required_params'] else '无'}")
            print(f"可选参数: {', '.join(desc['optional_params']) if desc['optional_params'] else '无'}")

    elif args.action == 'resume':
        overrides = {}
        # 基础可覆盖参数
        for name in ['model_id', 'generation_count', 'max_tokens', 'temperature', 'top_p', 'top_k']:
            val = getattr(args, name.replace('-', '_'), None) if '-' in name else getattr(args, name, None)
            if val is not None:
                overrides[name if name != 'model_id' else 'model_id'] = val
        # 新增：并发/限流/退避/克隆恢复
        if getattr(args, 'max_workers', None) is not None:
            overrides['max_workers'] = int(args.max_workers)
        if getattr(args, 'rate_limit_rps', None) is not None:
            overrides['rate_limit_rps'] = float(args.rate_limit_rps)
        if getattr(args, 'max_backoff', None) is not None:
            overrides['max_backoff'] = float(args.max_backoff)
        if getattr(args, 'resume_as_new', False):
            overrides['resume_as_new'] = True
        try:
            task_id = distill_generator.resume_generation(args.task_id, overrides or None)
            print(f"✓ 任务恢复已启动: {task_id}")
        except Exception as e:
            print(f"✗ 恢复任务失败: {e}")

    else:
        parser.print_help()
