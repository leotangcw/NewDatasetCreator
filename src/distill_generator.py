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
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import deque

# 可选：JSON数组流式解析（大 JSON 支持）
try:
    import ijson  # type: ignore
except Exception:
    ijson = None

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
基于以下数据样本，生成{count}个类似但不重复的数据样本：

原始样本：
{sample}

字段约束：
{field_hint}

要求：
1. 保持相同的数据结构和字段（如未指定字段约束，则可返回完整样本对象）
2. 内容要有变化但保持逻辑一致性
3. 确保生成的数据质量高且实用
4. 返回JSON格式的数据列表

生成的数据：
""",
            
        GenerationStrategy.ENHANCE: """
{sys_prefix}
对以下数据进行增强，添加更多有用的信息：

原始数据：
{data}

增强要求：
1. 添加相关的补充信息
2. 丰富数据的上下文
3. 保持原有信息的准确性
4. 返回增强后的JSON格式数据（如指定目标字段，请将增强内容写入该字段）

增强结果：
""",
            
        GenerationStrategy.PARAPHRASE: """
{sys_prefix}
对以下文本进行改写，生成{count}个不同的表达方式：

原文：
{text}

改写要求：
1. 保持原意不变
2. 使用不同的表达方式
3. 确保语言自然流畅
4. 返回改写后的文本列表（仅文本，不要额外说明）

改写结果：
""",
            
        GenerationStrategy.CLASSIFY_LABEL: """
{sys_prefix}
为以下数据生成分类标签和说明：

数据：
{data}

分类要求：
1. 生成合适的分类标签{label_hint}
2. 提供分类依据和说明
3. 确保分类的准确性
4. 返回包含标签和说明的JSON格式，字段建议：{{"label":"...","reason":"..."}}

分类结果：
""",
            
    GenerationStrategy.Q_TO_A: """
{sys_prefix}
请针对下述问题给出高质量回答（无需特定格式；按你自然的表达即可）：

问题：
{question}

回答：
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
            # 可选：标签集合，逗号分隔
            if 'label_set' in params and isinstance(params['label_set'], str):
                params['label_set'] = [s.strip() for s in params['label_set'].split(',') if s.strip()]
        
        # 验证输入数据
        if 'input_data' not in params and 'input_file' not in params:
            raise ValueError('需要指定input_data或input_file')
        
        if 'input_file' in params:
            input_file = Path(params['input_file'])
            if not input_file.exists():
                raise FileNotFoundError(f'输入文件不存在: {input_file}')
    
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
            generation_count = params.get('generation_count', 1)
            
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
            
            # 执行生成
            with open(output_file, 'w', encoding='utf-8') as f:
                if strategy in [GenerationStrategy.EXPAND, GenerationStrategy.PARAPHRASE, GenerationStrategy.Q_TO_A, GenerationStrategy.CUSTOM, GenerationStrategy.CLASSIFY_LABEL]:
                    self._generate_multiple(task_id, input_data, strategy, model_id, 
                                          generation_count, f, generation_stats, params)
                else:
                    self._generate_single(task_id, input_data, strategy, model_id, 
                                        f, generation_stats, params)
            
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
        """加载checkpoint"""
        ckpt_path = task_dir / 'checkpoint.json'
        if ckpt_path.exists():
            try:
                with open(ckpt_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return {}
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
        generation_count = params.get('generation_count', 1)
        input_file = Path(params['input_file'])
        task_dir = self.output_dir / task_id
        task_dir.mkdir(parents=True, exist_ok=True)

        # 初始化/加载 checkpoint
        ckpt = self._load_checkpoint(task_dir) or {}
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

        # 若有覆盖参数（例如更换模型），以 params 为准，合并存回 ckpt
        for k, v in params.items():
            if k != 'input_data':
                ckpt.setdefault('params', {})[k] = v
        model_id = ckpt['params'].get('model_id', model_id)
        generation_count = int(ckpt['params'].get('generation_count', generation_count))
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
                                        # 等待任意完成并写入可写部分
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
                                        stats['total_generated'] += 1
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
                state_manager.add_task(TaskType.DISTILL, base_params.get('strategy'), base_params, task_id=task_id)
                state_manager.update_state(task_id, 'status', 'running')
                state_manager.update_state(task_id, 'start_time', state_manager.get_task_state(task_id).get('start_time', datetime.now().isoformat()))

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
                          strategy: str, model_id: str, generation_count: int,
                          output_file, stats: Dict[str, int], params: Dict[str, Any]) -> None:
        """生成多个变体"""
        total_tasks = len(input_data)
        completed_tasks = 0
        fsync_interval = int(params.get('fsync_interval', 50))
        write_counter = 0
        local_max_workers = int(params.get('max_workers', self.max_workers))
        
        # 使用线程池并发生成
        with ThreadPoolExecutor(max_workers=local_max_workers) as executor:
            # 提交所有生成任务
            future_to_data = {}
            for i, data_item in enumerate(input_data):
                # 检查是否已暂停
                state = state_manager.get_task_state(task_id) or {}
                if state.get('status') == 'paused':
                    self.logger.info(f'检测到暂停，停止提交新任务: {task_id}')
                    break
                future = executor.submit(
                    self._generate_for_item,
                    task_id, data_item, strategy, model_id, generation_count, params
                )
                future_to_data[future] = (i, data_item)
            
            # 处理完成的任务
            for future in as_completed(future_to_data):
                try:
                    item_index, original_data = future_to_data[future]
                    generated_items = future.result()
                    
                    # 写入生成的数据
                    for generated_item in generated_items:
                        if self._evaluate_quality(generated_item, params):
                            output_file.write(json.dumps(generated_item, ensure_ascii=False) + '\n')
                            output_file.flush()
                            write_counter += 1
                            if write_counter % fsync_interval == 0:
                                try:
                                    os.fsync(output_file.fileno())
                                except Exception:
                                    pass
                            stats['quality_passed'] += 1
                        else:
                            stats['quality_failed'] += 1
                        
                        stats['total_generated'] += 1
                    
                    stats['successful_generations'] += 1
                    
                except Exception as e:
                    self.logger.error(f'生成任务失败: {e}')
                    stats['failed_generations'] += 1
                
                finally:
                    completed_tasks += 1
                    progress = completed_tasks / total_tasks * 100
                    state_manager.update_state(task_id, 'progress', progress)
                    # 若暂停，则尽快退出
                    state = state_manager.get_task_state(task_id) or {}
                    if state.get('status') == 'paused':
                        self.logger.info(f'处理中检测到暂停，提前结束: {task_id}')
                        break
    
    def _generate_single(self, task_id: str, input_data: List[Dict[str, Any]], 
                        strategy: str, model_id: str, output_file,
                        stats: Dict[str, int], params: Dict[str, Any]) -> None:
        """生成单个结果"""
        total_tasks = len(input_data)
        completed_tasks = 0
        fsync_interval = int(params.get('fsync_interval', 50))
        write_counter = 0
        local_max_workers = int(params.get('max_workers', self.max_workers))
        
        # 使用线程池并发生成
        with ThreadPoolExecutor(max_workers=local_max_workers) as executor:
            # 提交所有生成任务
            future_to_data = {}
            for i, data_item in enumerate(input_data):
                # 检查是否已暂停
                state = state_manager.get_task_state(task_id) or {}
                if state.get('status') == 'paused':
                    self.logger.info(f'检测到暂停，停止提交新任务: {task_id}')
                    break
                future = executor.submit(
                    self._generate_for_item,
                    task_id, data_item, strategy, model_id, 1, params
                )
                future_to_data[future] = (i, data_item)
            
            # 处理完成的任务
            for future in as_completed(future_to_data):
                try:
                    item_index, original_data = future_to_data[future]
                    generated_items = future.result()
                    
                    # 写入生成的数据
                    for generated_item in generated_items:
                        if self._evaluate_quality(generated_item, params):
                            output_file.write(json.dumps(generated_item, ensure_ascii=False) + '\n')
                            output_file.flush()
                            write_counter += 1
                            if write_counter % fsync_interval == 0:
                                try:
                                    os.fsync(output_file.fileno())
                                except Exception:
                                    pass
                            stats['quality_passed'] += 1
                        else:
                            stats['quality_failed'] += 1
                        
                        stats['total_generated'] += 1
                    
                    stats['successful_generations'] += 1
                    
                except Exception as e:
                    self.logger.error(f'生成任务失败: {e}')
                    stats['failed_generations'] += 1
                
                finally:
                    completed_tasks += 1
                    progress = completed_tasks / total_tasks * 100
                    state_manager.update_state(task_id, 'progress', progress)
                    # 若暂停，则尽快退出
                    state = state_manager.get_task_state(task_id) or {}
                    if state.get('status') == 'paused':
                        self.logger.info(f'处理中检测到暂停，提前结束: {task_id}')
                        break
    
    def _generate_for_item(self, task_id: str, data_item: Dict[str, Any], strategy: str, 
                          model_id: str, count: int, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """为单个数据项生成内容"""
        # 若任务被暂停，尽早返回空
        state = state_manager.get_task_state(task_id) or {}
        if state.get('status') == 'paused':
            return []
        # 构建提示
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
                generated_items = self._parse_response(response, strategy, data_item, params.get('target_field'), params)
                
                if generated_items:
                    return generated_items
                else:
                    self.logger.warning(f'模型返回空结果，重试第{attempt + 1}次')
                    
            except Exception as e:
                self.logger.error(f'模型调用失败（尝试{attempt + 1}）: {e}')
                if attempt == self.max_retries - 1:
                    raise
                # 指数退避 + 抖动，避免持续撞限流
                try:
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
        system_prompt = (params.get('system_prompt') or '').strip()
        sys_prefix = f"[系统提示]\n{system_prompt}\n\n" if system_prompt else ''
        field_hint = ''
        if params.get('selected_fields'):
            field_hint = f"仅对以下字段进行生成/改写：{', '.join(params['selected_fields'])}。"
        label_hint = ''
        if strategy == GenerationStrategy.CLASSIFY_LABEL and params.get('label_set'):
            label_hint = f"（可选标签集合：{', '.join(params['label_set'])}）"

        if strategy == GenerationStrategy.EXPAND:
            return template.format(
                sys_prefix=sys_prefix,
                count=count,
                sample=json.dumps(data_item, ensure_ascii=False, indent=2),
                field_hint=field_hint or '（未指定字段约束）'
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
            return template.format(
                sys_prefix=sys_prefix,
                data=json.dumps(data_item, ensure_ascii=False, indent=2),
                label_hint=label_hint
            )

        elif strategy == GenerationStrategy.Q_TO_A:
            q_text = ''
            if params.get('selected_fields'):
                for f in params['selected_fields']:
                    if f in data_item and isinstance(data_item[f], (str, int, float)):
                        q_text = str(data_item[f])
                        break
            if not q_text:
                # 常见问字段兜底
                for f in ['question', 'instruction', 'input', 'query', 'prompt']:
                    if f in data_item and data_item[f]:
                        q_text = str(data_item[f])
                        break
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
            if system_prompt:
                custom_parts.append(f"[系统]\n{system_prompt}")
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
        """从文本中提取JSON"""
        import re
        
        # 查找JSON对象或数组
        json_pattern = r'(\[.*?\]|\{.*?\})'
        matches = re.findall(json_pattern, text, re.DOTALL)
        
        results = []
        for match in matches:
            try:
                parsed = json.loads(match)
                if isinstance(parsed, dict):
                    results.append(parsed)
                elif isinstance(parsed, list):
                    results.extend(parsed)
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
        if strategy == GenerationStrategy.EXPAND and isinstance(gen, dict):
            # 合并原始与生成（生成优先）
            merged = dict(original_data)
            merged.update(gen)
            return merged
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
        # 常见问字段顺序
        for f in ['question', 'instruction', 'input', 'query', 'prompt']:
            if f in original_data and original_data.get(f) is not None:
                return str(original_data.get(f))
        return str(original_data)
    
    def _evaluate_quality(self, generated_item: Dict[str, Any], params: Dict[str, Any]) -> bool:
        """评估生成质量"""
        # 基本质量检查
        if not generated_item or not isinstance(generated_item, dict):
            return False
        
        # 检查是否有实际内容
        has_content = any(
            value and str(value).strip() 
            for value in generated_item.values() 
            if not str(value).startswith('original')
        )
        
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
