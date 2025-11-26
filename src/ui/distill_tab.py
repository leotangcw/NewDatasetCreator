import os
import json
import time
from datetime import datetime
from pathlib import Path
import gradio as gr
from typing import Dict, Any, Tuple, List, Optional
from ..dependencies import pd
from ..model_manager import model_manager
from ..distill_generator import distill_generator
from ..state_manager import state_manager, TaskType
import shutil

class DistillTabManager:
    def __init__(self, launcher):
        self.launcher = launcher
        self.logger = launcher.logger
        self.root_dir = launcher.root_dir
        
    def create_tab(self):
        """创建蒸馏生成标签页"""
        gr.Markdown("## 蒸馏生成管理")
        gr.Markdown("基于AI模型生成高质量的训练数据")

        initial_tasks_df = self._get_distill_tasks_df()
        
        with gr.Row():
            with gr.Column(scale=1):
                # 蒸馏配置区域
                gr.Markdown("### 生成配置")
                
                distill_source = gr.File(
                    label="源数据文件",
                    file_types=[".jsonl", ".json"]
                )
                
                distill_strategy = gr.Dropdown(
                    choices=[
                        ("数据扩充", "expand"),
                        ("数据增强", "enhance"),
                        ("文本改写", "paraphrase"),
                        ("分类标注", "classify_label"),
                        ("从Q生A", "q_to_a"),
                        ("自定义", "custom")
                    ],
                    value=self.launcher.config_manager.get_config("ui_state.distill.distill_strategy", "expand"),
                    label="生成策略",
                    info="选择数据生成的策略类型"
                )
                distill_strategy.change(lambda x: (self.launcher.config_manager.update_config("ui_state.distill.distill_strategy", x), None)[1], inputs=[distill_strategy], outputs=[])
                
                distill_model = gr.Dropdown(
                    label="选择模型",
                    choices=list(model_manager.get_all_models().keys()),
                    value=self.launcher.config_manager.get_config("ui_state.distill.distill_model", None),
                    info="选择用于生成的AI模型"
                )
                distill_model.change(lambda x: (self.launcher.config_manager.update_config("ui_state.distill.distill_model", x), None)[1], inputs=[distill_model], outputs=[])
                
                refresh_models_btn = gr.Button("刷新模型列表", size="sm")
                
                distill_count = gr.Slider(
                    minimum=1,
                    maximum=50,
                    value=self.launcher.config_manager.get_config("ui_state.distill.distill_count", 5),
                    step=1,
                    label="生成数量",
                    info="每个输入样本生成的数量"
                )
                distill_count.change(lambda x: (self.launcher.config_manager.update_config("ui_state.distill.distill_count", x), None)[1], inputs=[distill_count], outputs=[])
                
                distill_temperature = gr.Slider(
                    minimum=0.1,
                    maximum=2.0,
                    value=self.launcher.config_manager.get_config("ui_state.distill.distill_temperature", 0.7),
                    step=0.1,
                    label="温度参数",
                    info="控制生成的随机性，值越高越随机"
                )
                distill_temperature.change(lambda x: (self.launcher.config_manager.update_config("ui_state.distill.distill_temperature", x), None)[1], inputs=[distill_temperature], outputs=[])
                
                distill_max_tokens = gr.Slider(
                    minimum=100,
                    maximum=200000,
                    value=self.launcher.config_manager.get_config("ui_state.distill.distill_max_tokens", 2048),
                    step=100,
                    label="最大Token数",
                    info="限制生成文本的最大长度（将根据模型类型动态调整上限）"
                )
                distill_max_tokens.change(lambda x: (self.launcher.config_manager.update_config("ui_state.distill.distill_max_tokens", x), None)[1], inputs=[distill_max_tokens], outputs=[])

                distill_top_p = gr.Slider(
                    minimum=0.1,
                    maximum=1.0,
                    value=0.9,
                    step=0.05,
                    label="top_p",
                    info="采样截断概率"
                )

                distill_top_k = gr.Slider(
                    minimum=0,
                    maximum=200,
                    value=0,
                    step=1,
                    label="top_k (0 表示未启用)",
                    info="top-k 采样，0 表示关闭"
                )

                with gr.Accordion("高级设置：并发与性能", open=False):
                    concurrency_workers = gr.Slider(
                        minimum=1,
                        maximum=128,
                        value=self.launcher.config_manager.get_config("ui_state.distill.concurrency_workers", 8),
                        step=1,
                        label="并发度（同时请求数）",
                        info="根据模型吞吐与限流调整；JSONL 大任务建议 8~64 之间"
                    )
                    concurrency_workers.change(lambda x: (self.launcher.config_manager.update_config("ui_state.distill.concurrency_workers", x), None)[1], inputs=[concurrency_workers], outputs=[])
                    fsync_interval_slider = gr.Slider(
                        minimum=1,
                        maximum=1000,
                        value=50,
                        step=1,
                        label="写盘同步间隔（行）",
                        info="每多少行调用一次 fsync，越小越安全，越大吞吐越高"
                    )
                    checkpoint_interval_slider = gr.Slider(
                        minimum=10,
                        maximum=5000,
                        value=100,
                        step=10,
                        label="检查点保存间隔（行）",
                        info="每多少输入行保存一次 checkpoint.json"
                    )
                    inflight_multiplier_slider = gr.Slider(
                        minimum=1,
                        maximum=20,
                        value=4,
                        step=1,
                        label="在途任务倍数",
                        info="最大在途任务数 = 并发度 * 倍数（流式JSONL模式）"
                    )
                    unordered_write_checkbox = gr.Checkbox(
                        label="无序写入（完成即写）",
                        value=False,
                        info="提高吞吐量，放弃严格按输入顺序写出"
                    )
                    rate_limit_rps_number = gr.Number(
                        label="限流：每秒请求数（可空）",
                        value=None,
                        precision=2,
                        interactive=True
                    )
                    max_backoff_number = gr.Number(
                        label="最大退避秒数",
                        value=8.0,
                        precision=2,
                        interactive=True
                    )
                
                # 策略说明与提示词/字段/参数
                with gr.Accordion("策略与提示词", open=True):
                    strategy_desc = gr.Markdown("*选择策略后显示说明与参数*")

                    system_prompt_box = gr.Textbox(
                        label="System 提示词",
                        lines=3,
                        placeholder="可选：用于约束整体风格、禁则等",
                        value=self.launcher.config_manager.get_config("ui_state.distill.system_prompt_box", "")
                    )
                    system_prompt_box.change(lambda x: (self.launcher.config_manager.update_config("ui_state.distill.system_prompt_box", x), None)[1], inputs=[system_prompt_box], outputs=[])
                    q_prompt_box = gr.Textbox(
                        label="Q 提示词（仅 q_to_a/custom）",
                        lines=2,
                        visible=False,
                        placeholder="用于引导问题的构造或选取要点"
                    )
                    a_prompt_box = gr.Textbox(
                        label="A 提示词（仅 q_to_a/custom）",
                        lines=2,
                        visible=False,
                        placeholder="用于约束答案的风格与结构"
                    )

                with gr.Accordion("字段映射配置", open=False):
                    with gr.Row():
                        dataset_fields_box = gr.CheckboxGroup(
                            label="从数据文件中检测到的字段（多选）",
                            choices=[],
                            value=[],
                            interactive=True,
                            info="选择需要参与生成/改写的字段（将同步到下方文本框）"
                        )
                        dataset_single_field_dropdown = gr.Dropdown(
                            label="从数据文件中检测到的字段（单选）",
                            choices=[],
                            value=None,
                            interactive=True,
                            visible=False,
                            info="选择需要扩充/改写的源字段"
                        )
                    
                    selected_fields_input = gr.Textbox(
                        label="选定字段（逗号分隔）",
                        placeholder="例如：instruction,output 或 question,answer"
                    )
                    source_field_input = gr.Textbox(
                        label="源字段名（单选）",
                        placeholder="例如：instruction",
                        visible=False
                    )
                    
                    q_field_name_input = gr.Textbox(
                        label="Q 字段名（输出数据中使用）",
                        value=self.launcher.config_manager.get_config("ui_state.distill.q_field_name_input", "instruction"),
                        placeholder="默认 instruction，可自定义为 question 等"
                    )
                    q_field_name_input.change(lambda x: (self.launcher.config_manager.update_config("ui_state.distill.q_field_name_input", x), None)[1], inputs=[q_field_name_input], outputs=[])
                    
                    label_set_input = gr.Textbox(
                        label="标签集合（仅分类标注，逗号分隔）",
                        visible=False,
                        placeholder="例如：正向,负向,中立"
                    )
                    target_field_input = gr.Textbox(
                        label="目标字段名（生成结果写入此处）",
                        value=self.launcher.config_manager.get_config("ui_state.distill.target_field_input", "output"),
                        placeholder="生成内容写入的字段名，默认 output"
                    )
                    target_field_input.change(lambda x: (self.launcher.config_manager.update_config("ui_state.distill.target_field_input", x), None)[1], inputs=[target_field_input], outputs=[])
                
                with gr.Row():
                    start_distill_btn = gr.Button("开始生成", variant="primary")
                    pause_distill_btn = gr.Button("暂停生成", variant="secondary")
            
            with gr.Column(scale=2):
                # 任务状态区域
                gr.Markdown("### 任务进度")
                
                distill_progress = gr.Progress()
                
                distill_task_list = gr.Dataframe(
                    headers=["选择", "任务ID", "策略", "模型", "状态", "进度", "开始时间"],
                    datatype=["bool", "str", "str", "str", "str", "str", "str"],
                    label="蒸馏任务列表",
                    interactive=True,
                    wrap=True,
                    column_widths=["60px", "200px", "120px", "150px", "100px", "100px", "150px"],
                    value=initial_tasks_df
                )
                
                with gr.Row():
                    resume_as_new_checkbox = gr.Checkbox(label="克隆为新任务继续", value=False, info="恢复时创建新任务ID")
                    delete_files_checkbox = gr.Checkbox(label="删除时同时删除文件", value=False, info="慎用：将永久删除数据")

                with gr.Row():
                    refresh_distill_btn = gr.Button("刷新", size="sm")
                    view_report_btn = gr.Button("查看报告", size="sm")
                    start_task_btn = gr.Button("启动", size="sm", variant="primary")
                    pause_task_btn = gr.Button("暂停", size="sm", variant="secondary")
                    resume_task_btn = gr.Button("恢复(单任务)", size="sm")
                    delete_task_btn = gr.Button("删除", size="sm", variant="stop")
                
                selected_distill_task = gr.Textbox(
                    label="选中任务ID (用于查看报告/高级恢复)",
                    placeholder="点击任务行选择",
                    interactive=False
                )

                with gr.Accordion("高级恢复参数覆盖 (仅针对选中任务)", open=False):
                    gr.Markdown("**使用提示**：此处配置仅在点击【恢复(单任务)】时生效。当任务因报错（如限流、OOM）中断时，可在此临时调整参数（如降低并发、更换模型）继续运行，而无需新建任务。留空则沿用原配置。")
                    resume_model_override = gr.Dropdown(
                        label="覆盖模型（可选）", 
                        choices=list(model_manager.get_all_models().keys()), 
                        interactive=True
                    )
                    resume_workers = gr.Slider(minimum=1, maximum=128, value=8, step=1, label="并发度（覆盖可选）")
                    resume_temp = gr.Slider(minimum=0.1, maximum=2.0, value=0.7, step=0.1, label="温度（覆盖可选）")
                    resume_max_tokens = gr.Slider(minimum=100, maximum=200000, value=2048, step=100, label="最大Token（覆盖可选）")
                    resume_top_p = gr.Slider(minimum=0.1, maximum=1.0, value=0.9, step=0.05, label="top_p（覆盖可选）")
                    resume_top_k = gr.Slider(minimum=0, maximum=200, value=0, step=1, label="top_k（覆盖可选，0=不变）")
                    resume_rate_limit_rps = gr.Number(label="限流RPS（覆盖）", value=None, precision=2)
                    resume_max_backoff = gr.Number(label="最大退避（覆盖）", value=None, precision=2)
        
        # 状态输出区域
        with gr.Row():
            distill_status = gr.Textbox(
                label="状态信息",
                lines=5,
                interactive=False,
                show_copy_button=True
            )
        
        # 自动刷新定时器
        auto_refresh_timer = gr.Timer(value=2)
        
        # 使用 State 存储选中的任务ID，避免 Dataframe 输入问题
        # 使用 list 存储以便 Gradio 在 JSON 序列化时保持稳定
        selected_tasks_state = gr.State(value=[])
        current_df_state = gr.State(value=initial_tasks_df.copy(deep=True) if hasattr(initial_tasks_df, "copy") else initial_tasks_df)


        # 统一的 Dataframe 选择事件处理
        def _on_task_list_select(evt: gr.SelectData, df_value, current_selection):
            # 1. 准备基础数据
            selection_set = set(current_selection or [])
            df_copy = df_value.copy(deep=True) if isinstance(df_value, pd.DataFrame) else pd.DataFrame()
            
            # 默认的表单更新（不改变任何值）
            no_form_update = [gr.update()] * 30
            
            try:
                col_index = evt.index[1]
                
                # === 情况 A: 点击复选框列 (Column 0) ===
                # 这里不再处理状态更新，全权交给 change 事件处理
                # 仅保留日志以便调试
                if col_index == 0:
                    # self.logger.info(f"DEBUG: Cell select on checkbox column. Row={evt.index[0]}")
                    return [list(selection_set), df_copy] + no_form_update

                # === 情况 B: 点击任务ID列 (Column 1) ===
                elif col_index == 1:
                    # 调用原有的参数回填逻辑
                    form_updates = self._select_distill_task(evt)
                    return [list(selection_set), df_copy] + list(form_updates)

                # === 情况 C: 点击其他列 ===
                else:
                    return [list(selection_set), df_copy] + no_form_update

            except Exception as e:
                self.logger.error(f"Selection error: {e}")
                return [list(selection_set), df_copy] + no_form_update

        # 新增：监听表格数据变化（捕获复选框点击）
        def _on_task_list_change(df):
            try:
                if df is None or (hasattr(df, 'empty') and df.empty):
                    return []
                
                # 提取第一列为 True 的 Task ID
                # 假设第0列是 bool, 第1列是 Task ID
                selected_rows = df[df.iloc[:, 0] == True]
                if selected_rows.empty:
                    ids = []
                else:
                    ids = [str(x) for x in selected_rows.iloc[:, 1].tolist()]
                
                # self.logger.info(f"[DistillTab] Data change detected. Selected count: {len(ids)}")
                return ids
            except Exception as e:
                self.logger.error(f"Data change error: {e}")
                return []

        # 绑定 change 事件：只要数据变了（包括点复选框），就更新 State
        distill_task_list.change(
            fn=_on_task_list_change,
            inputs=[distill_task_list],
            outputs=[selected_tasks_state]
        )

        # 绑定 select 事件：处理点击任务ID的回填
        distill_task_list.select(
            fn=_on_task_list_select,
            inputs=[current_df_state, selected_tasks_state],
            outputs=[
                selected_tasks_state, 
                current_df_state,
                # 以下是表单组件
                selected_distill_task,
                distill_strategy, distill_model, distill_count,
                distill_temperature, distill_max_tokens, distill_top_p, distill_top_k,
                concurrency_workers, fsync_interval_slider, checkpoint_interval_slider,
                inflight_multiplier_slider, unordered_write_checkbox,
                rate_limit_rps_number, max_backoff_number,
                system_prompt_box, q_prompt_box, a_prompt_box,
                selected_fields_input, q_field_name_input, label_set_input, target_field_input,
                resume_model_override, resume_workers, resume_temp, resume_max_tokens,
                resume_top_p, resume_top_k, resume_rate_limit_rps, resume_max_backoff
            ]
        )
        
        # 刷新逻辑改为读取 State
        def _refresh_with_state(selected_ids):
            self._log_selection_state("timer-refresh-input", selected_ids)
            new_df = self._refresh_distill_tasks_logic_with_state(selected_ids)
            df_state = new_df.copy(deep=True) if hasattr(new_df, 'copy') else new_df
            true_count = int(new_df.iloc[:, 0].sum()) if hasattr(new_df, 'iloc') and not new_df.empty else 0
            self.logger.debug(f"[DistillTab] timer-refresh-output checkbox_true={true_count} rows={len(new_df)}")
            return new_df, df_state

        auto_refresh_timer.tick(
            fn=_refresh_with_state,
            inputs=[selected_tasks_state],
            outputs=[distill_task_list, current_df_state]
        )
        
        # 存储组件引用
        self.launcher.components['distill'] = {
            'auto_refresh_timer': auto_refresh_timer,
            'source': distill_source,
            'strategy': distill_strategy,
            'model': distill_model,
            'count': distill_count,
            'temperature': distill_temperature,
            'max_tokens': distill_max_tokens,
            'top_p': distill_top_p,
            'top_k': distill_top_k,
            'concurrency_workers': concurrency_workers,
            'fsync_interval': fsync_interval_slider,
            'checkpoint_interval': checkpoint_interval_slider,
            'inflight_multiplier': inflight_multiplier_slider,
            'unordered_write': unordered_write_checkbox,
            'rate_limit_rps': rate_limit_rps_number,
            'max_backoff': max_backoff_number,
            'system_prompt': system_prompt_box,
            'q_prompt': q_prompt_box,
            'a_prompt': a_prompt_box,
            'dataset_fields': dataset_fields_box,
            'selected_fields': selected_fields_input,
            'q_field_name': q_field_name_input,
            'label_set': label_set_input,
            'target_field': target_field_input,
            'task_list': distill_task_list,
            'selected_task': selected_distill_task,
            'status': distill_status,
            'resume_model_override': resume_model_override,
            'resume_workers': resume_workers,
            'resume_temp': resume_temp,
            'resume_max_tokens': resume_max_tokens,
            'resume_top_p': resume_top_p,
            'resume_top_k': resume_top_k,
            'resume_rate_limit_rps': resume_rate_limit_rps,
            'resume_max_backoff': resume_max_backoff,
            'resume_as_new': resume_as_new_checkbox,
            'selected_tasks_state': selected_tasks_state,
            'current_df_state': current_df_state
        }
        
        # 绑定事件处理器
        start_distill_btn.click(
            fn=self._start_distill_generation,
            inputs=[
                distill_source, distill_strategy, distill_model, distill_count,
                distill_temperature, distill_max_tokens, distill_top_p, distill_top_k,
                concurrency_workers, fsync_interval_slider, checkpoint_interval_slider,
                inflight_multiplier_slider, unordered_write_checkbox,
                rate_limit_rps_number, max_backoff_number,
                system_prompt_box, q_prompt_box, a_prompt_box,
                selected_fields_input, source_field_input, q_field_name_input, label_set_input, target_field_input,
                selected_tasks_state
            ],
            outputs=[distill_status, distill_task_list, current_df_state]
        )
        
        pause_distill_btn.click(
            fn=self._pause_distill_generation,
            inputs=[selected_distill_task, selected_tasks_state],
            outputs=[distill_status, distill_task_list, current_df_state]
        )
        
        refresh_distill_btn.click(
            fn=_refresh_with_state,
            inputs=[selected_tasks_state],
            outputs=[distill_task_list, current_df_state]
        )
        
        view_report_btn.click(
            fn=self._view_distill_report,
            inputs=[selected_distill_task],
            outputs=[distill_status]
        )

        resume_task_btn.click(
            fn=self._resume_distill_task,
            inputs=[
                selected_distill_task,
                resume_model_override,
                resume_workers,
                resume_temp,
                resume_max_tokens,
                resume_top_p,
                resume_top_k,
                resume_rate_limit_rps,
                resume_max_backoff,
                resume_as_new_checkbox,
                selected_tasks_state
            ],
            outputs=[distill_status, distill_task_list, current_df_state]
        )

        # 绑定右侧批量操作按钮
        start_task_btn.click(
            fn=self._start_multiple_tasks,
            inputs=[current_df_state, selected_tasks_state],
            outputs=[distill_status, distill_task_list, current_df_state]
        )
        
        pause_task_btn.click(
            fn=self._pause_multiple_tasks,
            inputs=[current_df_state, selected_tasks_state],
            outputs=[distill_status, distill_task_list, current_df_state]
        )

        delete_task_btn.click(
            fn=self._delete_multiple_tasks,
            inputs=[current_df_state, delete_files_checkbox, selected_tasks_state],
            outputs=[distill_status, distill_task_list, current_df_state]
        )
        
        # 策略变化事件
        distill_strategy.change(
            fn=self._on_strategy_change,
            inputs=[distill_strategy],
            outputs=[strategy_desc, distill_count, q_prompt_box, a_prompt_box, label_set_input, dataset_fields_box, dataset_single_field_dropdown, selected_fields_input, source_field_input, target_field_input, q_field_name_input]
        )

        # 模型变化事件（动态调整 max_tokens 上限）
        distill_model.change(
            fn=self._on_distill_model_change,
            inputs=[distill_model],
            outputs=[distill_max_tokens]
        )

        # 源文件变化事件（自动检测字段）
        distill_source.change(
            fn=self._on_distill_source_change,
            inputs=[distill_source],
            outputs=[dataset_fields_box, selected_fields_input, dataset_single_field_dropdown]
        )

        # 字段复选框变化事件（同步到文本框）
        dataset_fields_box.change(
            fn=self._sync_selected_fields_text,
            inputs=[dataset_fields_box],
            outputs=[selected_fields_input]
        )

        # 单选字段下拉框变化事件（同步到文本框）
        dataset_single_field_dropdown.change(
            fn=self._sync_source_field_text,
            inputs=[dataset_single_field_dropdown],
            outputs=[source_field_input]
        )
        
        # (已移除重复的 select 绑定，合并至上方的 _on_task_list_select)
        
        # 刷新模型列表按钮
        def refresh_model_choices():
            models = list(model_manager.get_all_models().keys())
            return gr.update(choices=models), gr.update(choices=models)
            
        refresh_models_btn.click(
            fn=refresh_model_choices,
            outputs=[distill_model, resume_model_override]
        )

        # 页面加载时触发一次策略更新，确保初始状态正确
        # 使用 Timer 触发一次，并在回调中关闭 Timer (兼容旧版 Gradio 不支持 repeat 参数)
        init_timer = gr.Timer(value=0.1)
        
        def _init_ui_wrapper(strategy):
            updates = self._on_strategy_change(strategy)
            # 追加关闭 Timer 的更新
            return updates + (gr.Timer(active=False),)

        init_timer.tick(
            fn=_init_ui_wrapper,
            inputs=[distill_strategy],
            outputs=[strategy_desc, distill_count, q_prompt_box, a_prompt_box, label_set_input, dataset_fields_box, dataset_single_field_dropdown, selected_fields_input, source_field_input, target_field_input, q_field_name_input, init_timer]
        )

    def _start_distill_generation(self, source_file, strategy: str, model_name: str, count: int,
                                temperature: float, max_tokens: int, top_p: float, top_k: int,
                                workers: int, fsync_interval: int, checkpoint_interval: int,
                                inflight_multiplier: int, unordered_write: bool,
                                rate_limit_rps: Optional[float], max_backoff: Optional[float],
                                system_prompt: str, q_prompt: str, a_prompt: str,
                                selected_fields: str, source_field: str, q_field_name: str, label_set: str, target_field: str,
                                selected_ids=None) -> Tuple[str, Any]:
        """启动蒸馏生成任务"""
        try:
            self.logger.info(f"启动蒸馏任务: strategy={strategy}, model={model_name}, count={count}")
            
            if source_file is None:
                return "请选择源数据文件", self._get_distill_tasks_df()
            
            if not model_name:
                return "请选择模型", self._get_distill_tasks_df()
            
            source_path = source_file.name
            if not os.path.exists(source_path):
                return "源文件不存在", self._get_distill_tasks_df()
            
            # 确保 count 是整数且有效
            try:
                count = int(count)
                if count < 1:
                    count = 1
            except:
                count = 1
            
            # 构建参数
            params = {
                'input_file': source_path,
                'strategy': strategy,
                'model_id': model_name,
                'generation_count': count,
                'temperature': float(temperature),
                'max_tokens': int(max_tokens),
                'top_p': float(top_p),
                'top_k': int(top_k),
                'max_workers': int(workers),
                'fsync_interval': int(fsync_interval),
                'checkpoint_interval': int(checkpoint_interval),
                'inflight_multiplier': int(inflight_multiplier),
                'unordered_write': bool(unordered_write)
            }

            # 限流与退避
            if rate_limit_rps is not None and str(rate_limit_rps).strip() != "":
                try:
                    params['rate_limit_rps'] = float(rate_limit_rps)
                except Exception:
                    pass
            
            if max_backoff is not None and str(max_backoff).strip() != "":
                try:
                    params['max_backoff'] = float(max_backoff)
                except Exception:
                    pass

            # 提示词
            if system_prompt and system_prompt.strip():
                params['system_prompt'] = system_prompt.strip()
            if q_prompt and q_prompt.strip():
                params['q_prompt'] = q_prompt.strip()
            if a_prompt and a_prompt.strip():
                params['a_prompt'] = a_prompt.strip()

            # 字段/标签/目标
            if selected_fields and selected_fields.strip():
                params['selected_fields'] = selected_fields.strip()
            if source_field and source_field.strip():
                params['source_field'] = source_field.strip()
                
            # Q 字段名（输出名）默认 instruction
            q_out = (q_field_name or '').strip() or 'instruction'
            params['q_field_name'] = q_out
            if label_set and label_set.strip():
                params['label_set'] = label_set.strip()
            # A 字段名（目标字段）默认 output
            tgt = (target_field or '').strip() or 'output'
            params['target_field'] = tgt
            
            # 调用蒸馏生成模块
            task_id = distill_generator.start_generation(params)
            
            # 返回更新后的任务列表
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"蒸馏生成任务已启动！\n任务ID: {task_id}\n策略: {strategy}\n模型: {model_name}", df, df_state
            
        except Exception as e:
            self.logger.error(f'启动蒸馏生成失败: {e}')
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"启动失败: {str(e)}", df, df_state

    def _on_strategy_change(self, strategy: str) -> Tuple[str, Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """策略切换时，更新说明与控件可见性"""
        try:
            # 获取策略描述
            desc = distill_generator.get_strategy_description(strategy)
            if 'error' in desc:
                md = f"未知策略: {strategy}"
            else:
                name = desc.get('name', strategy)
                description = desc.get('description', '')
                req = desc.get('required_params', [])
                opt = desc.get('optional_params', [])
                req_str = ', '.join(req) if req else '无'
                opt_str = ', '.join(opt) if opt else '无'
                md = f"### {name}\n\n{description}\n\n- 必需参数：{req_str}\n- 可选参数：{opt_str}"

            # 控件可见性
            need_count = strategy in {'expand', 'paraphrase', 'q_to_a'}
            show_q_a = strategy in {'q_to_a', 'custom'}
            show_label = strategy == 'classify_label'
            
            # 新增：expand 策略使用单选下拉框，其他策略使用多选复选框
            is_expand = (strategy == 'expand')
            show_single = is_expand
            show_multi = not is_expand
            
            # Q 字段名输入框仅在 q_to_a 时显示
            show_q_field_name = (strategy == 'q_to_a')

            # 动态更新字段标签
            source_label = "选定字段（逗号分隔）"
            target_label = "目标字段名（生成结果写入此处）"
            
            if strategy in {'expand', 'paraphrase'}:
                source_label = "original 字段内容来源（选定字段）"
                target_label = "target 字段名（生成结果）"
            elif strategy == 'classify_label':
                source_label = "input 字段内容来源（选定字段）"
                target_label = "label 字段名（标签结果）"

            return (
                md,
                gr.update(visible=need_count), # 不再强制重置 value
                gr.update(visible=show_q_a),
                gr.update(visible=show_q_a),
                gr.update(visible=show_label),
                gr.update(visible=show_multi),  # dataset_fields_box
                gr.update(visible=show_single), # dataset_single_field_dropdown
                gr.update(visible=show_multi, label=source_label),  # selected_fields_input
                gr.update(visible=show_single), # source_field_input
                gr.update(label=target_label),   # target_field_input
                gr.update(visible=show_q_field_name) # q_field_name_input
            )
        except Exception as e:
            self.logger.error(f'策略切换更新失败: {e}')
            # 失败时默认全部隐藏可选控件
            return (
                f"更新策略说明失败: {e}",
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(),
                gr.update(),
                gr.update(),
                gr.update(),
                gr.update(),
                gr.update()
            )
    
    def _start_multiple_tasks(self, task_df, selected_ids=None) -> Tuple[str, Any, Any]:
        """批量开始任务"""
        try:
            selected_tasks = self._extract_selected_ids(task_df, selected_ids)
            if not selected_tasks:
                df = self._get_distill_tasks_df()
                df = self._apply_selection_state(df, selected_ids)
                df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
                return "请先选择要开始的任务", df, df_state
            
            success_count = 0
            failed_count = 0
            results = []
            
            for task_id in selected_tasks:
                try:
                    distill_generator.resume_generation(task_id, None)
                    success_count += 1
                    results.append(f"✅ {task_id}")
                except Exception as e:
                    failed_count += 1
                    results.append(f"❌ {task_id}: {str(e)}")
            
            summary = f"批量启动完成: {success_count}个成功, {failed_count}个失败"
            details = "\n".join(results)
            
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"{summary}\n\n详情:\n{details}", df, df_state
                
        except Exception as e:
            self.logger.error(f'批量启动任务失败: {e}')
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"批量启动失败: {str(e)}", df, df_state

    def _pause_distill_generation(self, task_id: str, selected_ids=None) -> Tuple[str, Any]:
        """暂停蒸馏生成（单任务）"""
        try:
            if not task_id or not task_id.strip():
                return "请选择要暂停的任务", self._get_distill_tasks_df()
            
            # 这里由于distill_generator没有pause方法，我们更新状态
            state_manager.update_state(task_id.strip(), 'status', 'paused')
            
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"任务已暂停: {task_id}", df, df_state
            
        except Exception as e:
            self.logger.error(f'暂停蒸馏生成失败: {e}')
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"暂停失败: {str(e)}", df, df_state

    def _refresh_distill_tasks(self) -> Any:
        """刷新蒸馏任务列表"""
        return self._get_distill_tasks_df()

    def _pause_multiple_tasks(self, task_df, selected_ids=None) -> Tuple[str, Any, Any]:
        """批量暂停任务"""
        try:
            selected_tasks = self._extract_selected_ids(task_df, selected_ids)
            if not selected_tasks:
                df = self._get_distill_tasks_df()
                df = self._apply_selection_state(df, selected_ids)
                df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
                return "请先选择要暂停的任务", df, df_state
            
            success_count = 0
            failed_count = 0
            results = []
            
            for task_id in selected_tasks:
                try:
                    state_manager.update_state(task_id, 'status', 'paused')
                    success_count += 1
                    results.append(f"✅ {task_id}")
                except Exception as e:
                    failed_count += 1
                    results.append(f"❌ {task_id}: {str(e)}")
            
            summary = f"批量暂停完成: {success_count}个成功, {failed_count}个失败"
            details = "\n".join(results)
            
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"{summary}\n\n详情:\n{details}", df, df_state
                
        except Exception as e:
            self.logger.error(f'批量暂停任务失败: {e}')
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"批量暂停失败: {str(e)}", df, df_state

    def _delete_multiple_tasks(self, task_df, delete_files: bool, selected_ids=None) -> Tuple[str, Any, Any]:
        """批量删除任务"""
        try:
            selected_tasks = self._extract_selected_ids(task_df, selected_ids)
            if not selected_tasks:
                df = self._get_distill_tasks_df()
                df = self._apply_selection_state(df, selected_ids)
                df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
                return "请先选择要删除的任务", df, df_state
            
            success_count = 0
            failed_count = 0
            results = []
            
            for task_id in selected_tasks:
                try:
                    state = state_manager.get_task_state(task_id)
                    if not state:
                        failed_count += 1
                        results.append(f"❌ {task_id}: 不存在")
                        continue
                        
                    status = state.get('status')
                    if status == 'running':
                        failed_count += 1
                        results.append(f"❌ {task_id}: 运行中")
                        continue
                    
                    if not state_manager.delete_task(task_id):
                        failed_count += 1
                        results.append(f"❌ {task_id}: 删除状态失败")
                        continue
                    
                    msg = f"✅ {task_id}"
                    
                    if delete_files:
                        task_dir = self.root_dir / 'distilled' / task_id
                        if task_dir.exists():
                            try:
                                shutil.rmtree(task_dir)
                                msg += " (文件已删)"
                            except Exception as e:
                                msg += f" (文件删除失败: {e})"
                        else:
                            msg += " (无文件)"
                    
                    success_count += 1
                    results.append(msg)
                    
                except Exception as e:
                    failed_count += 1
                    results.append(f"❌ {task_id}: {str(e)}")
            
            summary = f"批量删除完成: {success_count}个成功, {failed_count}个失败"
            details = "\n".join(results)
            
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"{summary}\n\n详情:\n{details}", df, df_state
                
        except Exception as e:
            self.logger.error(f'批量删除任务失败: {e}')
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"批量删除失败: {str(e)}", df, df_state

    def _view_distill_report(self, task_id: str) -> str:
        """查看蒸馏报告"""
        try:
            if not task_id.strip():
                return "请选择要查看的任务"
            
            report = distill_generator.get_quality_report(task_id.strip())
            
            if 'error' in report:
                return f"获取报告失败: {report['error']}"
            
            # 格式化报告信息
            metrics = report.get('quality_metrics', {})
            report_text = f"""📋 任务 {task_id} 质量报告

🎯 基本信息:
  策略: {report.get('generation_strategy', 'Unknown')}
  模型: {report.get('model_id', 'Unknown')}

📊 生成统计:
  输入项目数: {metrics.get('total_input_items', 0)}
  生成项目数: {metrics.get('total_generated_items', 0)}
  质量通过数: {metrics.get('quality_passed_items', 0)}
  质量通过率: {metrics.get('quality_pass_rate', 0):.1f}%
  生成成功率: {metrics.get('generation_success_rate', 0):.1f}%
  平均生成倍数: {metrics.get('average_generations_per_input', 0):.1f}

⏰ 生成时间: {report.get('generated_time', 'Unknown')}
"""
            
            return report_text
            
        except Exception as e:
            self.logger.error(f'查看蒸馏报告失败: {e}')
            return f"查看报告失败: {str(e)}"

    def _resume_distill_task(self, task_id: str, model_override: Optional[str], workers: int, temperature: float, max_tokens: int, top_p: float, top_k: int, rate_limit_rps: Optional[float], max_backoff: Optional[float], resume_as_new: bool, selected_ids=None) -> Tuple[str, Any]:
        """恢复蒸馏任务，支持覆盖部分参数（模型/并发/采样）。"""
        try:
            if not task_id or not task_id.strip():
                return "请选择要恢复的任务", self._get_distill_tasks_df()

            overrides = {}
            if model_override:
                overrides['model_id'] = model_override
            if workers and int(workers) > 0:
                overrides['max_workers'] = int(workers)
            if temperature:
                overrides['temperature'] = float(temperature)
            if max_tokens:
                overrides['max_tokens'] = int(max_tokens)
            if top_p:
                overrides['top_p'] = float(top_p)
            if isinstance(top_k, (int, float)) and int(top_k) > 0:
                overrides['top_k'] = int(top_k)
            if rate_limit_rps is not None and str(rate_limit_rps).strip() != "":
                try:
                    overrides['rate_limit_rps'] = float(rate_limit_rps)
                except Exception:
                    pass
            if max_backoff is not None and str(max_backoff).strip() != "":
                try:
                    overrides['max_backoff'] = float(max_backoff)
                except Exception:
                    pass
            if bool(resume_as_new):
                overrides['resume_as_new'] = True

            new_task_id = distill_generator.resume_distill_task(task_id.strip(), overrides or None)
            
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"任务已恢复: {new_task_id}", df, df_state
        except Exception as e:
            self.logger.error(f'恢复任务失败: {e}')
            df = self._get_distill_tasks_df()
            df = self._apply_selection_state(df, selected_ids)
            df_state = df.copy(deep=True) if hasattr(df, 'copy') else df
            return f"恢复失败: {str(e)}", df, df_state

    def _on_distill_model_change(self, model_name: str) -> Dict[str, Any]:
        """根据模型类型动态调整 max_tokens 上限与提示"""
        try:
            if not model_name:
                return gr.update()
            # 获取该模型配置
            all_models = model_manager.get_all_models()
            cfg = all_models.get(model_name) if isinstance(all_models, dict) else None
            mtype = (cfg.get('type') or '').upper() if cfg else ''

            # 设定上限与说明
            if mtype == 'OPENAI':
                max_cap = 8192
                info = "最大Token数（OPENAI 兼容：建议<=8192，超出可能报错）"
            elif mtype in ('VLLM', 'OLLAMA', 'SGLANG'):
                max_cap = 200000
                info = "最大Token数（本地/兼容后端：已放宽至 200000，请按模型上下文限制合理设置）"
            else:
                max_cap = 4000
                info = "最大Token数（未知后端：保持默认上限 4000）"

            return gr.update(maximum=max_cap, info=info)
        except Exception:
            return gr.update()

    def _on_distill_source_change(self, source_file) -> Tuple[Dict[str, Any], str, Dict[str, Any]]:
        """选择源数据文件后，扫描若干行推断字段列表，填充字段复选框并同步文本框"""
        try:
            if source_file is None:
                return gr.update(choices=[], value=[]), "", gr.update(choices=[], value=None)
            
            # 处理 source_file 可能是 dict 或对象的情况
            if isinstance(source_file, dict):
                path = source_file.get('name')
            elif hasattr(source_file, 'name'):
                path = source_file.name
            else:
                path = str(source_file)
                
            if not path or not os.path.exists(path):
                return gr.update(choices=[], value=[]), "", gr.update(choices=[], value=None)
                
            ext = os.path.splitext(path)[1].lower()
            if not ext: # 无扩展名，忽略
                return gr.update(choices=[], value=[]), "", gr.update(choices=[], value=None)
                
            fields = set()
            limit = 100  # 采样前100行
            if ext == '.jsonl':
                with open(path, 'r', encoding='utf-8') as f:
                    for i, line in enumerate(f):
                        if i >= limit:
                            break
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            if isinstance(obj, dict):
                                fields.update(obj.keys())
                        except Exception:
                            continue
            elif ext == '.json':
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        fields.update(data.keys())
                    elif isinstance(data, list):
                        for i, obj in enumerate(data[:limit]):
                            if isinstance(obj, dict):
                                fields.update(obj.keys())
            else:
                # 不支持的格式，静默返回，不报错
                return gr.update(choices=[], value=[]), "", gr.update(choices=[], value=None)
            
            sorted_fields = sorted(list(fields))
            return (
                gr.update(choices=sorted_fields, value=[]), 
                "",
                gr.update(choices=sorted_fields, value=None)
            )
        except Exception as e:
            self.logger.error(f'源文件字段检测失败: {e}')
            return gr.update(choices=[], value=[]), "", gr.update(choices=[], value=None)

    def _sync_selected_fields_text(self, selected_list: list) -> str:
        """将复选框选择同步到文本框（逗号分隔）"""
        try:
            if not selected_list:
                return ""
            return ",".join(selected_list)
        except Exception:
            return ""

    def _sync_source_field_text(self, selected_field: str) -> str:
        """同步单选下拉框到文本框"""
        return selected_field if selected_field else ""
    
    def _select_distill_task(self, evt: gr.SelectData) -> Tuple[Any, ...]:
        """选择蒸馏任务并回填参数"""
        task_id = ""
        # 检查点击的是否是任务ID列 (列索引1)
        # 0: Checkbox, 1: TaskID, 2: Strategy, 3: Model, 4: Status, 5: Progress, 6: StartTime
        if evt.index[1] == 1:  
            task_id = str(evt.value)
        
        if not task_id:
            return (gr.update(),) * 30

        # 获取任务状态
        state = state_manager.get_task_state(task_id)
        if not state:
            return (task_id,) + (gr.update(),) * 29
            
        params = state.get('params', {})
        
        # 构造更新
        return (
            task_id,
            # 左侧配置回填
            gr.update(value=params.get('strategy')),
            gr.update(value=params.get('model_id')),
            gr.update(value=params.get('generation_count')),
            gr.update(value=params.get('temperature')),
            gr.update(value=params.get('max_tokens')),
            gr.update(value=params.get('top_p')),
            gr.update(value=params.get('top_k')),
            gr.update(value=params.get('max_workers')),
            gr.update(value=params.get('fsync_interval')),
            gr.update(value=params.get('checkpoint_interval')),
            gr.update(value=params.get('inflight_multiplier')),
            gr.update(value=params.get('unordered_write')),
            gr.update(value=params.get('rate_limit_rps')),
            gr.update(value=params.get('max_backoff')),
            gr.update(value=params.get('system_prompt')),
            gr.update(value=params.get('q_prompt')),
            gr.update(value=params.get('a_prompt')),
            gr.update(value=params.get('selected_fields')),
            gr.update(value=params.get('q_field_name')),
            gr.update(value=params.get('label_set')),
            gr.update(value=params.get('target_field')),
            # 恢复参数回填 (与原任务保持一致，方便查看)
            gr.update(value=params.get('model_id')),
            gr.update(value=params.get('max_workers')),
            gr.update(value=params.get('temperature')),
            gr.update(value=params.get('max_tokens')),
            gr.update(value=params.get('top_p')),
            gr.update(value=params.get('top_k')),
            gr.update(value=params.get('rate_limit_rps')),
            gr.update(value=params.get('max_backoff'))
        )

    def _extract_selected_ids(self, task_df, selected_ids) -> List[str]:
        """统一提取当前选中的任务ID."""
        try:
            if selected_ids:
                ids = [str(x) for x in selected_ids if str(x).strip()]
                self._log_selection_state("extract-from-state", ids, {'source': 'state'})
                return ids
            if task_df is not None and hasattr(task_df, 'empty') and not task_df.empty:
                selected_rows = task_df[task_df.iloc[:, 0] == True]
                if not selected_rows.empty:
                    ids = [str(x) for x in selected_rows.iloc[:, 1].tolist() if str(x).strip()]
                    self._log_selection_state("extract-from-df", ids, {'source': 'df'})
                    return ids
        except Exception as e:
            self.logger.error(f'提取选中任务失败: {e}')
        return []

    def _apply_selection_state(self, df, selected_ids):
        """根据 State 中的任务ID恢复复选框状态."""
        try:
            if df is None or not hasattr(df, 'empty') or df.empty:
                return df
            selected_set = {str(x) for x in (selected_ids or []) if str(x).strip()}
            if not selected_set:
                return df
            updated_df = df.copy(deep=True)
            updated_df.iloc[:, 0] = updated_df.iloc[:, 1].apply(lambda x: str(x) in selected_set)
            true_count = int(updated_df.iloc[:, 0].sum()) if not updated_df.empty else 0
            self._log_selection_state("apply-selection", selected_set, {'true_count': true_count})
            return updated_df
        except Exception as e:
            self.logger.error(f'同步复选框状态失败: {e}')
            return df

    def _log_selection_state(self, stage: str, selected_ids, extra: Optional[Dict[str, Any]] = None):
        """集中记录复选框选择状态，便于排查刷新后选择消失的问题."""
        try:
            ids = [str(x) for x in (selected_ids or []) if str(x).strip()]
            preview = ids[:5]
            info = extra or {}
            info_text = " ".join(f"{k}={v}" for k, v in info.items()) if info else ""
            # 临时改为 INFO 级别以便调试
            self.logger.info(f"[DistillTab] {stage} selected_count={len(ids)} sample={preview} {info_text}")
        except Exception as e:
            self.logger.error(f'记录选择状态日志失败: {e}')

    def _get_distill_tasks_df(self) -> Any:
        """获取蒸馏任务列表数据框"""
        try:
            # 获取所有蒸馏任务（按任务类型 DISTILL 过滤）
            tasks = state_manager.list_tasks(task_type=TaskType.DISTILL)
            
            if not tasks:
                return pd.DataFrame(columns=["选择", "任务ID", "策略", "模型", "状态", "进度", "开始时间"])
            
            # 构建数据框
            rows = []
            for task in tasks:
                task_id = task.get('task_id', '')
                params = task.get('params', {})
                strategy = params.get('strategy', '')
                model_id = params.get('model_id', '')
                status = task.get('status', 'unknown')
                progress = task.get('progress', 0)
                start_time = task.get('start_time', '')
                
                # 状态中文映射
                status_map = {
                    'pending': '等待中',
                    'running': '生成中',
                    'paused': '已暂停',
                    'completed': '已完成',
                    'failed': '失败'
                }
                
                status_cn = status_map.get(status, status)
                progress_str = f"{progress:.1f}%" if isinstance(progress, (int, float)) else "0%"
                
                # 策略中文映射
                strategy_map = {
                    'expand': '数据扩充',
                    'enhance': '内容增强',
                    'paraphrase': '文本改写',
                    'classify_label': '分类标注',
                    'q_to_a': '从Q生A',
                    'custom': '自定义'
                }
                
                strategy_cn = strategy_map.get(strategy, strategy)
                
                # 格式化开始时间
                if start_time:
                    try:
                        dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        start_time_str = dt.strftime('%m-%d %H:%M')
                    except:
                        start_time_str = start_time[:16] if len(start_time) > 16 else start_time
                else:
                    start_time_str = ""
                
                rows.append([
                    False, # 默认不选中
                    task_id,
                    strategy_cn,
                    model_id,
                    status_cn,
                    progress_str,
                    start_time_str
                ])
            
            return pd.DataFrame(rows, columns=["选择", "任务ID", "策略", "模型", "状态", "进度", "开始时间"])
            
        except Exception as e:
            self.logger.error(f'获取蒸馏任务列表失败: {e}')
            return pd.DataFrame(columns=["选择", "任务ID", "策略", "模型", "状态", "进度", "开始时间"])

    def _refresh_distill_tasks_logic(self, current_df=None):
        """刷新蒸馏任务列表，同时保留用户的选择状态"""
        try:
            selected_ids = set()
            if current_df is not None and not current_df.empty:
                try:
                    # 假设第0列是选择框(bool)，第1列是任务ID
                    # 筛选出被选中的行
                    selected_rows = current_df[current_df.iloc[:, 0] == True]
                    if not selected_rows.empty:
                        selected_ids = set(selected_rows.iloc[:, 1].tolist())
                except Exception:
                    pass
            
            # 获取最新的任务列表数据
            new_df = self._get_distill_tasks_df()
            
            # 如果有之前的选择，重新应用到新数据上
            if selected_ids and not new_df.empty:
                # 使用 apply 函数更新第一列
                # 注意：这里假设第1列是任务ID
                new_df.iloc[:, 0] = new_df.iloc[:, 1].apply(lambda x: x in selected_ids)
                
            return new_df
        except Exception as e:
            self.logger.error(f'刷新蒸馏任务列表失败: {e}')
            return self._get_distill_tasks_df()

    def _refresh_distill_tasks_logic_with_state(self, selected_ids):
        """刷新蒸馏任务列表，并应用 State 中的选择状态"""
        try:
            self._log_selection_state("refresh-with-state-input", selected_ids)
            selected_set = {str(x) for x in (selected_ids or [])}
            # 获取最新的任务列表数据
            new_df = self._get_distill_tasks_df()
            
            # 如果有之前的选择，重新应用到新数据上
            if selected_set and not new_df.empty:
                # 使用 apply 函数更新第一列
                # 注意：这里假设第1列是任务ID
                new_df.iloc[:, 0] = new_df.iloc[:, 1].apply(lambda x: str(x) in selected_set)
            true_count = int(new_df.iloc[:, 0].sum()) if hasattr(new_df, 'iloc') and not new_df.empty else 0
            self.logger.debug(f"[DistillTab] refresh-with-state-output checkbox_true={true_count} rows={len(new_df)}")
            return new_df
        except Exception as e:
            self.logger.error(f'刷新蒸馏任务列表失败: {e}')
            return self._get_distill_tasks_df()

def create_distill_tab(launcher):
    manager = DistillTabManager(launcher)
    manager.create_tab()
    return manager
