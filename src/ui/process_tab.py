import os
import time
from datetime import datetime
import re
import json
from pathlib import Path
import gradio as gr
from typing import Dict, Any, Tuple, List
from ..dependencies import pd
from ..data_cleaner import data_cleaner
from ..universal_field_extractor import get_field_names_universal, extract_fields_universal

class ProcessTabManager:
    def __init__(self, launcher):
        self.launcher = launcher
        self.logger = launcher.logger
        self.format_converter = launcher.format_converter
        self.field_extractor = launcher.field_extractor
        self.data_merger = launcher.data_merger
        self.data_cleaner = data_cleaner
        
        # 新增：合并文件路径列表
        self.merge_file_paths = []

    def _start_format_convert(self, source_file, target_format: str, output_dir: str) -> str:
        """开始格式转换"""
        try:
            if source_file is None:
                return "❌ 请选择源文件"
            
            source_path = source_file.name
            if not os.path.exists(source_path):
                return "❌ 源文件不存在"
            
            # 创建输出目录
            os.makedirs(output_dir, exist_ok=True)
            
            # 创建转换任务
            task_id = self.format_converter.add_convert_task(
                source_path=source_path,
                target_format=target_format,
                output_dir=output_dir,
                use_subdirectory=False
            )
            
            # 启动任务
            success = self.format_converter.start_task(task_id)
            if not success:
                return f"❌ 启动转换任务失败，任务ID: {task_id}"
            
            return f"✅ 转换任务已启动！\n任务ID: {task_id}\n文件: {os.path.basename(source_path)}\n目标格式: {target_format.upper()}"
            
        except Exception as e:
            self.logger.error(f'启动格式转换失败: {e}')
            return f"❌ 启动转换失败: {str(e)}"

    def _get_convert_tasks_df(self) -> pd.DataFrame:
        """获取转换任务列表"""
        try:
            tasks = self.format_converter.list_tasks()
            if not tasks:
                return pd.DataFrame(columns=["任务ID", "源文件", "目标格式", "状态", "进度", "输出文件"])
            
            task_data = []
            for task in tasks:
                task_id = task.get('task_id', 'N/A')
                source_path = task.get('source_path', '')
                source_file = os.path.basename(source_path) if source_path else 'N/A'
                target_format = task.get('target_format', 'N/A').upper()
                status = task.get('status', 'unknown')
                progress = f"{task.get('progress', 0)}%"
                
                # 生成输出文件名
                if status == 'completed':
                    source_stem = Path(source_path).stem if source_path else 'unknown'
                    source_ext = Path(source_path).suffix.replace('.', '') if source_path else ''
                    target_ext = 'md' if target_format.lower() == 'markdown' else target_format.lower()
                    output_filename = f"{source_stem}_{source_ext}2{target_ext}.{target_ext}"
                else:
                    output_filename = "转换中..."
                
                task_data.append([
                    task_id[:15] + "..." if len(task_id) > 15 else task_id,
                    source_file,
                    target_format,
                    status,
                    progress,
                    output_filename
                ])
            
            return pd.DataFrame(task_data, columns=["任务ID", "源文件", "目标格式", "状态", "进度", "输出文件"])
            
        except Exception as e:
            self.logger.error(f'获取转换任务列表失败: {e}')
            return pd.DataFrame(columns=["任务ID", "源文件", "目标格式", "状态", "进度", "输出文件"])

    def _select_convert_task(self, evt: gr.SelectData) -> str:
        """选择转换任务"""
        try:
            row_data = evt.row_value
            if row_data and len(row_data) >= 1:
                return row_data[0]  # 返回任务ID
            return ""
        except Exception as e:
            self.logger.error(f'选择转换任务失败: {e}')
            return ""

    def _view_convert_result(self, task_id: str) -> str:
        """查看转换结果详情"""
        try:
            if not task_id.strip():
                return "❌ 请选择要查看的任务"
            
            # 获取完整任务ID（如果被截断）
            tasks = self.format_converter.list_tasks()
            full_task_id = None
            for task in tasks:
                if task.get('task_id', '').startswith(task_id.replace('...', '')):
                    full_task_id = task.get('task_id')
                    break
            
            if not full_task_id:
                return "❌ 任务不存在"
                
            task = self.format_converter.get_task_progress(full_task_id)
            if not task:
                return "❌ 无法获取任务信息"
                
            info = [
                f"任务ID: {full_task_id}",
                f"源文件: {task.get('source_path', 'N/A')}",
                f"目标格式: {task.get('target_format', 'N/A')}",
                f"状态: {task.get('status', 'unknown')}",
                f"进度: {task.get('progress', 0)}%",
                f"开始时间: {task.get('start_time', 'N/A')}",
                f"结束时间: {task.get('end_time', 'N/A')}",
                f"错误信息: {task.get('error_msg', '无')}"
            ]
            
            return "\n".join(info)
            
        except Exception as e:
            self.logger.error(f'查看转换结果失败: {e}')
            return f"❌ 查看结果失败: {str(e)}"

    def _preview_extract_fields(self, file_obj) -> gr.CheckboxGroup:
        """获取文件字段列表"""
        try:
            if file_obj is None:
                return gr.CheckboxGroup(choices=[], value=[])
            
            file_path = file_obj.name
            fields = get_field_names_universal(file_path)
            
            return gr.CheckboxGroup(choices=fields, value=[])
        except Exception as e:
            self.logger.error(f'获取字段失败: {e}')
            return gr.CheckboxGroup(choices=[], value=[])

    def _reset_field_selection(self):
        """重置字段选择"""
        return gr.update(value=[]), pd.DataFrame(columns=["原字段", "新字段"])

    def _update_field_mapping(self, selected_fields):
        """更新字段映射表"""
        if not selected_fields:
            return gr.update(value=pd.DataFrame(columns=["原字段", "新字段"]), visible=False)
        
        data = [[field, field] for field in selected_fields]
        df = pd.DataFrame(data, columns=["原字段", "新字段"])
        return gr.update(value=df, visible=True)

    def _start_field_extract(self, source_file, fields: List[str], output_dir: str) -> str:
        """开始字段提取"""
        try:
            if source_file is None:
                return "❌ 请选择源文件"

            if not fields:
                return "❌ 请选择要提取的字段"

            source_path = source_file.name
            if not os.path.exists(source_path):
                return "❌ 源文件不存在"

            # 使用通用字段提取器
            self.logger.info(f"开始字段提取: {fields}")
            result_path = extract_fields_universal(
                source_path=source_path,
                fields=fields,
                output_dir=output_dir or str(self.launcher.root_dir / 'processed')
            )

            if result_path and os.path.exists(result_path):
                file_size = os.path.getsize(result_path)
                return f"✅ 字段提取完成！\n提取字段: {', '.join(fields)}\n输出文件: {result_path}\n文件大小: {file_size:,} 字节"
            else:
                return "❌ 字段提取失败"
                
        except Exception as e:
            self.logger.error(f'字段提取失败: {e}')
            return f"❌ 字段提取失败: {str(e)}"
    
    def _add_merge_file(self, file_obj, current_data) -> Tuple[None, Any]:
        """添加合并文件"""
        try:
            if file_obj is None:
                return None, current_data
            
            file_path = file_obj.name
            
            # 允许添加重复文件（支持不同目录同名文件，或有意重复合并）
            # if file_path in self.merge_file_paths:
            #     return None, current_data
                
            self.merge_file_paths.append(file_path)
            
            # 获取文件信息
            file_name = os.path.basename(file_path)
            file_size = self._format_size(os.path.getsize(file_path))
            
            # 更新DataFrame
            new_row = [file_name, file_path, file_size]
            
            if isinstance(current_data, pd.DataFrame):
                # 如果是DataFrame，添加新行
                new_df = pd.concat([current_data, pd.DataFrame([new_row], columns=["文件名", "路径", "大小"])], ignore_index=True)
                return None, new_df
            else:
                # 如果是列表（初始状态），创建新DataFrame
                if not current_data:
                    return None, pd.DataFrame([new_row], columns=["文件名", "路径", "大小"])
                else:
                    # 尝试转换现有数据
                    try:
                        df = pd.DataFrame(current_data, columns=["文件名", "路径", "大小"])
                        new_df = pd.concat([df, pd.DataFrame([new_row], columns=["文件名", "路径", "大小"])], ignore_index=True)
                        return None, new_df
                    except:
                        return None, pd.DataFrame([new_row], columns=["文件名", "路径", "大小"])
                        
        except Exception as e:
            self.logger.error(f'添加合并文件失败: {e}')
            return None, current_data

    def _delete_merge_file(self, evt: gr.SelectData, current_data) -> Any:
        """删除选中的合并文件"""
        try:
            if not current_data.empty and evt.index[0] < len(current_data):
                # 获取要删除的文件路径
                row_index = evt.index[0]
                file_path = current_data.iloc[row_index]["路径"]
                
                # 从列表中移除
                if file_path in self.merge_file_paths:
                    self.merge_file_paths.remove(file_path)
                
                # 从DataFrame中移除
                new_df = current_data.drop(row_index).reset_index(drop=True)
                return new_df
            return current_data
        except Exception as e:
            self.logger.error(f'删除合并文件失败: {e}')
            return current_data

    def _clear_merge_files(self) -> Any:
        """清空合并文件列表"""
        self.merge_file_paths = []
        return pd.DataFrame(columns=["文件名", "路径", "大小"])

    def _format_size(self, size_bytes: int) -> str:
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}TB"

    def _start_merge(self, output_filename: str, output_dir: str, merge_mode: str = "merge") -> str:
        """开始合并任务"""
        try:
            if not self.merge_file_paths:
                return "❌ 请先添加要合并的文件"
            
            if len(self.merge_file_paths) < 2:
                return "❌ 至少需要两个文件才能合并"
            
            if not output_filename:
                return "❌ 请输入输出文件名"
            
            # 生成任务ID和时间戳
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            task_id = f"merge_{int(time.time())}"
            
            # 创建任务专属目录 (类似格式转换)
            # 如果用户指定了 output_dir，我们在其下创建一个带时间戳的子目录
            # 这样可以避免文件覆盖，并保持目录结构清晰
            task_dir_name = f"merge-{timestamp}-{task_id[-6:]}"
            task_dir = os.path.join(output_dir, task_dir_name)
            os.makedirs(task_dir, exist_ok=True)
            
            # 构建输出路径
            if not output_filename.endswith('.jsonl'):
                output_filename += '.jsonl'
            target_path = os.path.join(task_dir, output_filename)
            
            # 构造参数
            params = {
                "task_id": task_id,
                "input_paths": self.merge_file_paths,
                "merge_mode": merge_mode,
                "target_path": target_path,
                "dedup_field": None,
                "dedup_strategy": "first"
            }
            
            # 调用合并
            result_path = self.data_merger.merge_datasets(params)
            
            if result_path:
                return f"✅ 合并成功！\n输出文件: {result_path}"
            else:
                return "❌ 合并失败"
                
        except Exception as e:
            self.logger.error(f'合并失败: {e}')
            return f"❌ 合并失败: {str(e)}"
    
    def _start_clean(self, source_file, operations: List[str], empty_fields: str,
                    empty_mode: str,
                    sensitive_words: str, sensitive_action: str, sensitive_replacement: str,
                    sensitive_fields: str, sensitive_exclude_fields: str, sensitive_field_policies: str,
                    sensitive_use_regex: bool, sensitive_case_sensitive: bool,
                    pii_enable: List[str], pii_repl_default: str, pii_repl_map: str,
                    normalize_modes: List[str]) -> str:
        """开始数据清洗"""
        try:
            if source_file is None:
                return "❌ 请选择源文件"
            if not operations:
                return "❌ 请选择至少一个清洗操作"
            
            source_path = source_file.name
            if not os.path.exists(source_path):
                return "❌ 源文件不存在"
            
            params: Dict[str, Any] = {
                'source_path': source_path,
                'operations': operations
            }
            
            # 去空字段与策略
            if 'remove_empty' in operations:
                if empty_fields and empty_fields.strip():
                    params['remove_empty_fields'] = [f.strip() for f in empty_fields.split(',') if f.strip()]
                if empty_mode:
                    params['empty_mode'] = empty_mode

            # 敏感词
            if 'filter_sensitive' in operations:
                if sensitive_words and sensitive_words.strip():
                    params['sensitive_words'] = [w.strip() for w in sensitive_words.split(',') if w.strip()]
                params['sensitive_action'] = sensitive_action or 'drop_record'
                if sensitive_action == 'replace_word':
                    params['sensitive_replacement'] = sensitive_replacement or '***'
                if sensitive_fields and sensitive_fields.strip():
                    params['sensitive_fields'] = [f.strip() for f in sensitive_fields.split(',') if f.strip()]
                if sensitive_exclude_fields and sensitive_exclude_fields.strip():
                    params['sensitive_exclude_fields'] = [f.strip() for f in sensitive_exclude_fields.split(',') if f.strip()]
                if sensitive_field_policies and sensitive_field_policies.strip():
                    mapping = {}
                    for seg in sensitive_field_policies.split(','):
                        seg = seg.strip()
                        if not seg:
                            continue
                        parts = seg.split(':')
                        if len(parts) >= 2:
                            field = parts[0].strip()
                            act = parts[1].strip()
                            repl = None
                            if len(parts) >= 3:
                                repl = ':'.join(parts[2:]).strip()
                            if field:
                                mapping[field] = (act, repl)
                    if mapping:
                        params['sensitive_field_policies_parsed'] = mapping
                        params['sensitive_field_policies'] = sensitive_field_policies
                if sensitive_use_regex:
                    params['sensitive_use_regex'] = True
                if sensitive_case_sensitive:
                    params['sensitive_case_sensitive'] = True

            # PII 脱敏
            if 'pii_desensitize' in operations:
                if pii_enable:
                    params['pii_enable'] = pii_enable
                repl_map: Dict[str, str] = {}
                if pii_repl_default and pii_repl_default.strip():
                    repl_map['default'] = pii_repl_default.strip()
                if pii_repl_map and pii_repl_map.strip():
                    parts = [p.strip() for p in pii_repl_map.split(',') if p.strip()]
                    for p in parts:
                        if ':' in p:
                            k, v = p.split(':', 1)
                            if k.strip() and v.strip():
                                repl_map[k.strip()] = v.strip()
                if repl_map:
                    params['pii_replacements'] = repl_map

            # 文本标准化
            if 'normalize_text' in operations and normalize_modes:
                params['normalize_modes'] = normalize_modes

            # 调用清洗器
            cleaner = self.data_cleaner.DataCleaner()
            task_id = cleaner.start_clean(params)
            
            return f"✅ 清洗任务已启动！\n任务ID: {task_id}\n请查看控制台日志获取详细进度。"
                
        except Exception as e:
            self.logger.error(f'清洗失败: {e}')
            return f"❌ 清洗失败: {str(e)}"

    def _preview_sensitive_words(self, text, sensitive_words, action, replacement,
                               sensitive_fields, sensitive_exclude_fields, field_policies,
                               use_regex, case_sensitive) -> str:
        """预览敏感词清洗效果"""
        try:
            if not text or not text.strip():
                return "⚠️ 请输入要预览的文本"
            
            cleaner = self.data_cleaner.DataCleaner()
            words = [w.strip() for w in (sensitive_words or '').split(',') if w.strip()] or cleaner.default_sensitive_words
            
            data = {'preview': text}
            allowed = [f.strip() for f in sensitive_fields.split(',') if f.strip()] if sensitive_fields else None
            exclude = [f.strip() for f in sensitive_exclude_fields.split(',') if f.strip()] if sensitive_exclude_fields else None
            
            mapping = {}
            if field_policies and field_policies.strip():
                for seg in field_policies.split(','):
                    seg = seg.strip()
                    if not seg:
                        continue
                    parts = seg.split(':')
                    if len(parts) >= 2:
                        fld = parts[0].strip()
                        act = parts[1].strip()
                        repl = None
                        if len(parts) >= 3:
                            repl = ':'.join(parts[2:]).strip()
                        mapping[fld] = (act, repl)
            
            # 统计容器模拟
            stats = {'sensitive_detail': {'field_hits': {}, 'word_hits': {}}}
            
            # 调用 DataCleaner 的内部方法 _process_sensitive
            # 注意：这里依赖 DataCleaner 的内部实现，如果 DataCleaner 接口变更可能需要调整
            hit, modified, dropped = cleaner._process_sensitive(
                data,
                words,
                action or 'drop_record',
                replacement or '***',
                allowed,
                exclude,
                mapping,
                bool(use_regex),
                bool(case_sensitive),
                stats
            )
            
            if dropped:
                return "🛑 结果: 记录将被丢弃 (drop_record 触发)\n\n原文本:\n" + text
            
            new_text = data.get('preview', '')
            if not hit:
                return "✅ 未命中任何敏感词\n\n原文本:\n" + text
            
            detail = stats['sensitive_detail']
            return (
                "🎯 命中敏感词预览\n" +
                f"动作: {action}\n\n" +
                "原文本:\n" + text + "\n\n" +
                "处理后:\n" + new_text + "\n\n" +
                "字段命中统计:" + json.dumps(detail['field_hits'], ensure_ascii=False) + "\n" +
                "词条命中统计:" + json.dumps(detail['word_hits'], ensure_ascii=False)
            )
            
        except Exception as e:
            return f"预览失败: {str(e)}"

    def create_tab(self):
        """创建数据加工标签页"""
        gr.Markdown("## 数据加工管理")
        gr.Markdown("支持格式转换、字段提取、数据合并、数据清洗等操作")
        
        # 功能选择标签
        with gr.Tabs():
            # 格式转换子标签
            with gr.TabItem("格式转换"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 转换配置")
                        
                        convert_source = gr.File(
                            label="源文件",
                            file_types=[".jsonl", ".csv", ".xlsx", ".json", ".xml", ".md", ".markdown"]
                        )
                        
                        convert_target = gr.Dropdown(
                            choices=["jsonl", "csv", "xlsx", "json", "xml", "markdown"],
                            value=self.launcher.config_manager.get_config("ui_state.process.convert_target", "jsonl"),
                            label="目标格式",
                            info="选择转换后的格式"
                        )
                        convert_target.change(lambda x: self.launcher.config_manager.update_config("ui_state.process.convert_target", x), inputs=[convert_target])
                        
                        convert_output_dir = gr.Textbox(
                            label="输出目录",
                            value=self.launcher.config_manager.get_config("ui_state.process.convert_output_dir", str(self.launcher.root_dir / "processed")),
                            info="转换结果保存路径"
                        )
                        convert_output_dir.change(lambda x: self.launcher.config_manager.update_config("ui_state.process.convert_output_dir", x), inputs=[convert_output_dir])
                        
                        convert_btn = gr.Button("开始转换", variant="primary")
                        refresh_convert_btn = gr.Button("刷新任务", size="sm")
                    
                    with gr.Column(scale=2):
                        gr.Markdown("### 转换任务列表")
                        
                        convert_task_list = gr.Dataframe(
                            headers=["任务ID", "源文件", "目标格式", "状态", "进度", "输出文件"],
                            datatype=["str", "str", "str", "str", "str", "str"],
                            label="",
                            interactive=False,
                            wrap=True,
                            elem_classes="convert-task-table"
                        )
                        
                        # 定时刷新任务列表 (每2秒刷新一次)
                        refresh_timer = gr.Timer(value=2)
                        refresh_timer.tick(
                            fn=self._get_convert_tasks_df,
                            outputs=[convert_task_list]
                        )
                        
                        with gr.Row():
                            selected_convert_task = gr.Textbox(
                                label="选中任务",
                                placeholder="点击任务行选择",
                                interactive=False,
                                scale=2
                            )
                            
                            view_convert_result_btn = gr.Button("查看结果", size="sm", scale=1)
                
                # 任务详情显示
                with gr.Row():
                    convert_detail_status = gr.Textbox(
                        label="任务详情",
                        lines=5,
                        interactive=False,
                        show_copy_button=True,
                        info="显示选中任务的详细信息"
                    )
                    
                    convert_status = gr.Textbox(
                        label="转换状态",
                        lines=5,
                        interactive=False,
                        show_copy_button=True,
                        info="显示转换任务的状态信息"
                    )
            
            # 字段提取子标签
            with gr.TabItem("字段提取"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 提取配置")
                        
                        extract_source = gr.File(
                            label="源文件",
                            file_types=[".jsonl", ".csv", ".xlsx", ".json", ".xml", ".md", ".markdown"]
                        )
                        
                        extract_preview_btn = gr.Button("预览字段", variant="secondary")
                        
                        extract_fields = gr.CheckboxGroup(
                            label="选择字段",
                            choices=[],
                            value=[],  # 确保初始值为空列表
                            info="选择要提取的字段"
                        )
                        
                        # 字段重命名区域
                        gr.Markdown("### 字段重命名")
                        field_mapping_df = gr.Dataframe(
                            headers=["原字段名", "新字段名"],
                            datatype=["str", "str"],
                            row_count=0,
                            col_count=(2, "fixed"),
                            interactive=True,
                            label="字段映射表",
                            visible=False  # 初始隐藏
                        )
                        
                        extract_output_dir = gr.Textbox(
                            label="输出目录",
                            value=self.launcher.config_manager.get_config("ui_state.process.extract_output_dir", str(self.launcher.root_dir / "processed")),
                            info="提取结果保存路径"
                        )
                        extract_output_dir.change(lambda x: self.launcher.config_manager.update_config("ui_state.process.extract_output_dir", x), inputs=[extract_output_dir])
                        
                        extract_btn = gr.Button("开始提取", variant="primary")
                    
                    with gr.Column(scale=1):
                        extract_status = gr.Textbox(
                            label="提取状态",
                            lines=8,
                            interactive=False,
                            show_copy_button=True
                        )
            
            # 数据合并子标签
            with gr.TabItem("数据合并"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 合并配置")
                        
                        # 文件选择和管理
                        gr.Markdown("#### 1. 选择合并文件")
                        
                        with gr.Row():
                            with gr.Column(scale=3):
                                merge_file_upload = gr.File(
                                    label="选择文件",
                                    file_types=[".jsonl", ".csv", ".xlsx", ".json", ".md", ".markdown"],
                                    file_count="single"
                                )
                            with gr.Column(scale=1):
                                add_file_btn = gr.Button("添加到列表", variant="primary")
                        
                        gr.Markdown("#### 2. 待合并文件列表")
                        
                        # 使用Dataframe替代Textbox，支持更直观的显示和操作
                        merge_file_list = gr.Dataframe(
                            headers=["文件名", "路径", "大小"],
                            datatype=["str", "str", "str"],
                            label="",
                            interactive=False,
                            wrap=True,
                            value=[]
                        )
                        
                        with gr.Row():
                            delete_file_btn = gr.Button("删除选中", size="sm", variant="secondary")
                            clear_files_btn = gr.Button("清空列表", size="sm", variant="stop")
                        
                        selected_merge_index = gr.Number(
                            value=-1,
                            label="选中索引",
                            visible=False
                        )
                        
                        # 合并选项
                        gr.Markdown("#### 3. 合并选项")
                        merge_mode = gr.Radio(
                            choices=[("均衡打散合并", "merge"), ("追加合并", "append")],
                            value=self.launcher.config_manager.get_config("ui_state.process.merge_mode", "merge"),
                            label="合并模式",
                            info="均衡打散合并: 所有文件数据混合打散后合并; 追加合并: 按文件顺序依次追加"
                        )
                        merge_mode.change(lambda x: self.launcher.config_manager.update_config("ui_state.process.merge_mode", x), inputs=[merge_mode])
                        
                        merge_output_filename = gr.Textbox(
                            label="输出文件名",
                            value=self.launcher.config_manager.get_config("ui_state.process.merge_output_filename", "merged_dataset.jsonl"),
                            info="合并后的文件名"
                        )
                        merge_output_filename.change(lambda x: self.launcher.config_manager.update_config("ui_state.process.merge_output_filename", x), inputs=[merge_output_filename])

                        merge_output_dir = gr.Textbox(
                            label="输出目录",
                            value=self.launcher.config_manager.get_config("ui_state.process.merge_output_dir", str(self.launcher.root_dir / "processed")),
                            info="合并结果保存路径"
                        )
                        merge_output_dir.change(lambda x: self.launcher.config_manager.update_config("ui_state.process.merge_output_dir", x), inputs=[merge_output_dir])
                        
                        merge_btn = gr.Button("开始合并", variant="primary")
                    
                    with gr.Column(scale=1):
                        merge_status = gr.Textbox(
                            label="合并状态",
                            lines=10,
                            interactive=False,
                            show_copy_button=True
                        )
            
            # 数据清洗子标签
            with gr.TabItem("数据清洗"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 清洗配置")
                        
                        clean_source = gr.File(
                            label="源文件",
                            file_types=[".jsonl", ".csv", ".xlsx", ".json"]
                        )
                        
                        clean_operations = gr.CheckboxGroup(
                            choices=[
                                ("去除空值", "remove_empty"),
                                ("敏感词处理", "filter_sensitive"),
                                ("PII脱敏", "pii_desensitize"),
                                ("文本标准化", "normalize_text")
                            ],
                            value=self.launcher.config_manager.get_config("ui_state.process.clean_operations", []),
                            label="清洗操作",
                            info="选择要执行的清洗操作 (支持多选)"
                        )
                        clean_operations.change(lambda x: self.launcher.config_manager.update_config("ui_state.process.clean_operations", x), inputs=[clean_operations])
                        
                        clean_empty_fields = gr.Textbox(
                            label="去空字段（可选）",
                            placeholder="例如：question,answer",
                            info="指定检查空值的字段，逗号分隔",
                            value=self.launcher.config_manager.get_config("ui_state.process.clean_empty_fields", "")
                        )
                        clean_empty_fields.change(lambda x: self.launcher.config_manager.update_config("ui_state.process.clean_empty_fields", x), inputs=[clean_empty_fields])

                        clean_empty_mode = gr.Radio(
                            choices=["any", "all"],
                            value="any",
                            label="空值策略",
                            info="any: 任一字段为空即丢弃; all: 所有指定字段都为空才丢弃"
                        )
                        
                        clean_sensitive_words = gr.Textbox(
                            label="敏感词列表（可选）",
                            placeholder="例如：密码,身份证,手机号",
                            info="指定敏感词，逗号分隔",
                            value=self.launcher.config_manager.get_config("ui_state.process.clean_sensitive_words", "")
                        )
                        clean_sensitive_words.change(lambda x: self.launcher.config_manager.update_config("ui_state.process.clean_sensitive_words", x), inputs=[clean_sensitive_words])

                        clean_sensitive_fields = gr.Textbox(
                            label="敏感词扫描字段（可选）",
                            placeholder="例如：instruction,output",
                            info="仅对这些字段执行敏感词处理；留空则扫描所有字符串字段"
                        )

                        clean_sensitive_action = gr.Radio(
                            choices=["drop_record", "remove_word", "replace_word"],
                            value="drop_record",
                            label="敏感词动作",
                            info="drop_record: 丢弃整条记录; remove_word: 删除词本身; replace_word: 替换为指定内容"
                        )

                        clean_sensitive_replacement = gr.Textbox(
                            label="敏感词替换文本（当选择 replace_word 时）",
                            value="***",
                            placeholder="例如：***",
                            info="仅在敏感词动作=replace_word 时使用"
                        )

                        clean_sensitive_exclude_fields = gr.Textbox(
                            label="敏感词排除字段（可选）",
                            placeholder="例如：meta,source",
                            info="这些字段将忽略敏感词处理"
                        )

                        clean_sensitive_field_policies = gr.Textbox(
                            label="字段级策略 (可选)",
                            placeholder="格式: 字段:动作[:替换]; 例 instruction:replace_word:@@@,note:remove_word",
                            info="覆盖全局动作，可选替换文本。动作=drop_record/remove_word/replace_word"
                        )

                        clean_sensitive_use_regex = gr.Checkbox(
                            label="敏感词使用正则模式",
                            value=False
                        )

                        clean_sensitive_case_sensitive = gr.Checkbox(
                            label="大小写敏感匹配",
                            value=False
                        )

                        with gr.Accordion("敏感词规则说明", open=False):
                            gr.Markdown(
                                """
**匹配模式说明**

1. 普通模式：按提供的词条逐一精确子串匹配（默认忽略大小写，除非勾选大小写敏感）。
2. 正则模式：勾选“敏感词使用正则模式”后，列表中每一项视为一个正则表达式，支持分组与量词。
3. 字段白名单 / 排除：
   - “敏感词扫描字段”填写后，仅这些字段会被检测。
   - “敏感词排除字段”优先生效，可排除部分字段。
4. 字段级策略优先级：字段策略 > 全局动作。格式: `字段:动作[:替换]`；动作支持 `drop_record|remove_word|replace_word`。
5. 统计信息：清洗完成后报告中 `sensitive_detail.field_hits` 记录各字段命中次数，`word_hits` 记录各词命中次数。`unused_parameters` 可帮助确认未生效的多余参数。
6. Drop Record 提前：一旦某字段策略或全局动作触发 `drop_record` 且匹配命中，该记录立即丢弃，不再继续替换其它字段。

**示例**
```
敏感词列表: 密钥,密码
字段级策略: instruction:remove_word,note:replace_word:[SENSITIVE]
```
表示 instruction 删除词本身，note 用 [SENSITIVE] 替换。
                                """
                            )

                        gr.Markdown("### 敏感词试运行 (不落地文件)")
                        sensitive_preview_text = gr.Textbox(
                            label="预览输入文本",
                            placeholder="在这里粘贴一段文本，点击下方按钮查看处理效果",
                            lines=3
                        )
                        sensitive_preview_btn = gr.Button("试运行预览", size="sm")
                        sensitive_preview_result = gr.Textbox(
                            label="预览结果",
                            lines=5,
                            interactive=False,
                            show_copy_button=True
                        )

                        pii_enable = gr.CheckboxGroup(
                            choices=["id_card", "phone", "email", "bank_card", "ip", "passport"],
                            label="启用的 PII 类型",
                            info="选择需要匹配并脱敏的个人信息类型"
                        )

                        pii_repl_default = gr.Textbox(
                            label="PII 默认替换文本（可选）",
                            placeholder="例如：<PII>",
                            info="未为分类单独指定时使用"
                        )

                        pii_repl_map = gr.Textbox(
                            label="PII 分类替换 (可选)",
                            placeholder="格式: 类型:替换文本, 例如 id_card:<ID>,phone:<TEL>",
                            info="按逗号分隔的 键:值 列表"
                        )

                        normalize_modes = gr.CheckboxGroup(
                            choices=["unicode_nfc", "fullwidth", "lowercase", "collapse_newlines"],
                            label="文本标准化模式",
                            info="多选组合，对文本做统一处理（点击下方说明获取详细差异）"
                        )

                        with gr.Accordion("文本标准化说明", open=False):
                            gr.Markdown(
                                """
**各选项含义与场景**

1. `unicode_nfc` 统一等价字符的内部表示（NFC 规范化）。
    - 解决：同样显示的字符因为分解/组合形式不同导致匹配/去重失败。
    - 例：`e + ́` -> `é`。

2. `fullwidth` 全角转半角（只作用于字母 / 数字 / 常见英文标点）。
    - 解决：输入法全角模式 / 网页复制导致的ＡＢＣ１２３，避免匹配失败。
    - 例：`ＡＢＣ１２３，．／` -> `ABC123,./`。

3. `lowercase` 所有字母转小写。
    - 适合：后续匹配/去重不关心大小写（如英文普通描述、标签）。
    - 不推荐：区分大小写有意义（专有名词、代码片段、变量、情感强调）。

4. `collapse_newlines` 折叠多余空行，避免大段空白。
    - 处理：将连续的空行收缩为 1 行，并清理多余空白；可减少 token / 噪声。
    - 保留：正常段落的单个换行。

**执行顺序（当前实现）** 先做空白折叠，再按所选模式应用（NFC → 全角 → 小写 → 空行折叠）。
如需更精细顺序或增加“保留大小写重要字段”白名单，可后续扩展。
"""
                            )
                        
                        clean_btn = gr.Button("开始清洗", variant="primary")
                    
                    with gr.Column(scale=1):
                        clean_status = gr.Textbox(
                            label="清洗状态",
                            lines=8,
                            interactive=False,
                            show_copy_button=True
                        )
        
        # 自动刷新定时器
        auto_refresh_timer = gr.Timer(value=2)
        auto_refresh_timer.tick(
            fn=self._get_convert_tasks_df,
            outputs=[convert_task_list]
        )
        
        # 存储组件引用
        self.launcher.components['process'] = {
            'auto_refresh_timer': auto_refresh_timer,
            'convert_source': convert_source,
            'convert_target': convert_target,
            'convert_output_dir': convert_output_dir,
            'convert_status': convert_status,
            'convert_task_list': convert_task_list,
            'selected_convert_task': selected_convert_task,
            'convert_detail_status': convert_detail_status,
            'extract_source': extract_source,
            'extract_fields': extract_fields,
            'extract_output_dir': extract_output_dir,
            'extract_status': extract_status,
            'merge_file_upload': merge_file_upload,
            'add_file_btn': add_file_btn,
            'clear_files_btn': clear_files_btn,
            'merge_file_list': merge_file_list,
            'merge_mode': merge_mode,
            'merge_output_dir': merge_output_dir,
            'merge_status': merge_status,
            'clean_source': clean_source,
            'clean_operations': clean_operations,
            'clean_empty_fields': clean_empty_fields,
            'clean_empty_mode': clean_empty_mode,
            'clean_sensitive_words': clean_sensitive_words,
            'clean_sensitive_fields': clean_sensitive_fields,
            'clean_sensitive_action': clean_sensitive_action,
            'clean_sensitive_replacement': clean_sensitive_replacement,
            'clean_sensitive_exclude_fields': clean_sensitive_exclude_fields,
            'clean_sensitive_field_policies': clean_sensitive_field_policies,
            'clean_sensitive_use_regex': clean_sensitive_use_regex,
            'clean_sensitive_case_sensitive': clean_sensitive_case_sensitive,
            'sensitive_preview_text': sensitive_preview_text,
            'sensitive_preview_btn': sensitive_preview_btn,
            'sensitive_preview_result': sensitive_preview_result,
            'pii_enable': pii_enable,
            'pii_repl_default': pii_repl_default,
            'pii_repl_map': pii_repl_map,
            'normalize_modes': normalize_modes,
            'clean_status': clean_status
        }
        
        # 绑定事件处理器
        convert_btn.click(
            fn=self._start_format_convert,
            inputs=[convert_source, convert_target, convert_output_dir],
            outputs=[convert_status]
        )
        
        # 新增：异步任务管理事件
        refresh_convert_btn.click(
            fn=self._get_convert_tasks_df,
            outputs=[convert_task_list]
        )
        
        convert_task_list.select(
            fn=self._select_convert_task,
            outputs=[selected_convert_task]
        )
        
        view_convert_result_btn.click(
            fn=self._view_convert_result,
            inputs=[selected_convert_task],
            outputs=[convert_detail_status]
        )
        
        extract_preview_btn.click(
            fn=self._preview_extract_fields,
            inputs=[extract_source],
            outputs=[extract_fields]
        )
        
        # 当文件上传时自动重置字段选择
        extract_source.change(
            fn=self._reset_field_selection,
            inputs=[],
            outputs=[extract_fields, field_mapping_df]
        )
        
        # 字段选择变化时更新映射表
        extract_fields.change(
            fn=self._update_field_mapping,
            inputs=[extract_fields],
            outputs=[field_mapping_df]
        )
        
        extract_btn.click(
            fn=self._start_field_extract_with_progress,
            inputs=[extract_source, extract_fields, field_mapping_df, extract_output_dir],
            outputs=[extract_status]
        )
        
        # 文件管理事件绑定
        add_file_btn.click(
            fn=self._add_merge_file,
            inputs=[merge_file_upload, merge_file_list],
            outputs=[merge_file_upload, merge_file_list]
        )
        
        clear_files_btn.click(
            fn=self._clear_merge_files,
            inputs=[],
            outputs=[merge_file_list]
        )
        
        # 删除选中文件事件
        # 改为使用索引删除，以支持重复文件名的正确删除
        
        def on_select_merge_file(evt: gr.SelectData):
            return evt.index[0]

        merge_file_list.select(
            fn=on_select_merge_file,
            inputs=[],
            outputs=[selected_merge_index]
        )
        
        def delete_selected_file(selected_idx, df):
            if selected_idx is None or selected_idx < 0:
                return df
            
            idx = int(selected_idx)
            if idx >= len(df):
                return df

            # 从列表中移除 (按索引)
            # 注意：self.merge_file_paths 和 df 必须保持同步
            if 0 <= idx < len(self.merge_file_paths):
                self.merge_file_paths.pop(idx)
            
            # 从DataFrame中移除
            new_df = df.drop(idx).reset_index(drop=True)
            return new_df

        delete_file_btn.click(
            fn=delete_selected_file,
            inputs=[selected_merge_index, merge_file_list],
            outputs=[merge_file_list]
        )
        
        merge_btn.click(
            fn=self._start_merge,
            inputs=[merge_output_filename, merge_output_dir, merge_mode],
            outputs=[merge_status]
        )

        # 清洗任务启动按钮事件绑定
        clean_btn.click(
            fn=self._start_clean,
            inputs=[clean_source, clean_operations, clean_empty_fields,
                clean_empty_mode,
                clean_sensitive_words, clean_sensitive_action, clean_sensitive_replacement,
                clean_sensitive_fields, clean_sensitive_exclude_fields, clean_sensitive_field_policies,
                clean_sensitive_use_regex, clean_sensitive_case_sensitive,
                pii_enable, pii_repl_default, pii_repl_map,
                normalize_modes],
            outputs=[clean_status]
        )

        # 敏感词试运行绑定
        sensitive_preview_btn.click(
            fn=self._preview_sensitive_words,
            inputs=[sensitive_preview_text, clean_sensitive_words, clean_sensitive_action, clean_sensitive_replacement,
                    clean_sensitive_fields, clean_sensitive_exclude_fields, clean_sensitive_field_policies,
                    clean_sensitive_use_regex, clean_sensitive_case_sensitive],
            outputs=[sensitive_preview_result]
        )

    def _start_field_extract_with_progress(self, source_file, fields, field_mapping_df, output_dir: str):
        """开始字段提取（带进度显示）"""
        try:
            if source_file is None:
                yield "❌ 请选择源文件"
                return

            if not fields:
                yield "❌ 请选择要提取的字段"
                return

            source_path = source_file.name
            if not os.path.exists(source_path):
                yield "❌ 源文件不存在"
                return

            # 进度信息收集
            progress_log = []
            
            def progress_callback(message, percent):
                """进度回调函数"""
                timestamp = time.strftime("%H:%M:%S")
                progress_info = f"[{timestamp}] {percent:3.0f}% - {message}"
                progress_log.append(progress_info)
                self.logger.info(progress_info)

            try:
                # 开始提取前的准备工作
                progress_callback("🚀 开始初始化字段提取任务...", 0)
                yield "\n".join(progress_log)
                
                # 处理字段映射
                field_mapping = {}
                mapping_data = []
                
                if field_mapping_df is not None:
                    try:
                        if hasattr(field_mapping_df, 'values'):
                            mapping_data = field_mapping_df.values.tolist()
                        elif isinstance(field_mapping_df, list):
                            mapping_data = field_mapping_df
                        elif hasattr(field_mapping_df, '__iter__'):
                            mapping_data = list(field_mapping_df)
                        
                        progress_callback("✅ 字段映射数据处理完成", 5)
                        yield "\n".join(progress_log)
                        
                    except Exception as e:
                        progress_callback(f"⚠️ 映射数据处理异常: {e}", 5)
                        mapping_data = []
                        yield "\n".join(progress_log)
                
                # 解析映射数据
                if mapping_data:
                    for row in mapping_data:
                        try:
                            if isinstance(row, (list, tuple)) and len(row) >= 2:
                                original_field = str(row[0]).strip() if row[0] else ""
                                new_field = str(row[1]).strip() if row[1] else ""
                                
                                if original_field and new_field:
                                    field_mapping[original_field] = new_field
                        except Exception as e:
                            continue
                    
                    if field_mapping:
                        progress_callback(f"🏷️ 字段重命名映射已设置: {len(field_mapping)} 个字段", 8)
                        yield "\n".join(progress_log)

                # 使用通用字段提取器（带进度回调）
                progress_callback("🔄 启动字段提取引擎...", 10)
                yield "\n".join(progress_log)
                
                # 方案：使用线程运行提取任务，主线程循环yield进度
                import threading
                import queue
                
                msg_queue = queue.Queue()
                result_queue = queue.Queue()
                
                def thread_callback(message, percent):
                    timestamp = time.strftime("%H:%M:%S")
                    progress_info = f"[{timestamp}] {percent:3.0f}% - {message}"
                    msg_queue.put(progress_info)
                
                def run_extract():
                    try:
                        res = extract_fields_universal(
                            source_path=source_path,
                            fields=fields,
                            output_dir=output_dir or str(self.launcher.root_dir / 'processed'),
                            field_mapping=field_mapping,
                            progress_callback=thread_callback
                        )
                        result_queue.put(('success', res))
                    except Exception as e:
                        result_queue.put(('error', str(e)))
                
                t = threading.Thread(target=run_extract)
                t.start()
                
                # 循环等待直到线程结束
                while t.is_alive():
                    # 获取所有新消息
                    while not msg_queue.empty():
                        msg = msg_queue.get()
                        progress_log.append(msg)
                    
                    yield "\n".join(progress_log)
                    time.sleep(0.1)
                
                # 处理剩余消息
                while not msg_queue.empty():
                    msg = msg_queue.get()
                    progress_log.append(msg)
                yield "\n".join(progress_log)
                
                # 获取结果
                status, result = result_queue.get()

                if status == 'success':
                    result_path = result
                    if result_path and os.path.exists(result_path):
                        file_size = os.path.getsize(result_path)
                        
                        # 构建详细的结果报告
                        mapping_info = ""
                        if field_mapping:
                            mapping_list = [f"{k} → {v}" for k, v in field_mapping.items()]
                            mapping_info = f"\n📋 字段映射: {', '.join(mapping_list)}"
                        
                        # 合并进度日志
                        progress_summary = "\n".join(progress_log)
                        
                        final_result = f"""✅ 字段提取任务完成！

📊 提取详情:
• 选择字段: {', '.join(fields)}{mapping_info}
• 输出文件: {result_path}  
• 文件大小: {file_size:,} 字节

📈 执行日志:
{progress_summary}

🎉 任务执行成功！"""
                        
                        self.logger.info(f"字段提取完成: {result_path}")
                        yield final_result
                    else:
                        error_summary = "\n".join(progress_log)
                        yield f"""❌ 字段提取失败

执行日志:
{error_summary}

请检查源文件格式和选择的字段"""
                else:
                    error_summary = "\n".join(progress_log)
                    yield f"❌ 字段提取异常: {result}\n\n执行日志:\n{error_summary}"

            except Exception as e:
                self.logger.error(f"字段提取过程异常: {e}")
                error_summary = "\n".join(progress_log)
                yield f"❌ 字段提取异常: {str(e)}\n\n执行日志:\n{error_summary}"
                
        except Exception as e:
            self.logger.error(f'字段提取失败: {e}')
            yield f"❌ 字段提取失败: {str(e)}"

def create_process_tab(launcher):
    manager = ProcessTabManager(launcher)
    manager.create_tab()
    return manager
