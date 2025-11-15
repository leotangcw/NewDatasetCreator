#!/usr/bin/env python3
"""
网页UI启动器

本模块基于Gradio实现网页界面，提供数据集下载、数据加工、模型配置、蒸馏生成、数据管理等功能的可视化操作入口。
功能特点：
- 5个主要标签页对应核心功能
- 实时状态同步和进度显示
- 友好的用户交互体验
- 完整的错误处理和提示

设计原则：
- 纯UI层，不包含业务逻辑
- 通过函数接口调用核心模块
- 统一的错误处理和用户提示
- 响应式设计和状态管理

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import os
import json
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# 添加通用字段提取器
try:
    from .universal_field_extractor import extract_fields_universal, get_field_names_universal
except Exception:
    from universal_field_extractor import extract_fields_universal, get_field_names_universal  # 兜底
import gradio as gr
import pandas as pd

# 基础支撑层导入
try:
    # 作为模块导入时使用相对导入
    from .config_manager import config_manager
    from .log_manager import log_manager
    from .state_manager import state_manager, TaskType
    from .utils import FileOperations
    # 核心功能模块导入 - 使用原始完整功能版本
    from .dataset_downloader import DatasetDownloader
    from .format_converter import FormatConverter, convert_format, start_convert, get_convert_progress, list_converts
    from .field_extractor import FieldExtractor, get_fields, extract_fields
    from .data_merger import DataMerger, merge_data
    from .model_manager import model_manager
    from .distill_generator import distill_generator
    from .data_manager import data_manager
    from .data_cleaner import data_cleaner
    from .dataset_previewer import DatasetPreviewer, PreviewConfig
except ImportError:
    # 直接运行时使用绝对导入
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from config_manager import config_manager
    from log_manager import log_manager
    from state_manager import state_manager, TaskType
    from utils import FileOperations
    # 核心功能模块导入 - 使用原始完整功能版本
    from dataset_downloader import DatasetDownloader
    from format_converter import FormatConverter, convert_format, start_convert, get_convert_progress, list_converts
    from field_extractor import FieldExtractor, get_fields, extract_fields
    from data_merger import DataMerger, merge_data
    from model_manager import model_manager
    from distill_generator import distill_generator
    from data_manager import data_manager
    from data_cleaner import data_cleaner
    from dataset_previewer import DatasetPreviewer, PreviewConfig


class UILauncher:
    """
    UI启动器类
    
    负责构建Gradio界面，对接所有核心功能模块。
    """
    
    def __init__(self):
        """初始化UI启动器"""
        self.logger = log_manager.get_logger('ui_launcher')
        
        # 获取配置
        self.root_dir = Path(config_manager.get_config('base.root_dir', './data'))
        self.update_interval = 2  # 状态更新间隔（秒）
        
        # 初始化核心功能模块实例
        self.dataset_downloader = DatasetDownloader()
        self.format_converter = FormatConverter()
        self.field_extractor = FieldExtractor()
        # 初始化字段提取器
        self.field_extractor.init_extractor()
        self.data_merger = DataMerger()
        # model_manager, distill_generator, data_manager, data_cleaner 已经是实例
        
        # 初始化数据预览器
        preview_config = PreviewConfig(
            max_rows=100,
            max_files=10,
            max_text_length=300,
            max_file_size_mb=500,
            enable_truncation=True,
            show_stats=True,
            include_metadata=True,
            smart_columns=True,
            show_all_columns=False
        )
        self.dataset_previewer = DatasetPreviewer(preview_config)
        
        # 界面组件存储
        self.components = {}
        
        # 状态管理
        self.merge_file_paths = []  # 存储待合并的文件路径
        self.running_tasks = {}
        self.last_update = 0
        
        # 多选任务状态
        self.selected_tasks = set()  # 存储已选择的任务ID
        
        self.logger.info('UI启动器初始化完成')
    
    def _get_saved_token(self, platform: str) -> str:
        """
        获取保存的token
        
        Args:
            platform: 平台名称 ('huggingface' 或 'modelscope')
            
        Returns:
            str: 保存的token，如果没有则返回空字符串
        """
        try:
            config_key = f'tokens.{platform}'
            return config_manager.get_config(config_key, '')
        except Exception as e:
            self.logger.warning(f'获取{platform} token失败: {e}')
            return ''
    
    def _save_token(self, platform: str, token: str):
        """
        保存token到配置
        
        Args:
            platform: 平台名称 ('huggingface' 或 'modelscope')
            token: token值
        """
        try:
            if token.strip():
                config_key = f'tokens.{platform}'
                config_manager.set_config(config_key, token.strip())
                self.logger.info(f'{platform} token已保存到配置')
            else:
                # 如果token为空，删除配置项
                config_key = f'tokens.{platform}'
                config_manager.set_config(config_key, '')
        except Exception as e:
            self.logger.error(f'保存{platform} token失败: {e}')
    
    def launch(self, share: bool = False, server_port: int = 7860):
        """
        启动Gradio界面
        
        Args:
            share (bool): 是否创建公共链接
            server_port (int): 服务器端口
        """
        # 创建Gradio界面
        with gr.Blocks(
            title="自动数据蒸馏软件",
            theme=gr.themes.Soft(),
            css=self._get_custom_css()
        ) as demo:
            
            gr.Markdown("# 🤖 自动数据蒸馏软件")
            gr.Markdown("*一站式数据集处理与AI模型蒸馏平台*")
            
            # 创建标签页
            with gr.Tabs():
                # 标签页1：数据集下载
                with gr.TabItem("📥 数据集下载", id="download"):
                    self._create_download_tab()
                
                # 标签页2：数据加工
                with gr.TabItem("🔧 数据加工", id="process"):
                    self._create_process_tab()
                
                # 标签页3：模型配置
                with gr.TabItem("⚙️ 模型配置", id="model"):
                    self._create_model_tab()
                
                # 标签页4：蒸馏生成
                with gr.TabItem("🧠 蒸馏生成", id="distill"):
                    self._create_distill_tab()
                
                # 标签页5：数据管理
                with gr.TabItem("📊 数据管理", id="manage"):
                    self._create_manage_tab()
            
            # 启动定时任务更新状态
            self._setup_auto_refresh(demo)
        
        # 启动服务器
        self.logger.info(f'启动Gradio服务器，端口: {server_port}')
        demo.launch(
            share=share,
            server_port=server_port,
            server_name="0.0.0.0",
            show_error=True
        )
    
    def _get_custom_css(self) -> str:
        """获取自定义CSS样式"""
        return """
        .status-running { background-color: #e3f2fd !important; }
        .status-completed { background-color: #e8f5e8 !important; }
        .status-failed { background-color: #ffebee !important; }
        .progress-bar { margin: 10px 0; }
        .task-item { 
            border: 1px solid #ddd; 
            border-radius: 8px; 
            padding: 10px; 
            margin: 5px 0; 
        }
        
        /* 改善DataFrame表格显示 */
        .dataframe {
            overflow-x: auto !important;
            font-size: 14px !important;
            max-height: 600px !important;
            overflow-y: auto !important;
        }
        
        .dataframe table {
            table-layout: fixed !important;
            width: 100% !important;
            border-collapse: collapse !important;
        }
        
        .dataframe td, .dataframe th {
            padding: 8px 12px !important;
            text-align: left !important;
            border: 1px solid #dee2e6 !important;
            word-wrap: break-word !important;
            vertical-align: top !important;
            position: relative !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
            white-space: nowrap !important;
        }
        
        /* 智能列宽分配 - 兼容性更好的写法 */
        .dataframe colgroup col {
            width: auto !important;
        }
        
        /* JavaScript动态设置列宽的默认规则 */
        .dataframe td, .dataframe th {
            min-width: 100px !important;
            max-width: 400px !important;
        }
        
        /* 鼠标悬停时显示完整内容 */
        .dataframe td:hover {
            white-space: normal !important;
            overflow: visible !important;
            max-width: none !important;
            z-index: 999 !important;
            background-color: #f8f9fa !important;
            box-shadow: 0 2px 8px rgba(0,0,0,0.15) !important;
        }
        
        /* 动态列宽调整 */
        .equal-width-table td, .equal-width-table th {
            width: auto !important;
        }
        </style>
        
        <script>
        // 动态调整表格列宽 - 智能分配
        function adjustTableColumns() {
            const tables = document.querySelectorAll('.dataframe table');
            tables.forEach(table => {
                const rows = table.querySelectorAll('tr');
                if (rows.length === 0) return;
                
                const firstRow = rows[0];
                const cells = firstRow.querySelectorAll('th, td');
                const columnCount = cells.length;
                
                if (columnCount > 0) {
                    // 智能分配列宽
                    let widths = [];
                    if (columnCount === 2) {
                        // 两列时，平均分配，稍微倾斜向第二列
                        widths = [45, 55];
                    } else if (columnCount === 3) {
                        // 三列时，中间列稍大
                        widths = [30, 40, 30];
                    } else if (columnCount >= 4) {
                        // 多列时，均匀分配，但确保最小宽度
                        const baseWidth = Math.max(Math.floor(100 / columnCount), 15);
                        widths = Array(columnCount).fill(baseWidth);
                        // 调整总和为100%
                        const totalWidth = widths.reduce((sum, w) => sum + w, 0);
                        if (totalWidth !== 100) {
                            widths[widths.length - 1] += 100 - totalWidth;
                        }
                    } else {
                        // 默认均匀分配
                        const width = Math.floor(100 / columnCount);
                        widths = Array(columnCount).fill(width);
                    }
                    
                    // 应用列宽
                    cells.forEach((cell, index) => {
                        if (index < widths.length) {
                            cell.style.width = widths[index] + '%';
                            cell.style.minWidth = '150px'; // 设置最小宽度
                        }
                    });
                    
                    // 为所有行应用相同的列宽
                    rows.forEach(row => {
                        const rowCells = row.querySelectorAll('th, td');
                        rowCells.forEach((cell, index) => {
                            if (index < widths.length) {
                                cell.style.width = widths[index] + '%';
                                cell.style.minWidth = '150px';
                            }
                        });
                    });
                }
            });
        }
        
        // 监听DOM变化，自动调整表格
        const observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                if (mutation.type === 'childList') {
                    // 延迟执行，确保DOM完全更新
                    setTimeout(adjustTableColumns, 100);
                }
            });
        });
        
        // 开始监听
        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
        
        // 页面加载完成后调整一次
        document.addEventListener('DOMContentLoaded', adjustTableColumns);
        
        // 在渲染后也调整一次
        setTimeout(adjustTableColumns, 500);
        </script>
        
        /* 悬停显示完整内容 */
        .dataframe td:hover {
            overflow: visible !important;
            white-space: normal !important;
            position: relative !important;
            background-color: #fff3cd !important;
            z-index: 10 !important;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1) !important;
            border: 2px solid #ffc107 !important;
            max-width: none !important;
            word-break: break-word !important;
        }
        
        /* 改善列标题样式 */
        .dataframe th {
            background-color: #e9ecef !important;
            font-weight: bold !important;
            border-bottom: 2px solid #dee2e6 !important;
            position: sticky !important;
            top: 0 !important;
            z-index: 5 !important;
            text-align: center !important;
        }
        
        /* 隐藏空值单元格的特殊样式 */
        .dataframe td:empty,
        .dataframe td[data-value=""],
        .dataframe td[data-value="null"],
        .dataframe td[data-value="None"] {
            background-color: #f8f9fa !important;
            opacity: 0.5 !important;
        }
        
        /* 截断文本的指示 */
        .dataframe td[title]:after {
            content: "..." !important;
            color: #6c757d !important;
            font-weight: bold !important;
        }
        
        /* 针对不同列设置不同的最小宽度 */
        .dataframe th:nth-child(1), .dataframe td:nth-child(1) {
            min-width: 120px !important; /* 名称列 */
        }
        .dataframe th:nth-child(2), .dataframe td:nth-child(2) {
            min-width: 80px !important;  /* 类型列 */
        }
        .dataframe th:nth-child(3), .dataframe td:nth-child(3) {
            min-width: 80px !important;  /* 大小列 */
        }
        .dataframe th:nth-child(4), .dataframe td:nth-child(4) {
            min-width: 140px !important; /* 时间列 */
        }
        .dataframe th:nth-child(5), .dataframe td:nth-child(5) {
            min-width: 200px !important; /* 路径列 */
            max-width: 300px !important;
            overflow: hidden !important;
            text-overflow: ellipsis !important;
        }
        
        /* 悬停显示完整内容 */
        .dataframe td:hover {
            overflow: visible !important;
            white-space: normal !important;
            position: relative !important;
            background-color: #f8f9fa !important;
            z-index: 10 !important;
        }
        
        /* 改善列标题样式 */
        .dataframe th {
            background-color: #f8f9fa !important;
            font-weight: bold !important;
            border-bottom: 2px solid #dee2e6 !important;
            position: sticky !important;
            top: 0 !important;
            z-index: 5 !important;
        }
        
        /* 添加表格容器的滚动提示 */
        .dataframe::after {
            content: "提示：表格可以水平滚动" !important;
            display: block !important;
            font-size: 12px !important;
            color: #6c757d !important;
            text-align: center !important;
            margin-top: 5px !important;
        }
        
        /* 选中行的样式 */
        .dataframe tr:hover {
            background-color: #e9ecef !important;
        }
        """
    
    def _create_download_tab(self):
        """创建数据集下载标签页"""
        gr.Markdown("## 数据集下载管理")
        gr.Markdown("支持从 Hugging Face、ModelScope 等平台下载数据集")
        
        with gr.Row():
            with gr.Column(scale=1):
                # 下载配置区域
                gr.Markdown("### 📝 下载配置")
                
                source_type = gr.Dropdown(
                    choices=["huggingface", "modelscope", "url"],
                    value="huggingface",
                    label="数据源类型",
                    info="选择数据集来源平台"
                )
                
                dataset_name = gr.Textbox(
                    label="数据集名称/URL",
                    placeholder="例如：squad 或 https://example.com/data.zip",
                    info="输入数据集名称或下载链接"
                )
                
                # 分别配置不同平台的API密钥
                with gr.Row():
                    huggingface_token = gr.Textbox(
                        label="🤗 Hugging Face Token",
                        type="password",
                        placeholder="输入Hugging Face访问token",
                        info="用于访问私有Hugging Face数据集",
                        value=self._get_saved_token('huggingface')
                    )
                    
                    modelscope_token = gr.Textbox(
                        label="🎯 ModelScope Token", 
                        type="password",
                        placeholder="输入ModelScope访问token",
                        info="用于访问私有ModelScope数据集",
                        value=self._get_saved_token('modelscope')
                    )
                
                save_dir = gr.Textbox(
                    label="保存目录",
                    value=str(self.root_dir / "raw"),
                    info="数据集保存路径"
                )
                
                with gr.Row():
                    add_task_btn = gr.Button("➕ 添加下载任务", variant="primary")
                    refresh_status_btn = gr.Button("🔄 刷新状态", variant="secondary")
            
            with gr.Column(scale=2):
                # 任务列表区域
                gr.Markdown("### 📋 下载任务列表")
                
                # 任务信息显示表格（仅显示，不可选择）
                task_list = gr.Dataframe(
                    headers=["任务ID", "数据集名称", "状态", "进度", "开始时间"],
                    datatype=["str", "str", "str", "str", "str"],
                    label="",
                    interactive=False,
                    wrap=True
                )
                
                # 任务选择区域
                with gr.Row():
                    with gr.Column(scale=3):
                        gr.Markdown("**🎯 多任务选择**")
                        task_selector = gr.CheckboxGroup(
                            label="选择要操作的任务",
                            choices=[],
                            value=[],
                            interactive=True
                        )
                    with gr.Column(scale=1):
                        # 单任务操作（从选中的任务中选第一个）
                        gr.Markdown("**单任务操作**")
                        with gr.Row():
                            start_single_btn = gr.Button("▶️ 开始", size="sm")
                            pause_single_btn = gr.Button("⏸️ 暂停", size="sm")
                            delete_single_btn = gr.Button("🗑️ 删除", size="sm", variant="stop")
                
                with gr.Row():
                    # 批量操作按钮
                    refresh_btn = gr.Button("🔄 刷新列表", size="sm")
                    start_all_btn = gr.Button("▶️ 批量开始", size="sm", variant="primary")
                    pause_all_btn = gr.Button("⏸️ 批量暂停", size="sm") 
                    delete_all_btn = gr.Button("🗑️ 批量删除", size="sm", variant="stop")
                
                # 删除选项
                with gr.Row():
                    delete_files_checkbox = gr.Checkbox(
                        label="删除任务时同时删除本地文件",
                        value=False,  # 默认不删除文件
                        info="勾选后将同时删除已下载的数据集文件"
                    )
        
        # 状态输出区域
        with gr.Row():
            download_status = gr.Textbox(
                label="状态信息",
                lines=3,
                interactive=False,
                show_copy_button=True
            )
        
        # 添加隐藏的定时器，用于自动刷新任务列表
        with gr.Row(visible=False):
            auto_refresh_timer = gr.Number(value=0, label="定时器", visible=False)
        
        # 存储组件引用
        self.components['download'] = {
            'source_type': source_type,
            'dataset_name': dataset_name,
            'huggingface_token': huggingface_token,
            'modelscope_token': modelscope_token,
            'save_dir': save_dir,
            'task_list': task_list,
            'task_selector': task_selector,
            'delete_files_checkbox': delete_files_checkbox,
            'status': download_status,
            'auto_refresh_timer': auto_refresh_timer
        }
        
        # 绑定事件处理器
        add_task_btn.click(
            fn=self._add_download_task,
            inputs=[source_type, dataset_name, huggingface_token, modelscope_token, save_dir],
            outputs=[download_status, task_list, task_selector]
        )
        
        # 左侧刷新（配置区）
        refresh_status_btn.click(
            fn=self._refresh_download_tasks_and_selector,
            outputs=[task_list, task_selector]
        )
        
        # 右侧刷新（任务区）
        try:
            refresh_btn.click(
                fn=self._refresh_download_tasks_and_selector,
                outputs=[task_list, task_selector]
            )
        except Exception:
            pass
        
        # 单任务操作
        start_single_btn.click(
            fn=self._start_single_task,
            inputs=[task_selector],
            outputs=[download_status, task_list, task_selector]
        )
        
        pause_single_btn.click(
            fn=self._pause_single_task,
            inputs=[task_selector],
            outputs=[download_status, task_list, task_selector]
        )
        
        delete_single_btn.click(
            fn=self._delete_single_task,
            inputs=[task_selector, delete_files_checkbox],
            outputs=[download_status, task_list, task_selector]
        )
        
        # 批量操作
        start_all_btn.click(
            fn=self._start_multiple_tasks_new,
            inputs=[task_selector],
            outputs=[download_status, task_list, task_selector]
        )
        
        pause_all_btn.click(
            fn=self._pause_multiple_tasks,
            inputs=[task_selector],
            outputs=[download_status, task_list, task_selector]
        )
        
        delete_all_btn.click(
            fn=self._delete_multiple_tasks,
            inputs=[task_selector, delete_files_checkbox],
            outputs=[download_status, task_list, task_selector]
        )
        
        # Token自动保存事件处理器
        huggingface_token.change(
            fn=lambda token: self._save_token('huggingface', token) if token.strip() else None,
            inputs=[huggingface_token],
            outputs=[]
        )
        
        modelscope_token.change(
            fn=lambda token: self._save_token('modelscope', token) if token.strip() else None,
            inputs=[modelscope_token],
            outputs=[]
        )
    
    def _add_download_task(self, source_type: str, dataset_name: str, 
                          huggingface_token: str, modelscope_token: str, save_dir: str) -> Tuple[str, pd.DataFrame, gr.CheckboxGroup]:
        """添加下载任务"""
        try:
            if not dataset_name.strip():
                return "❌ 请输入数据集名称或URL", self._get_download_tasks_df(), self._get_task_selector_choices()
            
            # 构建下载参数
            params = {
                'source_type': (source_type or '').strip().lower(),
                'dataset_name': dataset_name.strip(),
                'save_dir': (save_dir.strip() if save_dir else str(self.root_dir / "raw"))
            }
            
            # 根据source_type选择合适的token并保存
            if params['source_type'] == 'huggingface' and huggingface_token.strip():
                params['token'] = huggingface_token.strip()
                self._save_token('huggingface', huggingface_token.strip())
            elif params['source_type'] == 'modelscope' and modelscope_token.strip():
                params['token'] = modelscope_token.strip()
                self._save_token('modelscope', modelscope_token.strip())
            
            # 调用核心模块添加任务（解包参数）
            task_id = self.dataset_downloader.add_download_task(**params)
            
            return f"✅ 下载任务已添加: {task_id}", self._get_download_tasks_df(), self._get_task_selector_choices()
            
        except Exception as e:
            self.logger.error(f'添加下载任务失败: {e}')
            return f"❌ 添加任务失败: {str(e)}", self._get_download_tasks_df(), self._get_task_selector_choices()
    
    def _get_task_selector_choices(self) -> gr.CheckboxGroup:
        """获取任务选择器的选择列表"""
        try:
            tasks = self.dataset_downloader.list_tasks()
            choices = []
            for task in tasks:
                task_id = task.get('task_id', '')
                params = task.get('params', {})
                dataset_name = params.get('dataset_name', '')
                # 格式：任务ID - 数据集名称
                choice_label = f"{task_id} - {dataset_name}"
                choices.append((choice_label, task_id))
            
            return gr.CheckboxGroup(
                label="选择要操作的任务",
                choices=choices,
                value=[],
                interactive=True
            )
        except Exception as e:
            self.logger.error(f'获取任务选择器失败: {e}')
            return gr.CheckboxGroup(
                label="选择要操作的任务",
                choices=[],
                value=[],
                interactive=True
            )
    
    def _refresh_download_tasks_and_selector(self) -> Tuple[pd.DataFrame, gr.CheckboxGroup]:
        """刷新下载任务列表和任务选择器"""
        return self._get_download_tasks_df(), self._get_task_selector_choices()
    
    def _refresh_download_tasks(self) -> pd.DataFrame:
        """刷新下载任务列表"""
        return self._get_download_tasks_df()
    
    def _start_download_task(self, task_id: str) -> Tuple[str, pd.DataFrame]:
        """开始下载任务"""
        try:
            if not task_id.strip():
                return "❌ 请选择要开始的任务", self._get_download_tasks_df()
            
            # UI模式：使用异步执行，不阻塞界面
            success = self.dataset_downloader.start_task(task_id.strip(), async_mode=True)
            
            if success:
                return f"✅ 任务已开始: {task_id}", self._get_download_tasks_df()
            else:
                return f"❌ 任务开始失败: {task_id}", self._get_download_tasks_df()
                
        except Exception as e:
            self.logger.error(f'开始下载任务失败: {e}')
            return f"❌ 开始任务失败: {str(e)}", self._get_download_tasks_df()
    
    def _start_multiple_tasks(self, task_ids_str: str) -> Tuple[str, pd.DataFrame]:
        """开始多个下载任务"""
        try:
            self.logger.info(f"批量启动请求，输入: '{task_ids_str}'")
            
            # 优先使用已选任务列表，如果为空则解析输入字符串
            if self.selected_tasks:
                task_ids = list(self.selected_tasks)
                self.logger.info(f"使用已选任务列表: {task_ids}")
            elif task_ids_str.strip():
                # 解析输入的任务ID列表
                task_ids = [tid.strip() for tid in task_ids_str.split(',') if tid.strip()]
                self.logger.info(f"解析输入任务ID列表: {task_ids}")
            else:
                # 如果输入为空且没有选中任务，返回提示
                current_tasks = self.dataset_downloader.list_tasks()
                if current_tasks:
                    return "❌ 请先点击任务行选择要启动的任务，或在输入框中手动输入任务ID", self._get_download_tasks_df()
                else:
                    return "❌ 没有可用的任务", self._get_download_tasks_df()
            
            if not task_ids:
                return "❌ 请输入有效的任务ID格式：task1,task2,task3", self._get_download_tasks_df()
            
            # 验证任务是否存在
            available_tasks = {task['task_id'] for task in self.dataset_downloader.list_tasks()}
            valid_tasks = []
            invalid_tasks = []
            
            for task_id in task_ids:
                if task_id in available_tasks:
                    valid_tasks.append(task_id)
                else:
                    invalid_tasks.append(task_id)
            
            if invalid_tasks:
                return f"❌ 以下任务ID不存在: {', '.join(invalid_tasks)}\n可用任务: {', '.join(available_tasks)}", self._get_download_tasks_df()
            
            success_count = 0
            failed_count = 0
            results = []
            
            for task_id in valid_tasks:
                try:
                    # UI模式：使用异步执行，不阻塞界面
                    success = self.dataset_downloader.start_task(task_id, async_mode=True)
                    if success:
                        success_count += 1
                        results.append(f"✅ {task_id}")
                    else:
                        failed_count += 1
                        results.append(f"❌ {task_id}")
                except Exception as e:
                    failed_count += 1
                    results.append(f"❌ {task_id}: {str(e)}")
            
            summary = f"批量启动完成: {success_count}个成功, {failed_count}个失败"
            details = "\n".join(results)
            
            return f"{summary}\n\n详情:\n{details}", self._get_download_tasks_df()
                
        except Exception as e:
            self.logger.error(f'批量启动任务失败: {e}')
            return f"❌ 批量启动失败: {str(e)}", self._get_download_tasks_df()
    
    def _pause_download_task(self, task_id: str) -> Tuple[str, pd.DataFrame]:
        """暂停下载任务"""
        try:
            if not task_id.strip():
                return "❌ 请选择要暂停的任务", self._get_download_tasks_df()
            
            success = self.dataset_downloader.pause_task(task_id.strip())
            
            if success:
                return f"✅ 任务已暂停: {task_id}", self._get_download_tasks_df()
            else:
                return f"❌ 任务暂停失败: {task_id}", self._get_download_tasks_df()
                
        except Exception as e:
            self.logger.error(f'暂停下载任务失败: {e}')
            return f"❌ 暂停任务失败: {str(e)}", self._get_download_tasks_df()
    
    def _delete_download_task(self, task_id: str) -> Tuple[str, pd.DataFrame]:
        """删除下载任务"""
        try:
            if not task_id.strip():
                return "❌ 请选择要删除的任务", self._get_download_tasks_df()
            
            success = self.dataset_downloader.delete_task(task_id.strip())
            
            if success:
                return f"✅ 任务已删除: {task_id}", self._get_download_tasks_df()
            else:
                return f"❌ 任务删除失败: {task_id}", self._get_download_tasks_df()
                
        except Exception as e:
            self.logger.error(f'删除下载任务失败: {e}')
            return f"❌ 删除任务失败: {str(e)}", self._get_download_tasks_df()
    
    def _toggle_task_selection(self, evt: gr.SelectData) -> Tuple[str, str]:
        """切换任务选择状态（添加或移除）"""
        # 获取当前数据框
        tasks = self.dataset_downloader.list_tasks()
        if not tasks or evt.index[0] >= len(tasks):
            return "", self._format_selected_tasks()
        
        # 获取选中的任务ID
        task = tasks[evt.index[0]]
        task_id = task.get('task_id', '')
        
        if not task_id:
            return "", self._format_selected_tasks()
        
        # 切换选择状态
        if task_id in self.selected_tasks:
            self.selected_tasks.remove(task_id)
            self.logger.info(f'从多选列表中移除任务: {task_id}')
        else:
            self.selected_tasks.add(task_id)
            self.logger.info(f'添加任务到多选列表: {task_id}')
        
        return task_id, self._format_selected_tasks()
    
    def _clear_task_selection(self) -> str:
        """清空任务选择"""
        self.selected_tasks.clear()
        self.logger.info('清空多选任务列表')
        return ""
    
    def _format_selected_tasks(self) -> str:
        """格式化已选任务列表显示"""
        if not self.selected_tasks:
            return ""
        return ", ".join(sorted(self.selected_tasks))
    
    def _select_download_task(self, evt: gr.SelectData) -> str:
        """选择下载任务（保留兼容性）"""
        # 获取当前数据框
        tasks = self.dataset_downloader.list_tasks()
        if not tasks or evt.index[0] >= len(tasks):
            return ""
        
        # 返回选中行的任务ID（第一列）
        task = tasks[evt.index[0]]
        return task.get('task_id', '')
    
    def _get_download_tasks_df(self) -> pd.DataFrame:
        """获取下载任务列表数据框"""
        try:
            # 获取所有下载任务
            tasks = self.dataset_downloader.list_tasks()
            
            if not tasks:
                return pd.DataFrame(columns=["任务ID", "数据集名称", "状态", "进度", "开始时间"])
            
            # 构建数据框
            rows = []
            for task in tasks:
                task_id = task.get('task_id', '')
                params = task.get('params', {})
                progress_info = task.get('progress', {})
                
                dataset_name = params.get('dataset_name', '')
                status = progress_info.get('status', 'unknown')
                progress = progress_info.get('progress', 0)
                start_time = progress_info.get('start_time', '')
                
                # 状态中文映射
                status_map = {
                    'pending': '等待中',
                    'running': '下载中',
                    'paused': '已暂停',
                    'completed': '已完成',
                    'failed': '失败'
                }
                
                status_cn = status_map.get(status, status)
                progress_str = f"{progress:.1f}%" if isinstance(progress, (int, float)) else "0%"
                
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
                    task_id,
                    dataset_name,
                    status_cn,
                    progress_str,
                    start_time_str
                ])
            
            return pd.DataFrame(rows, columns=["任务ID", "数据集名称", "状态", "进度", "开始时间"])
            
        except Exception as e:
            self.logger.error(f'获取下载任务列表失败: {e}')
            return pd.DataFrame(columns=["任务ID", "数据集名称", "状态", "进度", "开始时间"])
    
    # 新的任务操作方法
    def _start_single_task(self, selected_tasks: list) -> Tuple[str, pd.DataFrame, gr.CheckboxGroup]:
        """开始单个任务（从选中列表的第一个）"""
        try:
            if not selected_tasks:
                return "❌ 请先选择要开始的任务", self._get_download_tasks_df(), self._get_task_selector_choices()
            
            task_id = selected_tasks[0]  # 取第一个选中的任务
            success = self.dataset_downloader.start_task(task_id, async_mode=True)
            
            if success:
                return f"✅ 任务已开始: {task_id}", self._get_download_tasks_df(), self._get_task_selector_choices()
            else:
                return f"❌ 任务开始失败: {task_id}", self._get_download_tasks_df(), self._get_task_selector_choices()
                
        except Exception as e:
            self.logger.error(f'开始单个任务失败: {e}')
            return f"❌ 开始任务失败: {str(e)}", self._get_download_tasks_df(), self._get_task_selector_choices()
    
    def _pause_single_task(self, selected_tasks: list) -> Tuple[str, pd.DataFrame, gr.CheckboxGroup]:
        """暂停单个任务（从选中列表的第一个）"""
        try:
            if not selected_tasks:
                return "❌ 请先选择要暂停的任务", self._get_download_tasks_df(), self._get_task_selector_choices()
            
            task_id = selected_tasks[0]  # 取第一个选中的任务
            success = self.dataset_downloader.pause_task(task_id)
            
            if success:
                return f"✅ 任务已暂停: {task_id}", self._get_download_tasks_df(), self._get_task_selector_choices()
            else:
                return f"❌ 任务暂停失败: {task_id}", self._get_download_tasks_df(), self._get_task_selector_choices()
                
        except Exception as e:
            self.logger.error(f'暂停单个任务失败: {e}')
            return f"❌ 暂停任务失败: {str(e)}", self._get_download_tasks_df(), self._get_task_selector_choices()
    
    def _delete_single_task(self, selected_tasks: list, delete_files: bool = False) -> Tuple[str, pd.DataFrame, gr.CheckboxGroup]:
        """删除单个任务（从选中列表的第一个）"""
        try:
            if not selected_tasks:
                return "❌ 请先选择要删除的任务", self._get_download_tasks_df(), self._get_task_selector_choices()
            
            task_id = selected_tasks[0]  # 取第一个选中的任务
            success = self.dataset_downloader.delete_task(task_id, delete_files=delete_files)
            
            if success:
                file_msg = "（包含文件）" if delete_files else "（保留文件）"
                return f"✅ 任务已删除{file_msg}: {task_id}", self._get_download_tasks_df(), self._get_task_selector_choices()
            else:
                return f"❌ 任务删除失败: {task_id}", self._get_download_tasks_df(), self._get_task_selector_choices()
                
        except Exception as e:
            self.logger.error(f'删除单个任务失败: {e}')
            return f"❌ 删除任务失败: {str(e)}", self._get_download_tasks_df(), self._get_task_selector_choices()
    
    def _start_multiple_tasks_new(self, selected_tasks: list) -> Tuple[str, pd.DataFrame, gr.CheckboxGroup]:
        """批量开始多个任务"""
        try:
            if not selected_tasks:
                return "❌ 请先选择要开始的任务", self._get_download_tasks_df(), self._get_task_selector_choices()
            
            success_count = 0
            failed_count = 0
            results = []
            
            for task_id in selected_tasks:
                try:
                    success = self.dataset_downloader.start_task(task_id, async_mode=True)
                    if success:
                        success_count += 1
                        results.append(f"✅ {task_id}")
                    else:
                        failed_count += 1
                        results.append(f"❌ {task_id}")
                except Exception as e:
                    failed_count += 1
                    results.append(f"❌ {task_id}: {str(e)}")
            
            summary = f"批量开始完成: {success_count}个成功, {failed_count}个失败"
            details = "\n".join(results)
            
            return f"{summary}\n\n详情:\n{details}", self._get_download_tasks_df(), self._get_task_selector_choices()
            
        except Exception as e:
            self.logger.error(f'批量开始任务失败: {e}')
            return f"❌ 批量开始失败: {str(e)}", self._get_download_tasks_df(), self._get_task_selector_choices()
    
    def _pause_multiple_tasks(self, selected_tasks: list) -> Tuple[str, pd.DataFrame, gr.CheckboxGroup]:
        """批量暂停多个任务"""
        try:
            if not selected_tasks:
                return "❌ 请先选择要暂停的任务", self._get_download_tasks_df(), self._get_task_selector_choices()
            
            success_count = 0
            failed_count = 0
            results = []
            
            for task_id in selected_tasks:
                try:
                    success = self.dataset_downloader.pause_task(task_id)
                    if success:
                        success_count += 1
                        results.append(f"✅ {task_id}")
                    else:
                        failed_count += 1
                        results.append(f"❌ {task_id}")
                except Exception as e:
                    failed_count += 1
                    results.append(f"❌ {task_id}: {str(e)}")
            
            summary = f"批量暂停完成: {success_count}个成功, {failed_count}个失败"
            details = "\n".join(results)
            
            return f"{summary}\n\n详情:\n{details}", self._get_download_tasks_df(), self._get_task_selector_choices()
            
        except Exception as e:
            self.logger.error(f'批量暂停任务失败: {e}')
            return f"❌ 批量暂停失败: {str(e)}", self._get_download_tasks_df(), self._get_task_selector_choices()
    
    def _delete_multiple_tasks(self, selected_tasks: list, delete_files: bool = False) -> Tuple[str, pd.DataFrame, gr.CheckboxGroup]:
        """批量删除多个任务"""
        try:
            if not selected_tasks:
                return "❌ 请先选择要删除的任务", self._get_download_tasks_df(), self._get_task_selector_choices()
            
            success_count = 0
            failed_count = 0
            results = []
            
            for task_id in selected_tasks:
                try:
                    success = self.dataset_downloader.delete_task(task_id, delete_files=delete_files)
                    if success:
                        success_count += 1
                        results.append(f"✅ {task_id}")
                    else:
                        failed_count += 1
                        results.append(f"❌ {task_id}")
                except Exception as e:
                    failed_count += 1
                    results.append(f"❌ {task_id}: {str(e)}")
            
            file_msg = "（包含文件）" if delete_files else "（保留文件）"
            summary = f"批量删除完成{file_msg}: {success_count}个成功, {failed_count}个失败"
            details = "\n".join(results)
            
            return f"{summary}\n\n详情:\n{details}", self._get_download_tasks_df(), self._get_task_selector_choices()
            
        except Exception as e:
            self.logger.error(f'批量删除任务失败: {e}')
            return f"❌ 批量删除失败: {str(e)}", self._get_download_tasks_df(), self._get_task_selector_choices()
    
    # 以下是旧的方法（保留以防兼容性问题）
    def _create_process_tab(self):
        """创建数据加工标签页"""
        gr.Markdown("## 数据加工管理")
        gr.Markdown("支持格式转换、字段提取、数据合并、数据清洗等操作")
        
        # 功能选择标签
        with gr.Tabs():
            # 格式转换子标签
            with gr.TabItem("🔄 格式转换"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 📝 转换配置")
                        
                        convert_source = gr.File(
                            label="源文件",
                            file_types=[".jsonl", ".csv", ".xlsx", ".json", ".xml", ".md", ".markdown"]
                        )
                        
                        convert_target = gr.Dropdown(
                            choices=["jsonl", "csv", "xlsx", "json", "xml", "markdown"],
                            value="jsonl",
                            label="目标格式",
                            info="选择转换后的格式"
                        )
                        
                        convert_output_dir = gr.Textbox(
                            label="输出目录",
                            value=str(self.root_dir / "processed"),
                            info="转换结果保存路径"
                        )
                        
                        convert_btn = gr.Button("🔄 开始转换", variant="primary")
                        refresh_convert_btn = gr.Button("🔄 刷新任务", size="sm")
                    
                    with gr.Column(scale=2):
                        gr.Markdown("### 📋 转换任务列表")
                        
                        convert_task_list = gr.Dataframe(
                            headers=["任务ID", "源文件", "目标格式", "状态", "进度", "输出文件"],
                            datatype=["str", "str", "str", "str", "str", "str"],
                            label="",
                            interactive=False,
                            wrap=True
                        )
                        
                        with gr.Row():
                            selected_convert_task = gr.Textbox(
                                label="选中任务",
                                placeholder="点击任务行选择",
                                interactive=False,
                                scale=2
                            )
                            
                            view_convert_result_btn = gr.Button("📁 查看结果", size="sm", scale=1)
                
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
            with gr.TabItem("📊 字段提取"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 📝 提取配置")
                        
                        extract_source = gr.File(
                            label="源文件",
                            file_types=[".jsonl", ".csv", ".xlsx", ".json", ".xml", ".md", ".markdown"]
                        )
                        
                        extract_preview_btn = gr.Button("👁️ 预览字段", variant="secondary")
                        
                        extract_fields = gr.CheckboxGroup(
                            label="选择字段",
                            choices=[],
                            value=[],  # 确保初始值为空列表
                            info="选择要提取的字段"
                        )
                        
                        # 字段重命名区域
                        gr.Markdown("### 🏷️ 字段重命名")
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
                            value=str(self.root_dir / "processed"),
                            info="提取结果保存路径"
                        )
                        
                        extract_btn = gr.Button("📊 开始提取", variant="primary")
                    
                    with gr.Column(scale=1):
                        extract_status = gr.Textbox(
                            label="提取状态",
                            lines=8,
                            interactive=False,
                            show_copy_button=True
                        )
            
            # 数据合并子标签
            with gr.TabItem("🔗 数据合并"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 📝 合并配置")
                        
                        # 文件选择和管理
                        gr.Markdown("#### 1. 选择合并文件")
                        merge_file_upload = gr.File(
                            label="添加文件",
                            file_types=[".jsonl", ".csv", ".xlsx", ".json", ".md", ".markdown"],
                            file_count="single"
                        )
                        
                        with gr.Row():
                            add_file_btn = gr.Button("➕ 添加文件", size="sm")
                            clear_files_btn = gr.Button("🗑️ 清空列表", size="sm")
                        
                        merge_file_list = gr.Textbox(
                            label="待合并文件列表",
                            lines=6,
                            interactive=False,
                            value="📝 待合并文件列表（至少需要2个文件）:\n\n暂无文件",
                            show_copy_button=True
                        )
                        
                        # 合并选项
                        gr.Markdown("#### 2. 合并选项")
                        merge_mode = gr.Radio(
                            choices=["merge", "append"],
                            value="merge",
                            label="合并模式",
                            info="merge: 创建新文件, append: 追加到第一个文件"
                        )
                        
                        merge_dedup_field = gr.Textbox(
                            label="去重字段（可选）",
                            placeholder="例如：id 或 question",
                            info="指定用于去重的字段名"
                        )
                        
                        merge_output_dir = gr.Textbox(
                            label="输出目录",
                            value=str(self.root_dir / "processed"),
                            info="合并结果保存路径"
                        )
                        
                        merge_btn = gr.Button("🔗 开始合并", variant="primary")
                    
                    with gr.Column(scale=1):
                        merge_status = gr.Textbox(
                            label="合并状态",
                            lines=10,
                            interactive=False,
                            show_copy_button=True
                        )
            
            # 数据清洗子标签
            with gr.TabItem("🧹 数据清洗"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("### 📝 清洗配置")
                        
                        clean_source = gr.File(
                            label="源文件",
                            file_types=[".jsonl", ".csv", ".xlsx", ".json"]
                        )
                        
                        clean_operations = gr.CheckboxGroup(
                            choices=[
                                ("去除空值", "remove_empty"),
                                ("模糊去重", "deduplicate"),
                                ("敏感词处理", "filter_sensitive"),
                                ("PII脱敏", "pii_desensitize"),
                                ("文本标准化", "normalize_text")
                            ],
                            label="清洗操作",
                            info="选择要执行的清洗操作 (支持多选)"
                        )
                        
                        clean_empty_fields = gr.Textbox(
                            label="去空字段（可选）",
                            placeholder="例如：question,answer",
                            info="指定检查空值的字段，逗号分隔"
                        )

                        clean_empty_mode = gr.Radio(
                            choices=["any", "all"],
                            value="any",
                            label="空值策略",
                            info="any: 任一字段为空即丢弃; all: 所有指定字段都为空才丢弃"
                        )
                        
                        clean_dedup_field = gr.Textbox(
                            label="去重字段（可选）",
                            placeholder="例如：question",
                            info="指定用于去重的字段名"
                        )

                        clean_dedup_threshold = gr.Slider(
                            minimum=0.50,
                            maximum=1.00,
                            value=0.95,
                            step=0.01,
                            label="模糊去重阈值",
                            info="仅在选择模糊去重时生效，推荐 0.85~0.97"
                        )
                        
                        clean_sensitive_words = gr.Textbox(
                            label="敏感词列表（可选）",
                            placeholder="例如：密码,身份证,手机号",
                            info="指定敏感词，逗号分隔"
                        )

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

                        with gr.Accordion("📘 敏感词规则说明", open=False):
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

                        gr.Markdown("### 🔍 敏感词试运行 (不落地文件)")
                        sensitive_preview_text = gr.Textbox(
                            label="预览输入文本",
                            placeholder="在这里粘贴一段文本，点击下方按钮查看处理效果",
                            lines=3
                        )
                        sensitive_preview_btn = gr.Button("▶ 试运行预览", size="sm")
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

                        with gr.Accordion("📘 文本标准化说明", open=False):
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
                        
                        clean_btn = gr.Button("🧹 开始清洗", variant="primary")
                    
                    with gr.Column(scale=1):
                        clean_status = gr.Textbox(
                            label="清洗状态",
                            lines=8,
                            interactive=False,
                            show_copy_button=True
                        )
        
        # 存储组件引用
        self.components['process'] = {
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
            'merge_dedup_field': merge_dedup_field,
            'merge_output_dir': merge_output_dir,
            'merge_status': merge_status,
            'clean_source': clean_source,
            'clean_operations': clean_operations,
            'clean_empty_fields': clean_empty_fields,
            'clean_empty_mode': clean_empty_mode,
            'clean_dedup_field': clean_dedup_field,
            'clean_dedup_threshold': clean_dedup_threshold,
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
            inputs=[merge_file_upload],
            outputs=[merge_file_upload, merge_file_list]
        )
        
        clear_files_btn.click(
            fn=self._clear_merge_files,
            inputs=[],
            outputs=[merge_file_list]
        )
        
        merge_btn.click(
            fn=self._start_data_merge,
            inputs=[merge_mode, merge_dedup_field, merge_output_dir],
            outputs=[merge_status]
        )

        # 清洗任务启动按钮事件绑定
        clean_btn.click(
            fn=self._start_data_clean,
            inputs=[clean_source, clean_operations, clean_empty_fields,
                clean_empty_mode, clean_dedup_field, clean_dedup_threshold,
                clean_sensitive_words, clean_sensitive_action, clean_sensitive_replacement,
                clean_sensitive_fields, clean_sensitive_exclude_fields, clean_sensitive_field_policies,
                clean_sensitive_use_regex, clean_sensitive_case_sensitive,
                pii_enable, pii_repl_default, pii_repl_map,
                normalize_modes],
            outputs=[clean_status]
        )

        # 敏感词试运行绑定
        sensitive_preview_btn.click(
            fn=self._preview_sensitive_processing,
            inputs=[sensitive_preview_text, clean_sensitive_words, clean_sensitive_action, clean_sensitive_replacement,
                    clean_sensitive_fields, clean_sensitive_exclude_fields, clean_sensitive_field_policies,
                    clean_sensitive_use_regex, clean_sensitive_case_sensitive],
            outputs=[sensitive_preview_result]
        )
    
    def _create_model_tab(self):
        """创建模型配置标签页"""
        gr.Markdown("## 模型配置管理")
        gr.Markdown("支持 vLLM、OpenAI、SGlang、Ollama 等多种模型类型")
        
        with gr.Row():
            with gr.Column(scale=1):
                # 模型配置区域
                gr.Markdown("### ➕ 添加新模型")
                
                model_name = gr.Textbox(
                    label="模型名称",
                    placeholder="例如：gpt-4 或 local-llama",
                    info="为模型设置一个唯一标识名称"
                )
                
                model_type = gr.Dropdown(
                    choices=["vllm", "openai", "sglang", "ollama"],
                    value="openai",
                    label="模型类型",
                    info="选择模型的部署类型"
                )
                
                model_url = gr.Textbox(
                    label="模型URL",
                    placeholder="例如：http://localhost:8000/v1 或 https://api.openai.com/v1",
                    info="模型服务的API地址"
                )
                
                model_api_key = gr.Textbox(
                    label="API密钥（可选）",
                    type="password",
                    placeholder="输入API密钥",
                    info="某些模型需要API密钥认证"
                )
                
                model_model_name = gr.Textbox(
                    label="实际模型名",
                    placeholder="例如：gpt-4 或 llama-2-7b",
                    info="API调用时使用的模型名称"
                )
                
                with gr.Row():
                    add_model_btn = gr.Button("➕ 添加模型", variant="primary")
                    test_all_btn = gr.Button("🔧 测试所有模型", variant="secondary")
            
            with gr.Column(scale=2):
                # 模型列表区域
                gr.Markdown("### 📋 已配置模型列表")
                
                model_list = gr.Dataframe(
                    headers=["模型名称", "类型", "状态", "URL", "响应时间", "操作"],
                    datatype=["str", "str", "str", "str", "str", "str"],
                    label="",
                    interactive=False,
                    wrap=True
                )
                
                with gr.Row():
                    test_model_btn = gr.Button("🔧 测试模型", size="sm")
                    delete_model_btn = gr.Button("🗑️ 删除模型", size="sm", variant="stop")
                    refresh_model_btn = gr.Button("🔄 刷新列表", size="sm")
                
                selected_model_name = gr.Textbox(
                    label="选中模型",
                    placeholder="点击模型行选择",
                    interactive=False
                )
        
        # 状态输出区域
        with gr.Row():
            model_status = gr.Textbox(
                label="状态信息",
                lines=4,
                interactive=False,
                show_copy_button=True
            )
        
        # 存储组件引用
        self.components['model'] = {
            'model_name': model_name,
            'model_type': model_type,
            'model_url': model_url,
            'model_api_key': model_api_key,
            'model_model_name': model_model_name,
            'model_list': model_list,
            'selected_model_name': selected_model_name,
            'status': model_status
        }
        
        # 绑定事件处理器
        add_model_btn.click(
            fn=self._add_model,
            inputs=[model_name, model_type, model_url, model_api_key, model_model_name],
            outputs=[model_status, model_list]
        )
        
        test_all_btn.click(
            fn=self._test_all_models,
            outputs=[model_status, model_list]
        )
        
        test_model_btn.click(
            fn=self._test_model,
            inputs=[selected_model_name],
            outputs=[model_status, model_list]
        )
        
        delete_model_btn.click(
            fn=self._delete_model,
            inputs=[selected_model_name],
            outputs=[model_status, model_list]
        )
        
        refresh_model_btn.click(
            fn=self._refresh_models,
            outputs=[model_list]
        )
        
        # 模型列表点击事件
        model_list.select(
            fn=self._select_model,
            outputs=[selected_model_name]
        )
    
    def _create_distill_tab(self):
        """创建蒸馏生成标签页"""
        gr.Markdown("## 蒸馏生成管理")
        gr.Markdown("基于AI模型生成高质量的训练数据")
        
        with gr.Row():
            with gr.Column(scale=1):
                # 蒸馏配置区域
                gr.Markdown("### 📝 生成配置")
                
                distill_source = gr.File(
                    label="源数据文件",
                    file_types=[".jsonl", ".json"]
                )
                
                distill_strategy = gr.Dropdown(
                    choices=[
                        ("数据扩充", "expand"),
                        ("内容增强", "enhance"),
                        ("文本改写", "paraphrase"),
                        ("分类标注", "classify_label"),
                        ("从Q生A", "q_to_a"),
                        ("自定义", "custom")
                    ],
                    value="expand",
                    label="生成策略",
                    info="选择数据生成的策略类型"
                )
                
                distill_model = gr.Dropdown(
                    label="选择模型",
                    choices=[],
                    info="选择用于生成的AI模型"
                )
                
                refresh_models_btn = gr.Button("🔄 刷新模型列表", size="sm")
                
                distill_count = gr.Slider(
                    minimum=1,
                    maximum=50,
                    value=5,
                    step=1,
                    label="生成数量",
                    info="每个输入样本生成的数量"
                )
                
                distill_temperature = gr.Slider(
                    minimum=0.1,
                    maximum=2.0,
                    value=0.7,
                    step=0.1,
                    label="温度参数",
                    info="控制生成的随机性，值越高越随机"
                )
                
                distill_max_tokens = gr.Slider(
                    minimum=100,
                    maximum=4000,
                    value=2048,
                    step=100,
                    label="最大Token数",
                    info="限制生成文本的最大长度（将根据模型类型动态调整上限）"
                )

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

                with gr.Accordion("⚙️ 并发与性能", open=False):
                    concurrency_workers = gr.Slider(
                        minimum=1,
                        maximum=128,
                        value=8,
                        step=1,
                        label="并发度（同时请求数）",
                        info="根据模型吞吐与限流调整；JSONL 大任务建议 8~64 之间"
                    )
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
                with gr.Accordion("📘 策略说明与提示词", open=True):
                    strategy_desc = gr.Markdown("*选择策略后显示说明与参数提示*")

                    system_prompt_box = gr.Textbox(
                        label="System 提示词",
                        lines=3,
                        placeholder="可选：用于约束整体风格、禁则等"
                    )
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

                with gr.Accordion("🧩 字段与目标输出", open=False):
                    with gr.Row():
                        dataset_fields_box = gr.CheckboxGroup(
                            label="从数据文件中检测到的字段",
                            choices=[],
                            value=[],
                            interactive=True,
                            info="选择需要参与生成/改写的字段（将同步到下方文本框）"
                        )
                    selected_fields_input = gr.Textbox(
                        label="选定字段（逗号分隔）",
                        placeholder="例如：instruction,output 或 question,answer"
                    )
                    q_field_name_input = gr.Textbox(
                        label="Q 字段名（输出数据中使用）",
                        value="instruction",
                        placeholder="默认 instruction，可自定义为 question 等"
                    )
                    label_set_input = gr.Textbox(
                        label="标签集合（仅分类标注，逗号分隔）",
                        visible=False,
                        placeholder="例如：正向,负向,中立"
                    )
                    target_field_input = gr.Textbox(
                        label="A 字段名（目标字段）",
                        value="output",
                        placeholder="生成内容写入的字段名，默认 output"
                    )
                
                with gr.Row():
                    start_distill_btn = gr.Button("🧠 开始生成", variant="primary")
                    pause_distill_btn = gr.Button("⏸️ 暂停生成", variant="secondary")
            
            with gr.Column(scale=2):
                # 任务状态区域
                gr.Markdown("### 📊 生成进度")
                
                distill_progress = gr.Progress()
                
                distill_task_list = gr.Dataframe(
                    headers=["任务ID", "策略", "模型", "状态", "进度", "开始时间"],
                    datatype=["str", "str", "str", "str", "str", "str"],
                    label="蒸馏任务列表",
                    interactive=False,
                    wrap=True
                )
                
                with gr.Row():
                    refresh_distill_btn = gr.Button("🔄 刷新任务", size="sm")
                    view_report_btn = gr.Button("📋 查看报告", size="sm")
                    resume_task_btn = gr.Button("⏯️ 恢复任务", size="sm")
                
                selected_distill_task = gr.Textbox(
                    label="选中任务ID",
                    placeholder="点击任务行选择",
                    interactive=False
                )

                with gr.Accordion("⏯️ 恢复与覆盖参数", open=False):
                    resume_model_override = gr.Dropdown(label="覆盖模型（可选）", choices=[], interactive=True)
                    resume_workers = gr.Slider(minimum=1, maximum=128, value=8, step=1, label="并发度（覆盖可选）")
                    resume_temp = gr.Slider(minimum=0.1, maximum=2.0, value=0.7, step=0.1, label="温度（覆盖可选）")
                    resume_max_tokens = gr.Slider(minimum=100, maximum=4000, value=2048, step=100, label="最大Token（覆盖可选）")
                    resume_top_p = gr.Slider(minimum=0.1, maximum=1.0, value=0.9, step=0.05, label="top_p（覆盖可选）")
                    resume_top_k = gr.Slider(minimum=0, maximum=200, value=0, step=1, label="top_k（覆盖可选，0=不变）")
                    resume_rate_limit_rps = gr.Number(label="限流RPS（覆盖）", value=None, precision=2)
                    resume_max_backoff = gr.Number(label="最大退避（覆盖）", value=None, precision=2)
                    resume_as_new_checkbox = gr.Checkbox(label="克隆为新任务继续（resume-as-new）", value=False)
        
        # 状态输出区域
        with gr.Row():
            distill_status = gr.Textbox(
                label="状态信息",
                lines=5,
                interactive=False,
                show_copy_button=True
            )
        
        # 存储组件引用
        self.components['distill'] = {
            'source': distill_source,
            'strategy': distill_strategy,
            'model': distill_model,
            'count': distill_count,
            'temperature': distill_temperature,
            'max_tokens': distill_max_tokens,
            'top_p': distill_top_p,
            'top_k': distill_top_k,
            'strategy_desc': strategy_desc,
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
            'unordered_write': unordered_write_checkbox,
            'rate_limit_rps': rate_limit_rps_number,
            'max_backoff': max_backoff_number
        }
        
        # 绑定事件处理器
        refresh_models_btn.click(
            fn=self._refresh_distill_models,
            outputs=[distill_model]
        )

        # 恢复参数模型下拉同步
        refresh_models_btn.click(
            fn=self._refresh_distill_models,
            outputs=[resume_model_override]
        )

        # 模型变更时，动态调整最大token上限
        distill_model.change(
            fn=self._on_distill_model_change,
            inputs=[distill_model],
            outputs=[distill_max_tokens]
        )
        
        start_distill_btn.click(
            fn=self._start_distill_generation,
            inputs=[distill_source, distill_strategy, distill_model, distill_count,
                   distill_temperature, distill_max_tokens, distill_top_p, distill_top_k,
                   concurrency_workers, fsync_interval_slider, checkpoint_interval_slider, inflight_multiplier_slider,
                   unordered_write_checkbox, rate_limit_rps_number, max_backoff_number,
                   system_prompt_box, q_prompt_box, a_prompt_box,
                   selected_fields_input, q_field_name_input, label_set_input, target_field_input],
            outputs=[distill_status, distill_task_list]
        )
        
        pause_distill_btn.click(
            fn=self._pause_distill_generation,
            inputs=[selected_distill_task],
            outputs=[distill_status, distill_task_list]
        )
        
        refresh_distill_btn.click(
            fn=self._refresh_distill_tasks,
            outputs=[distill_task_list]
        )
        
        view_report_btn.click(
            fn=self._view_distill_report,
            inputs=[selected_distill_task],
            outputs=[distill_status]
        )

        resume_task_btn.click(
            fn=self._resume_distill_task,
            inputs=[selected_distill_task, resume_model_override, resume_workers, resume_temp, resume_max_tokens, resume_top_p, resume_top_k, resume_rate_limit_rps, resume_max_backoff, resume_as_new_checkbox],
            outputs=[distill_status, distill_task_list]
        )

        # 选择文件后，自动扫描字段并填充复选框
        distill_source.change(
            fn=self._on_distill_source_change,
            inputs=[distill_source],
            outputs=[dataset_fields_box, selected_fields_input]
        )

        # 勾选字段时，同步到文本框
        dataset_fields_box.change(
            fn=self._sync_selected_fields_text,
            inputs=[dataset_fields_box],
            outputs=[selected_fields_input]
        )
        
        # 任务列表点击事件
        distill_task_list.select(
            fn=self._select_distill_task,
            outputs=[selected_distill_task]
        )

        # 策略切换时更新说明与可见性
        distill_strategy.change(
            fn=self._on_strategy_change,
            inputs=[distill_strategy],
            outputs=[strategy_desc, distill_count, q_prompt_box, a_prompt_box, label_set_input]
        )
    
    def _create_manage_tab(self):
        """创建数据管理标签页"""
        gr.Markdown("## 数据管理中心")
        gr.Markdown("统一管理所有数据集，支持按类型查看和预览操作")
        
        # 筛选和搜索区域
        with gr.Row():
            data_type_filter = gr.Dropdown(
                choices=["全部", "原始数据", "处理数据", "蒸馏数据"],
                value="全部",
                label="数据类型筛选",
                info="按数据类型筛选显示",
                scale=2
            )
            
            dataset_name_search = gr.Textbox(
                label="数据集名称搜索",
                placeholder="输入数据集名称关键词",
                info="模糊搜索数据集名称",
                scale=2
            )
            
            search_dataset_btn = gr.Button("🔍 搜索", size="sm", scale=1)
        
        # 数据集列表区域
        gr.Markdown("### 📂 数据集列表")
        gr.Markdown("💡 **操作提示**：点击表格行选择数据集 • 表格支持水平滚动查看完整内容 • 悬停单元格显示完整文本")
        gr.Markdown("⚠️ **删除警告**：删除数据会永久删除数据集文件或整个数据集目录，请谨慎操作！")
                
        dataset_list = gr.Dataframe(
            headers=["名称", "类型", "大小", "创建时间", "路径"],
            datatype=["str", "str", "str", "str", "str"],
            label="",
            interactive=False,
            wrap=True
        )
        
        # 操作按钮区域 - 放在列表下方
        with gr.Row():
            refresh_data_btn = gr.Button("🔄 刷新列表", size="sm")
            preview_data_btn = gr.Button("👁️ 预览数据", size="sm")
            delete_data_btn = gr.Button("🗑️ 危险删除", size="sm", variant="stop")
        
        selected_dataset = gr.Textbox(
            label="选中数据集",
            placeholder="点击数据集行选择",
            interactive=False
        )
        
        # 数据预览区域 - 改为下方完整区域
        gr.Markdown("### 👁️ 数据预览")
        
        with gr.Row():
            with gr.Column(scale=1):
                # 预览控制选项
                preview_rows = gr.Slider(
                    minimum=10,
                    maximum=1000,
                    value=100,
                    step=10,
                    label="预览行数",
                    info="设置要预览的数据行数"
                )
                
                auto_preview = gr.Checkbox(
                    label="自动预览",
                    value=True,
                    info="选择数据集时自动预览"
                )
                
                text_truncation = gr.Checkbox(
                    label="文本截断",
                    value=True,
                    info="对长文本进行智能截断"
                )
                
                max_text_length = gr.Slider(
                    minimum=50,
                    maximum=1000,
                    value=300,
                    step=50,
                    label="文本截断长度",
                    info="超过此长度的文本将被截断"
                )
            
            with gr.Column(scale=2):
                # 字段选择区域
                gr.Markdown("#### 📋 字段选择")
                gr.Markdown("💡 选择要显示的字段，实时更新预览表格")
                
                with gr.Row():
                    select_all_fields = gr.Button("全选", size="sm", variant="secondary")
                    clear_all_fields = gr.Button("清除", size="sm", variant="secondary")
                    select_common_fields = gr.Button("常用字段", size="sm", variant="primary")
                
                # 字段选择框 - 这里是核心组件
                field_selector = gr.CheckboxGroup(
                    label="可用字段",
                    choices=[],
                    value=[],
                    interactive=True,
                    info="选择要在预览中显示的字段"
                )
        
        # 数据集信息显示
        with gr.Row():
            dataset_info = gr.Markdown("*选择数据集以显示详细信息*")
        
        # 预览结果显示 - 改进为更大的显示区域
        result_display = gr.Dataframe(
            label="数据内容预览",
            interactive=False,
            wrap=True
        )
        
        # 展开/折叠文本控制
        with gr.Row():
            with gr.Column():
                expanded_text = gr.Textbox(
                    label="完整文本内容",
                    lines=10,
                    interactive=False,
                    visible=False,
                    info="点击预览表格中的截断文本可在此处查看完整内容"
                )
                expand_text_btn = gr.Button("查看完整文本", visible=False, size="sm")
        
        # 操作状态显示区域 - 移到最下面
        with gr.Row():
            data_status = gr.Textbox(
                label="操作状态",
                lines=3,
                interactive=False,
                show_copy_button=True,
                info="显示数据操作的状态信息"
            )
        
        # 存储组件引用
        self.components['manage'] = {
            'data_type_filter': data_type_filter,
            'dataset_name_search': dataset_name_search,
            'search_dataset_btn': search_dataset_btn,
            'dataset_list': dataset_list,
            'selected_dataset': selected_dataset,
            'preview_rows': preview_rows,
            'auto_preview': auto_preview,
            'text_truncation': text_truncation,
            'max_text_length': max_text_length,
            'field_selector': field_selector,
            'select_all_fields': select_all_fields,
            'clear_all_fields': clear_all_fields,
            'select_common_fields': select_common_fields,
            'dataset_info': dataset_info,
            'result_display': result_display,
            'data_status': data_status,
            'expanded_text': expanded_text,
            'expand_text_btn': expand_text_btn
        }
        
        # 缓存当前数据集的数据和字段信息，避免重复读取
        self.current_dataset_cache = {
            'path': None,
            'data': None,
            'fields': [],
            'original_preview': None
        }
        
        # 绑定事件处理器
        data_type_filter.change(
            fn=self._filter_datasets,
            inputs=[data_type_filter, dataset_name_search],
            outputs=[dataset_list]
        )
        
        search_dataset_btn.click(
            fn=self._filter_datasets,
            inputs=[data_type_filter, dataset_name_search],
            outputs=[dataset_list]
        )
        
        refresh_data_btn.click(
            fn=self._refresh_datasets,
            inputs=[data_type_filter, dataset_name_search],
            outputs=[dataset_list]
        )
        
        preview_data_btn.click(
            fn=self._preview_dataset_with_field_filter,
            inputs=[selected_dataset, preview_rows, text_truncation, max_text_length, field_selector],
            outputs=[result_display, data_status]
        )
        
        delete_data_btn.click(
            fn=self._delete_dataset,
            inputs=[selected_dataset, data_type_filter],
            outputs=[data_status, dataset_list, selected_dataset, dataset_info, result_display]
        )
        
        # 数据集列表点击事件 - 支持自动预览和字段加载
        dataset_list.select(
            fn=self._select_dataset_with_auto_preview,
            inputs=[auto_preview, preview_rows, text_truncation, max_text_length],
            outputs=[selected_dataset, dataset_info, result_display, field_selector, data_status]
        )
        
        # 字段选择器变化事件 - 实时更新预览
        field_selector.change(
            fn=self._update_preview_by_fields,
            inputs=[field_selector, preview_rows, text_truncation, max_text_length],
            outputs=[result_display, data_status]
        )
        
        # 字段选择按钮事件
        select_all_fields.click(
            fn=self._select_all_dataset_fields,
            outputs=[field_selector]
        )
        
        clear_all_fields.click(
            fn=self._clear_all_dataset_fields,
            outputs=[field_selector]
        )
        
        select_common_fields.click(
            fn=self._select_common_dataset_fields,
            outputs=[field_selector]
        )
    
    def _start_format_convert_async(self, source_file, target_format: str, output_dir: str) -> Tuple[pd.DataFrame, str]:
        """开始格式转换 - 异步版本"""
        try:
            if source_file is None:
                return pd.DataFrame(), "❌ 请选择源文件"
            
            source_path = source_file.name
            if not os.path.exists(source_path):
                return pd.DataFrame(), "❌ 源文件不存在"
            
            # 创建输出目录
            os.makedirs(output_dir, exist_ok=True)
            
            # 创建转换任务 - 改进：不使用子目录，直接输出到指定目录
            task_id = convert_format(
                source_path=source_path,
                target_format=target_format,
                output_dir=output_dir,
                use_subdirectory=False  # 直接输出到指定目录，避免复杂路径
            )
            
            # 启动任务
            success = start_convert(task_id)
            if not success:
                return pd.DataFrame(), f"❌ 启动转换任务失败，任务ID: {task_id}"
            
            # 返回更新后的任务列表
            task_df = self._get_convert_tasks_df()
            
            return task_df, f"✅ 转换任务已启动！\n任务ID: {task_id}\n文件: {os.path.basename(source_path)}\n目标格式: {target_format.upper()}"
            
        except Exception as e:
            self.logger.error(f'启动格式转换失败: {e}')
            return pd.DataFrame(), f"❌ 启动转换失败: {str(e)}"
    
    def _get_convert_tasks_df(self) -> pd.DataFrame:
        """获取转换任务列表"""
        try:
            tasks = list_converts()
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
                    # 根据新的命名规则构建文件名
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
            tasks = list_converts()
            full_task_id = None
            for task in tasks:
                if task.get('task_id', '').startswith(task_id.replace('...', '')):
                    full_task_id = task.get('task_id')
                    break
            
            if not full_task_id:
                return f"❌ 未找到任务: {task_id}"
            
            progress = get_convert_progress(full_task_id)
            status = progress.get('status', 'unknown')
            
            if status == 'completed':
                # 从任务进度中获取实际的输出文件路径
                output_file_path = progress.get('output_file')
                
                if output_file_path and Path(output_file_path).exists():
                    output_file = Path(output_file_path)
                    file_size = output_file.stat().st_size / (1024 * 1024)  # MB
                    return f"""✅ 转换完成！
📁 输出文件: {output_file.name}
📂 完整路径: {output_file}
📊 文件大小: {file_size:.2f} MB
📈 处理行数: {progress.get('processed_rows', 'N/A')}
🕐 任务ID: {full_task_id}

💡 文件已保存，可以直接打开使用"""
                else:
                    # 如果进度中没有输出路径，尝试推测（向后兼容）
                    source_path = ""
                    target_format = ""
                    for task in tasks:
                        if task.get('task_id') == full_task_id:
                            source_path = task.get('source_path', '')
                            target_format = task.get('target_format', '')
                            break
                    
                    if source_path and target_format:
                        # 尝试两种可能的路径：有子目录和无子目录
                        source_stem = Path(source_path).stem
                        source_ext = Path(source_path).suffix.replace('.', '')
                        target_ext = 'md' if target_format.lower() == 'markdown' else target_format.lower()
                        output_filename = f"{source_stem}_{source_ext}2{target_ext}.{target_ext}"
                        
                        # 优先检查有任务ID子目录的路径
                        output_dir_with_id = Path(self.root_dir / "processed") / full_task_id
                        output_file_with_id = output_dir_with_id / output_filename
                        
                        # 其次检查无子目录的路径
                        output_dir_direct = Path(self.root_dir / "processed")
                        output_file_direct = output_dir_direct / output_filename
                        
                        if output_file_with_id.exists():
                            output_file = output_file_with_id
                            file_size = output_file.stat().st_size / (1024 * 1024)  # MB
                            return f"""✅ 转换完成！
📁 输出文件: {output_filename}
📂 完整路径: {output_file}
📊 文件大小: {file_size:.2f} MB
📈 处理行数: {progress.get('processed_rows', 'N/A')}
🕐 任务ID: {full_task_id}

💡 文件已保存，可以直接打开使用"""
                        elif output_file_direct.exists():
                            output_file = output_file_direct
                            file_size = output_file.stat().st_size / (1024 * 1024)  # MB
                            return f"""✅ 转换完成！
📁 输出文件: {output_filename}
📂 完整路径: {output_file}
📊 文件大小: {file_size:.2f} MB
📈 处理行数: {progress.get('processed_rows', 'N/A')}
🕐 任务ID: {full_task_id}

💡 文件已保存，可以直接打开使用"""
                        else:
                            return f"""❌ 转换完成但文件不存在
检查路径:
1. {output_file_with_id}
2. {output_file_direct}
任务ID: {full_task_id}"""
                    else:
                        return "❌ 无法获取任务详细信息"
            
            elif status == 'failed':
                error_msg = progress.get('error_msg', '未知错误')
                return f"❌ 转换失败\n错误: {error_msg}\n任务ID: {full_task_id}"
            
            elif status in ['pending', 'running']:
                progress_pct = progress.get('progress', 0)
                processed = progress.get('processed_rows', 0)
                speed = progress.get('speed', '计算中')
                eta = progress.get('eta', '未知')
                
                return f"""🔄 转换进行中...
📊 进度: {progress_pct}%
📈 已处理: {processed} 行
⚡ 速度: {speed}
⏱️ 预计剩余: {eta}
🆔 任务ID: {full_task_id}"""
            
            else:
                return f"❓ 未知状态: {status}\n任务ID: {full_task_id}"
                
        except Exception as e:
            self.logger.error(f'查看转换结果失败: {e}')
            return f"❌ 查看结果失败: {str(e)}"
    
    def _start_format_convert(self, source_file, target_format: str, output_dir: str) -> str:
        """开始格式转换 - 同步版本（兼容旧接口）"""
        task_df, message = self._start_format_convert_async(source_file, target_format, output_dir)
        return message
        """开始格式转换 - 修复版2"""
        try:
            if source_file is None:
                return "❌ 请选择源文件"
            
            source_path = source_file.name
            if not os.path.exists(source_path):
                return "❌ 源文件不存在"
            
            # 创建输出目录
            os.makedirs(output_dir, exist_ok=True)
            
            # 创建转换任务
            task_id = convert_format(
                source_path=source_path,
                target_format=target_format,
                output_dir=output_dir
            )
            
            # 启动任务
            success = start_convert(task_id)
            if not success:
                return f"❌ 启动转换任务失败，任务ID: {task_id}"
            
            # 等待任务完成（简单的同步等待）
            max_wait = 120  # 最多等待120秒（大文件需要更多时间）
            wait_time = 0
            
            while wait_time < max_wait:
                progress = get_convert_progress(task_id)
                status = progress.get('status', 'unknown')
                
                if status == 'completed':
                    # 根据任务ID构建输出文件路径
                    expected_output_dir = Path(output_dir) / task_id
                    expected_output_file = expected_output_dir / f"converted.{target_format}"
                    
                    if expected_output_file.exists():
                        return f"""✅ 格式转换完成！
📁 输出文件: converted.{target_format}
📂 保存位置: {expected_output_file}
📊 处理行数: {progress.get('processed_rows', 'N/A')}
🕐 转换时间: {progress.get('duration', 'N/A')}
📋 任务ID: {task_id}

💡 提示：文件已保存到 {expected_output_file}"""
                    else:
                        # 尝试查找输出目录中的文件
                        if expected_output_dir.exists():
                            output_files = list(expected_output_dir.glob(f"*.{target_format}"))
                            if output_files:
                                actual_file = output_files[0]
                                return f"""✅ 格式转换完成！
📁 输出文件: {actual_file.name}
📂 保存位置: {actual_file}
📊 处理行数: {progress.get('processed_rows', 'N/A')}
🕐 转换时间: {progress.get('duration', 'N/A')}
� 任务ID: {task_id}"""
                        
                        return f"❌ 转换完成但找不到输出文件\n预期位置: {expected_output_file}\n任务ID: {task_id}"
                        
                elif status == 'failed':
                    error_msg = progress.get('error_msg', '未知错误')
                    return f"❌ 转换失败: {error_msg}\n任务ID: {task_id}"
                
                elif status in ['pending', 'running']:
                    # 显示进度
                    progress_pct = progress.get('progress', 0)
                    processed = progress.get('processed_rows', 0)
                    if wait_time % 5 == 0:  # 每5秒更新一次UI反馈
                        self.logger.info(f"转换进度: {progress_pct}%, 已处理: {processed} 行")
                    time.sleep(1)
                    wait_time += 1
                else:
                    return f"❌ 未知任务状态: {status}\n任务ID: {task_id}"
            
            return f"⏳ 转换超时，请检查任务状态\n任务ID: {task_id}\n预期输出目录: {Path(output_dir) / task_id}"
            
        except Exception as e:
            self.logger.error(f'格式转换失败: {e}')
            return f"❌ 转换失败: {str(e)}"
    
    def _reset_field_selection(self):
        """重置字段选择状态"""
        self.logger.info("🔄 重置字段选择状态")
        return (
            gr.CheckboxGroup(choices=[], value=[], label="选择字段", info="请先预览字段"),
            gr.Dataframe(
                headers=["原字段名", "新字段名"],
                datatype=["str", "str"],
                row_count=0,
                col_count=(2, "fixed"),
                interactive=True,
                label="字段映射表",
                visible=False
            )
        )
    
    def _preview_extract_fields(self, source_file) -> gr.CheckboxGroup:
        """预览可提取的字段"""
        # 重置状态，清除之前的选择
        self.logger.info(f"预览字段，文件: {source_file.name if source_file else None}")
        try:
            if source_file is None:
                return gr.CheckboxGroup(choices=[], value=[], label="选择字段")
            
            source_path = source_file.name
            if not os.path.exists(source_path):
                return gr.CheckboxGroup(choices=[], value=[], label="选择字段 - 文件不存在")
            
            # 直接获取字段名称列表
            field_names = self._get_simple_field_names(source_path)
            
            if not field_names:
                return gr.CheckboxGroup(choices=[], value=[], label="选择字段 - 未找到字段")
            
            # 明确重置选择状态
            return gr.CheckboxGroup(
                choices=field_names, 
                value=[],  # 明确设置为空列表
                label=f"选择字段 (共{len(field_names)}个字段)",
                info="选择要提取的字段"
            )
            
        except Exception as e:
            self.logger.error(f'预览字段失败: {e}')
            return gr.CheckboxGroup(
                choices=[], 
                value=[],
                label=f"选择字段 - 错误: {str(e)}"
            )
    
    def _get_simple_field_names(self, file_path: str) -> List[str]:
        """获取文件的字段名称列表（通用版本）"""
        try:
            # 使用通用字段提取器获取字段名
            return get_field_names_universal(file_path)
        except Exception as e:
            self.logger.error(f'获取字段名称失败: {e}')
            return []
    
    def _start_field_extract(self, source_file, fields: List[str], output_dir: str) -> str:
        """开始字段提取（通用版本）"""
        try:
            if source_file is None:
                return "❌ 请选择源文件"

            if not fields:
                return "❌ 请选择要提取的字段"

            source_path = source_file.name
            if not os.path.exists(source_path):
                return "❌ 源文件不存在"

            try:
                # 使用通用字段提取器
                self.logger.info(f"开始字段提取: {fields}")
                result_path = extract_fields_universal(
                    source_path=source_path,
                    fields=fields,
                    output_dir=output_dir or str(self.root_dir / 'processed')
                )

                if result_path and os.path.exists(result_path):
                    file_size = os.path.getsize(result_path)
                    self.logger.info(f"字段提取完成: {result_path}")
                    return f"✅ 字段提取完成！\\n提取字段: {', '.join(fields)}\\n输出文件: {result_path}\\n文件大小: {file_size:,} 字节"
                else:
                    return f"❌ 字段提取失败\\n请检查源文件格式和选择的字段"

            except Exception as e:
                self.logger.error(f'字段提取异常: {e}')
                return f"❌ 提取异常: {str(e)}"
            
        except Exception as e:
            self.logger.error(f'字段提取失败: {e}')
            return f"❌ 提取失败: {str(e)}"
    
    def _update_field_mapping(self, selected_fields) -> gr.Dataframe:
        """根据选择的字段更新字段映射表"""
        try:
            if not selected_fields:
                return gr.Dataframe(
                    value=[],
                    headers=["原字段名", "新字段名"],
                    visible=False
                )
            
            # 创建映射表数据，默认新字段名和原字段名相同
            mapping_data = [[field, field] for field in selected_fields]
            
            return gr.Dataframe(
                value=mapping_data,
                headers=["原字段名", "新字段名"],
                datatype=["str", "str"],
                interactive=True,
                visible=True
            )
            
        except Exception as e:
            self.logger.error(f'更新字段映射失败: {e}')
            return gr.Dataframe(
                value=[],
                headers=["原字段名", "新字段名"],
                visible=False
            )
    
    def _start_field_extract_with_mapping(self, source_file, fields, field_mapping_df, output_dir: str) -> str:
        """开始字段提取（支持字段重命名和进度显示）"""
        try:
            if source_file is None:
                return "❌ 请选择源文件"

            if not fields or len(fields) == 0:
                return "❌ 请先预览字段并选择要提取的字段"

            source_path = source_file.name
            if not os.path.exists(source_path):
                return "❌ 源文件不存在"

            # 创建进度状态变量
            progress_status = {"message": "🚀 准备开始字段提取...", "percent": 0}
            
            def progress_callback(message, percent):
                """进度回调函数"""
                progress_status["message"] = message
                progress_status["percent"] = percent
                self.logger.info(f"提取进度: {percent}% - {message}")

            try:
                # 处理字段映射
                field_mapping = {}
                
                # 安全处理field_mapping_df，可能是DataFrame、列表或None
                mapping_data = []
                if field_mapping_df is not None:
                    try:
                        # 如果是pandas DataFrame
                        if hasattr(field_mapping_df, 'values'):
                            mapping_data = field_mapping_df.values.tolist()
                        # 如果是列表
                        elif isinstance(field_mapping_df, list):
                            mapping_data = field_mapping_df
                        # 如果是其他可迭代对象
                        elif hasattr(field_mapping_df, '__iter__'):
                            mapping_data = list(field_mapping_df)
                        
                        self.logger.info(f"映射数据类型: {type(field_mapping_df)}")
                        self.logger.info(f"处理后的映射数据: {mapping_data}")
                        
                    except Exception as e:
                        self.logger.error(f"处理映射数据失败: {e}")
                        mapping_data = []
                
                # 解析映射数据
                if mapping_data:
                    for row in mapping_data:
                        try:
                            if isinstance(row, (list, tuple)) and len(row) >= 2:
                                original_field = str(row[0]).strip() if row[0] else ""
                                new_field = str(row[1]).strip() if row[1] else ""
                                
                                if original_field and new_field:
                                    field_mapping[original_field] = new_field
                                    self.logger.info(f"字段映射: {original_field} -> {new_field}")
                        except Exception as e:
                            self.logger.error(f"处理映射行失败: {row}, 错误: {e}")
                            continue

                # 使用通用字段提取器（带进度回调）
                self.logger.info(f"🔍 字段提取调试信息:")
                self.logger.info(f"   源文件: {source_path}")
                self.logger.info(f"   选中字段列表: {fields}")
                self.logger.info(f"   字段类型: {type(fields)}")
                self.logger.info(f"   字段长度: {len(fields) if fields else 0}")
                self.logger.info(f"   字段映射: {field_mapping}")
                
                result_path = extract_fields_universal(
                    source_path=source_path,
                    fields=fields,
                    output_dir=output_dir or str(self.root_dir / 'processed'),
                    field_mapping=field_mapping,
                    progress_callback=progress_callback
                )

                if result_path and os.path.exists(result_path):
                    file_size = os.path.getsize(result_path)
                    self.logger.info(f"字段提取完成: {result_path}")
                    
                    # 显示映射信息
                    mapping_info = ""
                    if field_mapping:
                        mapping_list = [f"{k} -> {v}" for k, v in field_mapping.items()]
                        mapping_info = f"\\n字段映射: {', '.join(mapping_list)}"
                    
                    return f"✅ 字段提取完成！\\n提取字段: {', '.join(fields)}{mapping_info}\\n输出文件: {result_path}\\n文件大小: {file_size:,} 字节"
                else:
                    return f"❌ 字段提取失败\\n请检查源文件格式和选择的字段"

            except Exception as e:
                self.logger.error(f'字段提取异常: {e}')
                return f"❌ 提取异常: {str(e)}"
            
        except Exception as e:
            self.logger.error(f'字段提取失败: {e}')
            return f"❌ 提取失败: {str(e)}"
    
    def _start_field_extract_with_progress(self, source_file, fields, field_mapping_df, output_dir: str) -> str:
        """开始字段提取（带进度显示）"""
        import time
        
        try:
            if source_file is None:
                return "❌ 请选择源文件"

            if not fields:
                return "❌ 请选择要提取的字段"

            source_path = source_file.name
            if not os.path.exists(source_path):
                return "❌ 源文件不存在"

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
                        
                    except Exception as e:
                        progress_callback(f"⚠️ 映射数据处理异常: {e}", 5)
                        mapping_data = []
                
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

                # 使用通用字段提取器（带进度回调）
                progress_callback("🔄 启动字段提取引擎...", 10)
                
                result_path = extract_fields_universal(
                    source_path=source_path,
                    fields=fields,
                    output_dir=output_dir or str(self.root_dir / 'processed'),
                    field_mapping=field_mapping,
                    progress_callback=progress_callback
                )

                if result_path and os.path.exists(result_path):
                    file_size = os.path.getsize(result_path)
                    
                    # 构建详细的结果报告
                    mapping_info = ""
                    if field_mapping:
                        mapping_list = [f"{k} → {v}" for k, v in field_mapping.items()]
                        mapping_info = f"\\n📋 字段映射: {', '.join(mapping_list)}"
                    
                    # 合并进度日志
                    progress_summary = "\\n".join(progress_log)
                    
                    final_result = f"""✅ 字段提取任务完成！

📊 提取详情:
• 选择字段: {', '.join(fields)}{mapping_info}
• 输出文件: {result_path}  
• 文件大小: {file_size:,} 字节

📈 执行日志:
{progress_summary}

🎉 任务执行成功！"""
                    
                    self.logger.info(f"字段提取完成: {result_path}")
                    return final_result
                else:
                    error_summary = "\\n".join(progress_log)
                    return f"""❌ 字段提取失败

执行日志:
{error_summary}

请检查源文件格式和选择的字段"""

            except Exception as e:
                error_summary = "\\n".join(progress_log)
                self.logger.error(f'字段提取异常: {e}')
                return f"""❌ 提取过程异常: {str(e)}

执行日志:
{error_summary}"""
            
        except Exception as e:
            self.logger.error(f'字段提取失败: {e}')
            return f"❌ 提取失败: {str(e)}"
    
    def _add_merge_file(self, file) -> tuple:
        """添加文件到合并列表"""
        if file is None:
            return None, self._get_merge_file_list_display()
        
        file_path = file.name
        
        # 检查文件是否已存在
        if file_path in self.merge_file_paths:
            message = f"⚠️ 文件已在列表中: {os.path.basename(file_path)}"
            return None, message + "\n\n" + self._get_merge_file_list_display()
        
        # 检查文件是否存在
        if not os.path.exists(file_path):
            message = f"❌ 文件不存在: {file_path}"
            return None, message + "\n\n" + self._get_merge_file_list_display()
        
        # 添加文件到列表
        self.merge_file_paths.append(file_path)
        
        message = f"✅ 已添加文件: {os.path.basename(file_path)}"
        return None, message + "\n\n" + self._get_merge_file_list_display()
    
    def _clear_merge_files(self) -> str:
        """清空合并文件列表"""
        self.merge_file_paths = []
        return "🗑️ 文件列表已清空\n\n" + self._get_merge_file_list_display()
    
    def _get_merge_file_list_display(self) -> str:
        """获取文件列表显示内容"""
        if not self.merge_file_paths:
            return "📝 待合并文件列表（至少需要2个文件）:\n\n暂无文件"
        
        display_text = f"📝 待合并文件列表（共 {len(self.merge_file_paths)} 个文件）:\n\n"
        for i, path in enumerate(self.merge_file_paths, 1):
            file_name = os.path.basename(path)
            file_size = os.path.getsize(path) if os.path.exists(path) else 0
            size_mb = file_size / (1024 * 1024)
            display_text += f"{i}. {file_name} ({size_mb:.2f} MB)\n"
            display_text += f"   路径: {path}\n\n"
        
        return display_text.strip()

    def _start_data_merge(self, mode: str, dedup_field: str, output_dir: str) -> str:
        """开始数据合并"""
        try:
            if len(self.merge_file_paths) < 2:
                return "❌ 请添加至少2个文件进行合并"
            
            # 检查文件是否存在
            for path in self.merge_file_paths:
                if not os.path.exists(path):
                    return f"❌ 文件不存在: {path}"
            
            # 调用数据合并模块
            try:
                result_path = merge_data(
                    source_paths=self.merge_file_paths,
                    mode=mode,
                    output_dir=output_dir,
                    dedup_field=dedup_field.strip() if dedup_field.strip() else None
                )
            except Exception as merge_error:
                # 捕获合并过程中的具体错误
                error_msg = f"❌ 合并过程失败: {str(merge_error)}"
                self.logger.error(f'合并过程异常: {merge_error}')
                return error_msg
            
            if result_path:
                # 获取输出文件信息
                output_size = os.path.getsize(result_path) if os.path.exists(result_path) else 0
                output_size_mb = output_size / (1024 * 1024)
                
                merge_info = f"✅ 数据合并完成！\n\n"
                merge_info += f"📁 输出文件: {os.path.basename(result_path)}\n"
                merge_info += f"📍 完整路径: {result_path}\n"
                merge_info += f"📊 文件大小: {output_size_mb:.2f} MB\n"
                merge_info += f"🔗 合并模式: {mode}\n"
                merge_info += f"📝 源文件数量: {len(self.merge_file_paths)}\n"
                
                if dedup_field.strip():
                    merge_info += f"🔍 去重字段: {dedup_field.strip()}\n"
                
                # 显示合并的文件列表
                merge_info += f"\n� 已合并的源文件:\n"
                for i, path in enumerate(self.merge_file_paths, 1):
                    file_size = os.path.getsize(path) if os.path.exists(path) else 0
                    file_size_mb = file_size / (1024 * 1024)
                    merge_info += f"  {i}. {os.path.basename(path)} ({file_size_mb:.2f} MB)\n"
                
                merge_info += f"\n💡 提示: 合并后的文件已保存到指定目录，可直接使用！"
                
                return merge_info
            else:
                # result_path 为 None 或空字符串的情况
                debug_info = f"❌ 合并失败: 合并函数返回了空结果\n\n"
                debug_info += f"调试信息:\n"
                debug_info += f"- 源文件数量: {len(self.merge_file_paths)}\n"
                debug_info += f"- 合并模式: {mode}\n"
                debug_info += f"- 输出目录: {output_dir}\n"
                debug_info += f"- 去重字段: {dedup_field.strip() if dedup_field.strip() else '无'}\n"
                debug_info += f"- 源文件列表:\n"
                for i, path in enumerate(self.merge_file_paths, 1):
                    exists = os.path.exists(path)
                    debug_info += f"  {i}. {path} ({'存在' if exists else '不存在'})\n"
                
                self.logger.error(f'合并返回空结果 - 文件: {self.merge_file_paths}, 模式: {mode}')
                return debug_info
            
        except Exception as e:
            self.logger.error(f'数据合并失败: {e}')
            return f"❌ 合并失败: {str(e)}"
    
    def _start_data_clean(self, source_file, operations: List[str], empty_fields: str,
                          empty_mode: str, dedup_field: str, dedup_threshold: float,
                          sensitive_words: str, sensitive_action: str, sensitive_replacement: str,
                          sensitive_fields: str, sensitive_exclude_fields: str, sensitive_field_policies: str,
                          sensitive_use_regex: bool, sensitive_case_sensitive: bool,
                          pii_enable: List[str], pii_repl_default: str, pii_repl_map: str,
                          normalize_modes: List[str]) -> str:
        """开始数据清洗（新版参数联动）"""
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
            # 去空字段与策略 (仅当选择了 remove_empty)
            if 'remove_empty' in operations:
                if empty_fields and empty_fields.strip():
                    params['remove_empty_fields'] = [f.strip() for f in empty_fields.split(',') if f.strip()]
                if empty_mode:
                    params['empty_mode'] = empty_mode

            # 模糊去重
            if 'deduplicate' in operations and dedup_field and dedup_field.strip():
                params['dedup_field'] = dedup_field.strip()
                if dedup_threshold:
                    params['dedup_threshold'] = float(dedup_threshold)

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
                    # 解析: 字段:动作[:替换]
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
                                repl = ':'.join(parts[2:]).strip()  # 支持替换文本中再含冒号
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
                    # 解析  id_card:<ID>,phone:<TEL>
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

            # 旧版字段脱敏已移除，不再透传

            task_id = data_cleaner.start_clean(params)
            user_params = {k: v for k, v in params.items() if k not in ['source_path', 'operations']}
            # 进一步只展示当前操作真正相关的参数，减少混淆
            op_related_keys = set()
            if 'remove_empty' in operations:
                op_related_keys.update({'remove_empty_fields', 'empty_mode'})
            if 'deduplicate' in operations:
                op_related_keys.update({'dedup_field', 'dedup_threshold'})
            if 'filter_sensitive' in operations:
                op_related_keys.update({'sensitive_words', 'sensitive_action', 'sensitive_replacement', 'sensitive_fields',
                                        'sensitive_exclude_fields', 'sensitive_field_policies', 'sensitive_use_regex',
                                        'sensitive_case_sensitive'})
            if 'pii_desensitize' in operations:
                op_related_keys.update({'pii_enable', 'pii_replacements'})
            if 'normalize_text' in operations:
                op_related_keys.update({'normalize_modes'})
            # desensitize 已废弃
            filtered_params = {k: user_params[k] for k in user_params if k in op_related_keys}
            return ("✅ 数据清洗任务已启动！\n"
                    f"任务ID: {task_id}\n"
                    f"清洗操作: {', '.join(operations)}\n"
                    f"有效参数: {json.dumps(filtered_params, ensure_ascii=False)}")
        except Exception as e:
            self.logger.error(f'数据清洗失败: {e}')
            return f"❌ 清洗任务启动失败: {e}"

    def _preview_sensitive_processing(self, text: str, sensitive_words: str, action: str, replacement: str,
                                      sensitive_fields: str, sensitive_exclude_fields: str, field_policies: str,
                                      use_regex: bool, case_sensitive: bool) -> str:
        """对单条文本进行敏感词处理预览，不写入文件。

        逻辑：构造一个临时数据 dict，只包含一个字段 preview；根据用户参数执行 _process_sensitive，返回前后对比与命中统计。
        字段级策略如果包含 preview:动作[:替换] 会生效。
        """
        try:
            from .data_cleaner import data_cleaner as _dc  # 延迟导入避免循环
            if not text or not text.strip():
                return "⚠️ 请输入要预览的文本"
            words = [w.strip() for w in (sensitive_words or '').split(',') if w.strip()] or _dc.default_sensitive_words
            data = {'preview': text}
            params = {}
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
            hit, modified, dropped = _dc._process_sensitive(
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
                "词命中统计:" + json.dumps(detail['word_hits'], ensure_ascii=False)
            )
        except Exception as e:
            self.logger.error(f'敏感词试运行失败: {e}')
            return f"❌ 试运行失败: {e}"
    def _add_model(self, name: str, model_type: str, url: str, 
                  api_key: str, model_name: str) -> Tuple[str, pd.DataFrame]:
        """添加新模型"""
        try:
            if not name.strip():
                return "❌ 请输入模型名称", self._get_models_df()
            
            if not url.strip():
                return "❌ 请输入模型URL", self._get_models_df()
            
            if not model_name.strip():
                return "❌ 请输入实际模型名", self._get_models_df()
            
            # 重名预检查，提供更友好的错误提示
            existing = model_manager.get_all_models()
            if name.strip() in existing:
                return f"❌ 模型名称已存在，请更换名称或先删除: {name}", self._get_models_df()

            # 构建模型配置
            model_info = {
                'name': name.strip(),
                'type': model_type,
                'url': url.strip(),
                'model_name': model_name.strip()
            }
            
            if api_key.strip():
                model_info['api_key'] = api_key.strip()
            
            # 调用模型管理模块
            success = model_manager.add_model(model_info)
            
            if success:
                return f"✅ 模型已添加: {name}", self._get_models_df()
            else:
                return f"❌ 模型添加失败: {name}", self._get_models_df()
                
        except Exception as e:
            self.logger.error(f'添加模型失败: {e}')
            return f"❌ 添加失败: {str(e)}", self._get_models_df()
    
    def _test_all_models(self) -> Tuple[str, pd.DataFrame]:
        """测试所有模型（包含离线/未知状态）"""
        try:
            # 获取全部模型（名称 -> 配置）
            all_models = model_manager.get_all_models()
            names = list(all_models.keys())
            if not names:
                return "❌ 没有已注册的模型", self._get_models_df()

            results = []
            for model_name in names:
                try:
                    result = model_manager.test_model(model_name)
                    if result.get('success'):
                        rt = result.get('response_time', 0)
                        status = f"✅ 正常 ({rt:.2f}ms)"
                    else:
                        status = f"❌ 失败: {result.get('error_msg') or result.get('error', 'Unknown error')}"
                    results.append(f"模型 {model_name}: {status}")
                except Exception as e:
                    results.append(f"模型 {model_name}: ❌ 异常: {str(e)}")

            return f"🔧 测试完成:\n" + "\n".join(results), self._get_models_df()

        except Exception as e:
            self.logger.error(f'测试所有模型失败: {e}')
            return f"❌ 测试失败: {str(e)}", self._get_models_df()
    
    def _test_model(self, model_name: str) -> Tuple[str, pd.DataFrame]:
        """测试单个模型"""
        try:
            if not model_name.strip():
                return "❌ 请选择要测试的模型", self._get_models_df()
            
            result = model_manager.test_model(model_name.strip())
            
            if result.get('success'):
                response_time = result.get('response_time', 0)
                return f"✅ 模型 {model_name} 测试成功！\n响应时间: {response_time:.2f}s", self._get_models_df()
            else:
                error_msg = result.get('error', 'Unknown error')
                return f"❌ 模型 {model_name} 测试失败: {error_msg}", self._get_models_df()
                
        except Exception as e:
            self.logger.error(f'测试模型失败: {e}')
            return f"❌ 测试失败: {str(e)}", self._get_models_df()
    
    def _delete_model(self, model_name: str) -> Tuple[str, pd.DataFrame]:
        """删除模型"""
        try:
            if not model_name.strip():
                return "❌ 请选择要删除的模型", self._get_models_df()
            
            success = model_manager.delete_model(model_name.strip())
            
            if success:
                return f"✅ 模型已删除: {model_name}", self._get_models_df()
            else:
                return f"❌ 模型删除失败: {model_name}", self._get_models_df()
                
        except Exception as e:
            self.logger.error(f'删除模型失败: {e}')
            return f"❌ 删除失败: {str(e)}", self._get_models_df()
    
    def _refresh_models(self) -> pd.DataFrame:
        """刷新模型列表"""
        return self._get_models_df()
    
    def _select_model(self, evt: gr.SelectData) -> str:
        """选择模型"""
        if evt.index[1] == 0:  # 点击模型名称列
            return evt.value
        return ""
    
    def _get_models_df(self) -> pd.DataFrame:
        """获取模型列表数据框"""
        try:
            # 使用缓存的模型配置，避免逐个实时连测导致阻塞
            all_models = model_manager.get_all_models()  # { name: config }

            if not all_models:
                return pd.DataFrame(columns=["模型名称", "类型", "状态", "URL", "响应时间", "操作"])

            # 构建数据框（使用已记录的 status/response_time）
            rows = []
            for name, cfg in all_models.items():
                model_type = (cfg.get('type') or '').upper()
                url = cfg.get('url', '')
                status_val = cfg.get('status', 'unknown')
                rt_ms = cfg.get('response_time', 0)

                if status_val == 'online':
                    status = "✅ 正常"
                elif status_val == 'offline':
                    status = "❌ 离线"
                elif status_val == 'error':
                    status = "⚠️ 错误"
                else:
                    status = "❓ 未知"

                # 展示为毫秒，避免误解为秒
                response_time = f"{rt_ms:.2f}ms" if rt_ms else "-"

                display_url = url[:50] + "..." if len(url) > 50 else url

                rows.append([
                    name,
                    model_type,
                    status,
                    display_url,
                    response_time,
                    "点击选择"
                ])

            return pd.DataFrame(rows, columns=["模型名称", "类型", "状态", "URL", "响应时间", "操作"])
            
        except Exception as e:
            self.logger.error(f'获取模型列表失败: {e}')
            return pd.DataFrame(columns=["模型名称", "类型", "状态", "URL", "响应时间", "操作"])
    
    def _refresh_distill_models(self) -> gr.Dropdown:
        """刷新蒸馏模型列表"""
        try:
            models = model_manager.get_active_models()
            model_choices = [model.get('name', '') for model in models if model.get('name')]
            
            return gr.Dropdown(choices=model_choices, value=model_choices[0] if model_choices else None)
            
        except Exception as e:
            self.logger.error(f'刷新模型列表失败: {e}')
            return gr.Dropdown(choices=[])
    
    def _start_distill_generation(self,
                                 source_file,
                                 strategy: str,
                                 model_name: str,
                                 count: int,
                                 temperature: float,
                                 max_tokens: int,
                                 top_p: float,
                                 top_k: int,
                                 max_workers: int,
                                 fsync_interval: int,
                                 checkpoint_interval: int,
                                 inflight_multiplier: int,
                                 unordered_write: bool,
                                 rate_limit_rps: Optional[float],
                                 max_backoff: Optional[float],
                                 system_prompt: str,
                                 q_prompt: str,
                                 a_prompt: str,
                                 selected_fields: str,
                                 q_field_name: str,
                                 label_set: str,
                                 target_field: str) -> Tuple[str, pd.DataFrame]:
        """开始蒸馏生成（对齐新版策略与参数）"""
        try:
            if source_file is None:
                return "❌ 请选择源数据文件", self._get_distill_tasks_df()
            
            if not model_name:
                return "❌ 请选择生成模型", self._get_distill_tasks_df()
            
            source_path = source_file.name
            if not os.path.exists(source_path):
                return "❌ 源文件不存在", self._get_distill_tasks_df()
            
            # 校验格式
            ext = os.path.splitext(source_path)[1].lower()
            if ext not in [".jsonl", ".json"]:
                return "❌ 仅支持 .jsonl 或 .json 源数据文件", self._get_distill_tasks_df()
            
            # 构建生成参数
            params = {
                'strategy': strategy,
                'model_id': model_name,
                'input_file': source_path,
                'temperature': float(temperature),
                'max_tokens': int(max_tokens),
                'top_p': float(top_p),
                'max_workers': int(max_workers),
                'fsync_interval': int(fsync_interval),
                'checkpoint_interval': int(checkpoint_interval),
                'inflight_multiplier': int(inflight_multiplier)
            }

            # generation_count 仅对需要的策略生效
            strategies_need_count = {'expand', 'paraphrase', 'q_to_a'}
            if strategy in strategies_need_count:
                params['generation_count'] = int(count)

            # 采样参数
            if isinstance(top_k, (int, float)) and int(top_k) > 0:
                params['top_k'] = int(top_k)

            # 并发/鲁棒性
            if unordered_write:
                params['unordered_write'] = True
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
            
            return f"✅ 蒸馏生成任务已启动！\n任务ID: {task_id}\n策略: {strategy}\n模型: {model_name}", self._get_distill_tasks_df()
            
        except Exception as e:
            self.logger.error(f'启动蒸馏生成失败: {e}')
            return f"❌ 启动失败: {str(e)}", self._get_distill_tasks_df()

    def _on_strategy_change(self, strategy: str):
        """策略切换时，更新说明与控件可见性"""
        try:
            # 获取策略描述
            desc = distill_generator.get_strategy_description(strategy)
            if 'error' in desc:
                md = f"❌ 未知策略: {strategy}"
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

            return (
                md,
                gr.update(visible=need_count),
                gr.update(visible=show_q_a),
                gr.update(visible=show_q_a),
                gr.update(visible=show_label)
            )
        except Exception as e:
            self.logger.error(f'策略切换更新失败: {e}')
            # 失败时默认全部隐藏可选控件
            return (
                f"❌ 更新策略说明失败: {e}",
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False),
                gr.update(visible=False)
            )
    
    def _pause_distill_generation(self, task_id: str) -> Tuple[str, pd.DataFrame]:
        """暂停蒸馏生成"""
        try:
            if not task_id.strip():
                return "❌ 请选择要暂停的任务", self._get_distill_tasks_df()
            
            # 这里由于distill_generator没有pause方法，我们更新状态
            state_manager.update_state(task_id.strip(), 'status', 'paused')
            
            return f"✅ 任务已暂停: {task_id}", self._get_distill_tasks_df()
            
        except Exception as e:
            self.logger.error(f'暂停蒸馏生成失败: {e}')
            return f"❌ 暂停失败: {str(e)}", self._get_distill_tasks_df()
    
    def _refresh_distill_tasks(self) -> pd.DataFrame:
        """刷新蒸馏任务列表"""
        return self._get_distill_tasks_df()

    def _on_distill_model_change(self, model_name: str):
        """根据模型类型动态调整 max_tokens 上限与提示
        规则（保守）：
        - OPENAI: 上限 8192（避免 400 错）
        - VLLM/OLLAMA/SGLANG: 上限 200000（可调高，视模型与部署）
        - 其他/未知: 上限 4000（默认）
        """
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
    
    def _view_distill_report(self, task_id: str) -> str:
        """查看蒸馏报告"""
        try:
            if not task_id.strip():
                return "❌ 请选择要查看的任务"
            
            report = distill_generator.get_quality_report(task_id.strip())
            
            if 'error' in report:
                return f"❌ 获取报告失败: {report['error']}"
            
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
            return f"❌ 查看报告失败: {str(e)}"

    def _resume_distill_task(self, task_id: str, model_override: Optional[str], workers: int, temperature: float, max_tokens: int, top_p: float, top_k: int, rate_limit_rps: Optional[float], max_backoff: Optional[float], resume_as_new: bool) -> Tuple[str, pd.DataFrame]:
        """恢复蒸馏任务，支持覆盖部分参数（模型/并发/采样）。"""
        try:
            if not task_id or not task_id.strip():
                return "❌ 请选择要恢复的任务", self._get_distill_tasks_df()

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

            distill_generator.resume_generation(task_id.strip(), overrides or None)
            return f"✅ 已触发恢复: {task_id}", self._get_distill_tasks_df()
        except Exception as e:
            self.logger.error(f'恢复任务失败: {e}')
            return f"❌ 恢复失败: {str(e)}", self._get_distill_tasks_df()

    def _on_distill_source_change(self, source_file):
        """选择源数据文件后，扫描若干行推断字段列表，填充字段复选框并同步文本框"""
        try:
            if source_file is None:
                return gr.update(choices=[], value=[]), ""
            path = source_file.name
            if not os.path.exists(path):
                return gr.update(choices=[], value=[]), ""
            ext = os.path.splitext(path)[1].lower()
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
                return gr.update(choices=[], value=[]), ""

            choices = sorted(list(fields))
            # 默认选择推测到的常用字段之一
            defaults = []
            for cand in ['instruction', 'input', 'prompt', 'question', 'query']:
                if cand in fields:
                    defaults.append(cand)
                    break
            return gr.update(choices=choices, value=defaults), (", ".join(defaults) if defaults else "")
        except Exception:
            return gr.update(choices=[], value=[]), ""

    def _sync_selected_fields_text(self, selected_list: list) -> str:
        """将复选框选择同步到文本框（逗号分隔）"""
        try:
            if not selected_list:
                return ""
            return ",".join(selected_list)
        except Exception:
            return ""
    
    def _select_distill_task(self, evt: gr.SelectData) -> str:
        """选择蒸馏任务"""
        if evt.index[1] == 0:  # 点击任务ID列
            return evt.value
        return ""
    
    def _get_distill_tasks_df(self) -> pd.DataFrame:
        """获取蒸馏任务列表数据框"""
        try:
            # 获取所有蒸馏任务（按任务类型 DISTILL 过滤）
            tasks = state_manager.list_tasks(task_type=TaskType.DISTILL)
            
            if not tasks:
                return pd.DataFrame(columns=["任务ID", "策略", "模型", "状态", "进度", "开始时间"])
            
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
                    task_id,
                    strategy_cn,
                    model_id,
                    status_cn,
                    progress_str,
                    start_time_str
                ])
            
            return pd.DataFrame(rows, columns=["任务ID", "策略", "模型", "状态", "进度", "开始时间"])
            
        except Exception as e:
            self.logger.error(f'获取蒸馏任务列表失败: {e}')
            return pd.DataFrame(columns=["任务ID", "策略", "模型", "状态", "进度", "开始时间"])
    
    def _filter_datasets(self, data_type: str, search_name: str = "") -> pd.DataFrame:
        """根据数据类型和名称筛选数据集"""
        return self._get_datasets_df(data_type, search_name)
    
    def _refresh_datasets(self, data_type: str, search_name: str = "") -> pd.DataFrame:
        """刷新数据集列表（优化版本）"""
        try:
            self.logger.info(f"开始刷新数据集列表: 类型={data_type}, 搜索={search_name}")
            
            # 使用线程池来避免长时间阻塞
            import concurrent.futures
            import threading
            
            def get_datasets_with_timeout():
                return self._get_datasets_df(data_type, search_name)
            
            try:
                # 使用线程池执行，设置合理的超时时间
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(get_datasets_with_timeout)
                    
                    try:
                        # 设置30秒超时
                        result = future.result(timeout=30)
                        self.logger.info(f"数据集列表刷新完成: {len(result)} 个数据集")
                        return result
                    except concurrent.futures.TimeoutError:
                        self.logger.error("数据集列表刷新超时（30秒）")
                        return pd.DataFrame(columns=["名称", "类型", "大小", "创建时间", "路径"])
                        
            except Exception as e:
                self.logger.error(f"执行数据集刷新时出错: {e}")
                return pd.DataFrame(columns=["名称", "类型", "大小", "创建时间", "路径"])
            
        except Exception as e:
            self.logger.error(f'刷新数据集列表失败: {e}')
            # 返回空的DataFrame，但包含正确的列名
            return pd.DataFrame(columns=["名称", "类型", "大小", "创建时间", "路径"])
    
    def _preview_dataset(self, dataset_path: str, rows: int, 
                        enable_truncation: bool = True, max_text_length: int = 300,
                        show_metadata: bool = True, show_stats: bool = True,
                        smart_columns: bool = True, show_all_columns: bool = False,
                        column_info_display: bool = False) -> Tuple[pd.DataFrame, str]:
        """预览数据集 - 使用新的增强预览器"""
        try:
            if not dataset_path.strip():
                return pd.DataFrame(), "❌ 请选择要预览的数据集"
            
            if not os.path.exists(dataset_path):
                return pd.DataFrame(), "❌ 数据集文件不存在"
            
            # 更新预览器配置
            self.dataset_previewer.config.max_rows = rows
            self.dataset_previewer.config.enable_truncation = enable_truncation
            self.dataset_previewer.config.max_text_length = max_text_length
            self.dataset_previewer.config.show_stats = show_stats
            self.dataset_previewer.config.include_metadata = show_metadata
            self.dataset_previewer.config.smart_columns = smart_columns
            self.dataset_previewer.config.show_all_columns = show_all_columns
            
            # 使用增强预览器预览数据
            preview_result = self.dataset_previewer.preview_dataset(dataset_path, rows)
            
            if not preview_result.success:
                return pd.DataFrame(), f"❌ 预览失败: {preview_result.error_message}"
            
            if not preview_result.data:
                return pd.DataFrame(), "❌ 数据集为空"
            
            # 转换为DataFrame
            df = pd.DataFrame(preview_result.data)
            
            # 构建详细状态信息
            status_parts = [f"✅ 预览成功！"]
            
            # 基础信息
            status_parts.append(f"📁 路径: {dataset_path}")
            status_parts.append(f"📊 格式: {preview_result.format.value.upper()}")
            status_parts.append(f"📈 总行数: {preview_result.total_rows:,}")
            status_parts.append(f"👀 预览行数: {len(preview_result.data)}")
            status_parts.append(f"📂 文件数: {preview_result.total_files}")
            
            # 文件信息
            if preview_result.files:
                file_info = preview_result.files[0]  # 主文件
                status_parts.append(f"💾 文件大小: {self._format_size(file_info.size)}")
                if file_info.columns:
                    status_parts.append(f"📋 总列数: {len(file_info.columns)}")
                    # 显示当前显示的列数
                    if preview_result.data:
                        displayed_cols = len(preview_result.data[0].keys())
                        status_parts.append(f"👁️ 显示列数: {displayed_cols}")
                    
                    column_names = list(preview_result.data[0].keys())[:5] if preview_result.data else []
                    if len(column_names) > 5:
                        column_names.append("...")
                    status_parts.append(f"🔤 当前列名: {', '.join(column_names)}")
            
            # 隐藏列信息
            if preview_result.hidden_columns:
                status_parts.append(f"🙈 隐藏列数: {len(preview_result.hidden_columns)}")
                hidden_names = preview_result.hidden_columns[:3]
                if len(preview_result.hidden_columns) > 3:
                    hidden_names.append("...")
                status_parts.append(f"🔒 隐藏列名: {', '.join(hidden_names)}")
            
            # 截断信息
            if preview_result.truncated_fields:
                status_parts.append(f"✂️ 截断字段: {', '.join(preview_result.truncated_fields)}")
            
            # 列统计信息
            if column_info_display and preview_result.column_info:
                status_parts.append("\n📊 列统计信息:")
                for col, stats in list(preview_result.column_info.items())[:5]:  # 只显示前5列
                    null_rate = stats.get('null_rate', 0) * 100
                    unique_count = stats.get('unique_count', 0)
                    avg_length = stats.get('avg_length', 0)
                    status_parts.append(f"  • {col}: 空值{null_rate:.1f}%, 唯一值{unique_count}, 平均长度{avg_length:.1f}")
            
            # 元数据信息
            if show_metadata and preview_result.metadata:
                metadata = preview_result.metadata
                if 'column_types' in metadata:
                    type_summary = {}
                    for col, types in metadata['column_types'].items():
                        for t in types:
                            type_summary[t] = type_summary.get(t, 0) + 1
                    
                    type_str = ", ".join([f"{t}: {count}" for t, count in type_summary.items()])
                    status_parts.append(f"🔢 数据类型: {type_str}")
            
            status_msg = "\n".join(status_parts)
            
            return df, status_msg
            
        except Exception as e:
            self.logger.error(f'预览数据集失败: {e}')
            return pd.DataFrame(), f"❌ 预览失败: {str(e)}"
    
    def _format_size(self, size_bytes: int) -> str:
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}TB"
    
    def _preview_dataset_enhanced(self, dataset_path: str, rows: int, 
                                 enable_truncation: bool, max_text_length: int,
                                 show_metadata: bool, show_stats: bool,
                                 smart_columns: bool, show_all_columns: bool,
                                 column_info_display: bool) -> Tuple[str, pd.DataFrame, str]:
        """增强预览数据集（通过预览按钮触发）"""
        try:
            if not dataset_path.strip():
                return "*请先选择要预览的数据集*", pd.DataFrame(), "❌ 请选择要预览的数据集"
            
            # 调用增强预览功能
            preview_df, status_msg = self._preview_dataset(
                dataset_path, rows, enable_truncation, max_text_length, 
                show_metadata, show_stats, smart_columns, show_all_columns, column_info_display
            )
            
            # 生成数据集信息
            dataset_info = self._generate_dataset_info(dataset_path, preview_df, status_msg)
            
            return dataset_info, preview_df, status_msg
            
        except Exception as e:
            self.logger.error(f'增强预览数据集失败: {e}')
            error_msg = f"❌ 预览失败: {str(e)}"
            return "*预览失败*", pd.DataFrame(), error_msg
    
    def _generate_dataset_info(self, dataset_path: str, preview_df: pd.DataFrame, status_msg: str) -> str:
        """生成数据集信息Markdown"""
        try:
            if preview_df.empty:
                return "*暂无数据集信息*"
            
            # 获取数据集摘要
            summary = self.dataset_previewer.get_dataset_summary(dataset_path)
            
            if 'error' in summary:
                return f"*数据集信息获取失败: {summary['error']}*"
            
            info_parts = [
                f"### 📊 数据集详情",
                f"**名称**: {summary.get('name', 'Unknown')}",
                f"**类型**: {summary.get('type', 'Unknown')}",
                f"**格式**: {summary.get('format', 'Unknown').upper()}"
            ]
            
            if summary.get('size_human'):
                info_parts.append(f"**大小**: {summary['size_human']}")
            
            if summary.get('estimated_rows'):
                info_parts.append(f"**预估行数**: {summary['estimated_rows']:,}")
            
            if summary.get('file_count'):
                info_parts.append(f"**文件数量**: {summary['file_count']}")
            
            if summary.get('formats'):
                info_parts.append(f"**包含格式**: {', '.join(summary['formats'])}")
            
            # 预览数据信息
            if not preview_df.empty:
                info_parts.extend([
                    "",
                    f"### 📈 预览统计",
                    f"**预览行数**: {len(preview_df)}",
                    f"**列数**: {len(preview_df.columns)}",
                    f"**列名**: {', '.join(preview_df.columns.tolist()[:5])}{'...' if len(preview_df.columns) > 5 else ''}"
                ])
            
            return "\n".join(info_parts)
            
        except Exception as e:
            self.logger.error(f'生成数据集信息失败: {e}')
            return f"*数据集信息生成失败: {str(e)}*"
    
    def _delete_dataset(self, dataset_path: str, data_type: str) -> Tuple[str, pd.DataFrame, str, str, pd.DataFrame]:
        """删除数据集"""
        try:
            print(f"[DEBUG] 删除请求: path={dataset_path}, type={data_type}")
            
            if not dataset_path.strip():
                return "❌ 请选择要删除的数据集", pd.DataFrame(), "", "", pd.DataFrame()
            
            # 检查路径类型
            from pathlib import Path
            path_obj = Path(dataset_path)
            if path_obj.is_file():
                path_type = "文件"
            elif path_obj.is_dir():
                path_type = "数据集目录"
            else:
                path_type = "路径"
            
            # 调用数据管理模块删除数据
            print(f"[DEBUG] 准备删除{path_type}: {dataset_path}")
            success = data_manager.delete_data(dataset_path)
            print(f"[DEBUG] 删除结果: {success}")
            
            if success:
                # 删除成功后刷新当前选中的数据类型列表
                print(f"[DEBUG] 刷新数据类型: {data_type}")
                refreshed_df = self._get_datasets_df(data_type)
                print(f"[DEBUG] 刷新后数据集数量: {len(refreshed_df)}")
                
                # 清空选中状态和显示内容
                return (f"✅ 删除{path_type}完成！\n{path_type}: {dataset_path}", 
                       refreshed_df, 
                       "",  # 清空selected_dataset
                       "",  # 清空dataset_info
                       pd.DataFrame())  # 清空result_display用空DataFrame
            else:
                return f"❌ 删除{path_type}失败：可能文件不存在或权限不足", pd.DataFrame(), dataset_path, "", pd.DataFrame()
                
        except Exception as e:
            print(f"[DEBUG] 删除异常: {e}")
            self.logger.error(f'删除数据集失败: {e}')
            return f"❌ 删除失败: {str(e)}", pd.DataFrame(), dataset_path, "", pd.DataFrame()
    
    def _get_storage_stats(self) -> str:
        """获取存储统计"""
        try:
            # 调用数据管理模块获取统计信息
            stats = data_manager.get_storage_statistics()
            
            # 格式化统计信息
            stats_text = f"""📊 存储统计信息

📁 数据类型统计:
  原始数据: {stats.get('raw_count', 0)} 个文件
  处理数据: {stats.get('processed_count', 0)} 个文件  
  蒸馏数据: {stats.get('distilled_count', 0)} 个文件
  备份数据: {stats.get('backup_count', 0)} 个文件

💾 存储空间:
  总大小: {stats.get('total_size_mb', 0):.1f} MB
  原始数据: {stats.get('raw_size_mb', 0):.1f} MB
  处理数据: {stats.get('processed_size_mb', 0):.1f} MB
  蒸馏数据: {stats.get('distilled_size_mb', 0):.1f} MB

📈 最新活动:
  最新文件: {stats.get('latest_file', 'N/A')}
  创建时间: {stats.get('latest_time', 'N/A')}
"""
            
            return stats_text
            
        except Exception as e:
            self.logger.error(f'获取存储统计失败: {e}')
            return f"❌ 获取统计失败: {str(e)}"
    
    def _select_dataset(self, evt: gr.SelectData) -> str:
        """选择数据集 - 支持点击任意列"""
        try:
            # 获取当前行的所有数据
            row_data = evt.row_value
            if row_data and len(row_data) >= 5:
                # 返回路径列（第5列，索引4）的值
                return row_data[4]
            return ""
        except Exception as e:
            self.logger.error(f'选择数据集失败: {e}')
            return ""
    
    def _select_dataset_with_enhanced_preview(self, evt: gr.SelectData, auto_preview: bool, preview_rows: int,
                                            enable_truncation: bool, max_text_length: int,
                                            show_metadata: bool, show_stats: bool,
                                            smart_columns: bool, show_all_columns: bool,
                                            column_info_display: bool) -> Tuple[str, str, pd.DataFrame, str]:
        """选择数据集并使用增强预览"""
        try:
            # 获取当前行的所有数据
            row_data = evt.row_value
            if not row_data or len(row_data) < 5:
                return "", "*请选择有效的数据集*", pd.DataFrame(), ""
            
            # 提取数据集信息
            dataset_name = row_data[0]
            dataset_type = row_data[1]
            dataset_size = row_data[2]
            dataset_time = row_data[3]
            dataset_path = row_data[4]
            
            # 如果启用自动预览，则执行增强预览
            if auto_preview and dataset_path:
                preview_df, status_msg = self._preview_dataset(
                    dataset_path, preview_rows, enable_truncation, max_text_length, 
                    show_metadata, show_stats, smart_columns, show_all_columns, column_info_display
                )
                dataset_info = self._generate_dataset_info(dataset_path, preview_df, status_msg)
                return dataset_path, dataset_info, preview_df, status_msg
            else:
                # 构建基础数据集信息显示
                info_text = f"""**📊 数据集信息**
- **名称**: {dataset_name}
- **类型**: {dataset_type}
- **大小**: {dataset_size}
- **创建时间**: {dataset_time}
- **路径**: `{os.path.basename(dataset_path)}`"""
                
                return dataset_path, info_text, pd.DataFrame(), "✅ 数据集已选择，点击预览按钮查看数据内容"
                
        except Exception as e:
            self.logger.error(f'选择数据集失败: {e}')
            return "", f"❌ 选择失败: {str(e)}", pd.DataFrame(), ""
    
    def _get_datasets_df(self, data_type: str = "全部", search_name: str = "") -> pd.DataFrame:
        """获取数据集列表数据框"""
        try:
            # 数据类型映射
            type_map = {
                "全部": None,
                "原始数据": "raw",
                "处理数据": "processed", 
                "蒸馏数据": "distilled"
            }
            
            filter_type = type_map.get(data_type)
            
            # 调用数据管理模块获取数据集列表
            datasets = data_manager.list_datasets(data_type=filter_type)
            
            if not datasets:
                return pd.DataFrame(columns=["名称", "类型", "大小", "创建时间", "路径"])
            
            # 过滤和优化数据集列表
            filtered_datasets = self._filter_datasets_for_display(datasets)
            
            # 根据名称搜索过滤
            if search_name and search_name.strip():
                search_keyword = search_name.strip().lower()
                filtered_datasets = [
                    ds for ds in filtered_datasets 
                    if search_keyword in ds.get('display_name', '').lower() or
                       search_keyword in ds.get('path', '').lower()
                ]
            
            # 构建数据框
            rows = []
            for dataset in filtered_datasets:
                file_path = dataset.get('path', '')
                
                # 使用新的分层显示逻辑
                if 'display_name' in dataset:
                    # 使用分层显示名称
                    display_name = dataset['display_name']
                    # 分层条目使用 provider 作为类型显示（例如：蒸馏数据/处理数据/提供商名）
                    data_type_cn = dataset.get('provider') or dataset.get('type', '未知')
                    
                    # 格式化大小显示
                    size_mb = dataset.get('size_mb', 0)
                    if size_mb > 1024:
                        size_display = f"{size_mb/1024:.1f} GB"
                    elif size_mb > 0:
                        size_display = f"{size_mb:.1f} MB"
                    else:
                        size_display = "计算中..."
                    
                    # 文件数量信息
                    file_count = dataset.get('file_count', 0)
                    if file_count > 1:
                        size_display += f" ({file_count}个文件)"
                    
                else:
                    # 兼容旧格式 - 检查是否为汇总条目
                    if dataset.get('is_summary', False):
                        # 数据集汇总条目的特殊处理
                        dataset_name = dataset.get('dataset_name', '')
                        file_count = dataset.get('file_count', 0)
                        organized_count = dataset.get('organized_count', 0)
                        cache_count = dataset.get('cache_count', 0)
                        
                        # 显示名称包含统计信息
                        display_name = f"📁 {dataset_name} ({file_count}个文件)"
                        if organized_count > 0:
                            display_name += f" ✨{organized_count}整理"
                        if cache_count > 0:
                            display_name += f" 📦{cache_count}缓存"
                        
                        data_type_cn = "数据集汇总"
                        
                    else:
                        # 普通文件条目
                        file_name = os.path.basename(file_path)
                        display_name = file_name
                        
                        # 确定数据类型
                        path_norm = (file_path or '').replace('\\', '/')
                        if '/raw/' in path_norm:
                            if '/organized_files/' in path_norm:
                                data_type_cn = "原始数据(整理)"
                            elif '/cache/' in path_norm:
                                data_type_cn = "原始数据(缓存)"
                            else:
                                data_type_cn = "原始数据"
                        elif '/distilled/' in path_norm:
                            data_type_cn = "蒸馏数据"
                        elif '/processed/' in path_norm:
                            data_type_cn = "处理数据"
                        else:
                            data_type_cn = "其他"
                    
                    # 文件大小 - 改进显示格式
                    size_mb = dataset.get('size_mb', 0)
                    if size_mb > 1024:
                        size_display = f"{size_mb/1024:.1f} GB"
                    elif size_mb > 1:
                        size_display = f"{size_mb:.1f} MB"
                    elif size_mb > 0:
                        size_display = f"{size_mb*1024:.0f} KB"
                    else:
                        size_display = "计算中..."
                
                # 创建时间
                create_time = dataset.get('create_time', '')
                if create_time:
                    try:
                        dt = datetime.fromisoformat(create_time.replace('Z', '+00:00'))
                        time_str = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        time_str = create_time[:16] if len(create_time) > 16 else create_time
                else:
                    time_str = ""
                
                rows.append([
                    display_name,
                    data_type_cn,
                    size_display,
                    time_str,
                    file_path
                ])
            
            return pd.DataFrame(rows, columns=["名称", "类型", "大小", "创建时间", "路径"])
            
        except Exception as e:
            self.logger.error(f'获取数据集列表失败: {e}')
            return pd.DataFrame(columns=["名称", "类型", "大小", "创建时间", "路径"])
    
    def _select_dataset_with_auto_preview(self, evt: gr.SelectData, auto_preview: bool, 
                                        preview_rows: int, enable_truncation: bool, 
                                        max_text_length: int) -> Tuple[str, str, pd.DataFrame, gr.CheckboxGroup, str]:
        """选择数据集并自动预览（新版本，包含字段选择）"""
        try:
            # 获取当前行的所有数据
            row_data = evt.row_value
            if not row_data or len(row_data) < 5:
                return "", "*请选择有效的数据集*", pd.DataFrame(), gr.CheckboxGroup(choices=[], value=[]), ""
            
            # 提取数据集信息
            dataset_name = row_data[0]
            dataset_type = row_data[1]
            dataset_size = row_data[2]
            dataset_time = row_data[3]
            dataset_path = row_data[4]
            
            # 如果启用自动预览，则执行预览并加载字段
            if auto_preview and dataset_path:
                return self._load_dataset_with_fields(dataset_path, preview_rows, enable_truncation, max_text_length)
            else:
                # 构建基础数据集信息显示
                info_text = f"""**📊 数据集信息**
- **名称**: {dataset_name}
- **类型**: {dataset_type}
- **大小**: {dataset_size}
- **创建时间**: {dataset_time}
- **路径**: `{os.path.basename(dataset_path)}`"""
                
                return dataset_path, info_text, pd.DataFrame(), gr.CheckboxGroup(choices=[], value=[]), "✅ 数据集已选择，等待预览..."
                
        except Exception as e:
            self.logger.error(f'选择数据集失败: {e}')
            return "", f"❌ 选择失败: {str(e)}", pd.DataFrame(), gr.CheckboxGroup(choices=[], value=[]), ""
    
    def _load_dataset_with_fields(self, dataset_path: str, preview_rows: int, 
                                enable_truncation: bool, max_text_length: int) -> Tuple[str, str, pd.DataFrame, gr.CheckboxGroup, str]:
        """加载数据集并分析字段（核心功能）"""
        try:
            if not dataset_path.strip():
                return "", "*请选择要预览的数据集*", pd.DataFrame(), gr.CheckboxGroup(choices=[], value=[]), "❌ 请选择要预览的数据集"
            
            if not os.path.exists(dataset_path):
                return "", "*数据集文件不存在*", pd.DataFrame(), gr.CheckboxGroup(choices=[], value=[]), "❌ 数据集文件不存在"
            
            # 检查是否是新的数据集，如果是则清空缓存
            if self.current_dataset_cache['path'] != dataset_path:
                self.current_dataset_cache = {
                    'path': dataset_path,
                    'data': None,
                    'fields': [],
                    'original_preview': None
                }
            
            # 如果缓存中没有数据，则读取数据集
            if self.current_dataset_cache['data'] is None:
                # 更新预览器配置
                self.dataset_previewer.config.max_rows = preview_rows
                self.dataset_previewer.config.enable_truncation = False  # 先不截断，保存完整数据
                self.dataset_previewer.config.max_text_length = max_text_length
                self.dataset_previewer.config.smart_columns = False  # 不智能隐藏列，显示所有字段
                self.dataset_previewer.config.show_all_columns = True
                
                # 使用增强预览器预览数据
                preview_result = self.dataset_previewer.preview_dataset(dataset_path, preview_rows)
                
                if not preview_result.success:
                    return "", "*预览失败*", pd.DataFrame(), gr.CheckboxGroup(choices=[], value=[]), f"❌ 预览失败: {preview_result.error_message}"
                
                if not preview_result.data:
                    return "", "*数据集为空*", pd.DataFrame(), gr.CheckboxGroup(choices=[], value=[]), "❌ 数据集为空"
                
                # 缓存数据
                self.current_dataset_cache['data'] = preview_result.data
                self.current_dataset_cache['original_preview'] = preview_result
                
                # 使用universal_field_extractor分析字段（修复HuggingFace数据集问题）
                try:
                    field_names = self._get_simple_field_names(dataset_path)
                    if field_names:
                        self.current_dataset_cache['fields'] = field_names
                    else:
                        # 如果universal_field_extractor失败，回退到从数据中提取字段
                        if preview_result.data:
                            available_fields = list(preview_result.data[0].keys())
                            self.current_dataset_cache['fields'] = available_fields
                        else:
                            self.current_dataset_cache['fields'] = []
                except Exception as field_error:
                    self.logger.warning(f'字段提取失败，使用预览数据字段: {field_error}')
                    # 回退到从数据中提取字段
                    if preview_result.data:
                        available_fields = list(preview_result.data[0].keys())
                        self.current_dataset_cache['fields'] = available_fields
                    else:
                        self.current_dataset_cache['fields'] = []
            
            # 获取可用字段
            available_fields = self.current_dataset_cache['fields']
            
            # 识别常用字段并设为默认选中
            common_fields = self._identify_common_fields(available_fields)
            
            # 创建字段选择器
            field_choices = [(field, field) for field in available_fields]
            field_selector = gr.CheckboxGroup(
                label="可用字段",
                choices=field_choices,
                value=common_fields,  # 默认选中常用字段
                interactive=True,
                info=f"数据集包含 {len(available_fields)} 个字段，已默认选中常用字段"
            )
            
            # 使用默认选中的字段生成预览
            preview_df, status_msg = self._filter_preview_by_fields(common_fields, enable_truncation, max_text_length)
            
            # 生成数据集信息
            dataset_info = self._generate_dataset_info_from_cache(dataset_path, preview_df)
            
            return dataset_path, dataset_info, preview_df, field_selector, status_msg
            
        except Exception as e:
            self.logger.error(f'加载数据集和字段失败: {e}')
            error_msg = f"❌ 加载失败: {str(e)}"
            return "", "*加载失败*", pd.DataFrame(), gr.CheckboxGroup(choices=[], value=[]), error_msg
    
    def _identify_common_fields(self, available_fields: List[str]) -> List[str]:
        """识别常用字段"""
        # 常用字段优先级列表（按重要性排序）
        common_patterns = [
            'instruction', 'input', 'output', 'response', 'answer', 'question', 
            'text', 'content', 'prompt', 'completion', 'query', 'reply',
            'conversations', 'messages', 'dialogue', 'chat',
            'title', 'description', 'summary', 'context'
        ]
        
        selected_fields = []
        available_lower = [field.lower() for field in available_fields]
        
        # 按优先级选择字段
        for pattern in common_patterns:
            for i, field_lower in enumerate(available_lower):
                if pattern in field_lower and available_fields[i] not in selected_fields:
                    selected_fields.append(available_fields[i])
                    break  # 每种模式只选择一个字段
        
        # 如果没有匹配到常用字段，选择前几个字段
        if not selected_fields:
            selected_fields = available_fields[:min(3, len(available_fields))]
        
        # 限制选中字段数量（避免显示过多列）
        max_fields = 5
        if len(selected_fields) > max_fields:
            selected_fields = selected_fields[:max_fields]
        
        return selected_fields
    
    def _filter_preview_by_fields(self, selected_fields: List[str], 
                                enable_truncation: bool, max_text_length: int) -> Tuple[pd.DataFrame, str]:
        """根据选中字段过滤预览数据"""
        try:
            if not self.current_dataset_cache['data']:
                return pd.DataFrame(), "❌ 没有缓存的数据"
            
            if not selected_fields:
                return pd.DataFrame(), "❌ 请选择至少一个字段"
            
            # 从缓存的完整数据中过滤字段
            filtered_data = []
            for row in self.current_dataset_cache['data']:
                filtered_row = {}
                for field in selected_fields:
                    # 检查是否是嵌套字段路径
                    if '.' in field or '[' in field:
                        # 使用universal_field_extractor的方法提取嵌套值
                        try:
                            from universal_field_extractor import _extractor
                            value = _extractor._get_nested_value(row, field)
                        except:
                            value = None
                    else:
                        # 简单字段直接获取
                        value = row.get(field)
                    
                    # 处理None值
                    if value is None:
                        filtered_row[field] = ""
                    else:
                        # 应用文本截断
                        if enable_truncation and isinstance(value, str) and len(value) > max_text_length:
                            value = value[:max_text_length] + "..."
                        # 确保值可以被pandas处理
                        if isinstance(value, (list, dict)):
                            filtered_row[field] = str(value)
                        else:
                            filtered_row[field] = value
                    
                filtered_data.append(filtered_row)
            
            # 转换为DataFrame
            df = pd.DataFrame(filtered_data)
            
            # 构建状态信息
            preview_result = self.current_dataset_cache['original_preview']
            status_parts = [
                f"✅ 字段过滤预览成功！",
                f"📁 数据集: {os.path.basename(self.current_dataset_cache['path'])}",
                f"📊 总字段数: {len(self.current_dataset_cache['fields'])}",
                f"👁️ 显示字段数: {len(selected_fields)}",
                f"📈 总行数: {preview_result.total_rows:,}",
                f"👀 预览行数: {len(filtered_data)}",
                f"📋 选中字段: {', '.join(selected_fields)}"
            ]
            
            if enable_truncation:
                status_parts.append(f"✂️ 文本截断: {max_text_length} 字符")
            
            status_msg = "\n".join(status_parts)
            
            return df, status_msg
            
        except Exception as e:
            self.logger.error(f'过滤预览数据失败: {e}')
            return pd.DataFrame(), f"❌ 过滤失败: {str(e)}"
    
    def _generate_dataset_info_from_cache(self, dataset_path: str, preview_df: pd.DataFrame) -> str:
        """从缓存信息生成数据集信息"""
        try:
            if not self.current_dataset_cache['original_preview']:
                return "*暂无数据集信息*"
            
            preview_result = self.current_dataset_cache['original_preview']
            
            info_parts = [
                f"### 📊 数据集详情",
                f"**名称**: {os.path.basename(dataset_path)}",
                f"**格式**: {preview_result.format.value.upper()}",
                f"**总行数**: {preview_result.total_rows:,}",
                f"**总字段数**: {len(self.current_dataset_cache['fields'])}",
                f"**文件数**: {preview_result.total_files}"
            ]
            
            # 文件信息
            if preview_result.files:
                file_info = preview_result.files[0]
                info_parts.append(f"**文件大小**: {self._format_size(file_info.size)}")
            
            # 预览信息
            if not preview_df.empty:
                info_parts.extend([
                    "",
                    f"### 📈 当前预览",
                    f"**预览行数**: {len(preview_df)}",
                    f"**显示字段**: {len(preview_df.columns)}",
                    f"**字段列表**: {', '.join(preview_df.columns.tolist())}"
                ])
            
            # 所有可用字段
            all_fields = self.current_dataset_cache['fields']
            if len(all_fields) > len(preview_df.columns):
                hidden_fields = [f for f in all_fields if f not in preview_df.columns]
                info_parts.append(f"**隐藏字段**: {', '.join(hidden_fields[:5])}{'...' if len(hidden_fields) > 5 else ''}")
            
            return "\n".join(info_parts)
            
        except Exception as e:
            self.logger.error(f'生成数据集信息失败: {e}')
            return f"*数据集信息生成失败: {str(e)}*"
    
    def _update_preview_by_fields(self, selected_fields: List[str], preview_rows: int, 
                                enable_truncation: bool, max_text_length: int) -> Tuple[pd.DataFrame, str]:
        """根据字段选择更新预览（实时响应）"""
        try:
            if not selected_fields:
                return pd.DataFrame(), "💡 请选择要显示的字段"
            
            # 使用缓存的数据进行字段过滤
            return self._filter_preview_by_fields(selected_fields, enable_truncation, max_text_length)
            
        except Exception as e:
            self.logger.error(f'更新字段预览失败: {e}')
            return pd.DataFrame(), f"❌ 更新失败: {str(e)}"
    
    def _preview_dataset_with_field_filter(self, dataset_path: str, preview_rows: int,
                                         enable_truncation: bool, max_text_length: int,
                                         selected_fields: List[str]) -> Tuple[pd.DataFrame, str]:
        """使用字段过滤器预览数据集（手动预览按钮）"""
        try:
            if not dataset_path.strip():
                return pd.DataFrame(), "❌ 请选择要预览的数据集"
            
            # 如果不是当前缓存的数据集，重新加载
            if self.current_dataset_cache['path'] != dataset_path:
                _, _, preview_df, _, status_msg = self._load_dataset_with_fields(
                    dataset_path, preview_rows, enable_truncation, max_text_length
                )
                return preview_df, status_msg
            
            # 使用选中的字段过滤预览
            if selected_fields:
                return self._filter_preview_by_fields(selected_fields, enable_truncation, max_text_length)
            else:
                return pd.DataFrame(), "💡 请选择要显示的字段"
            
        except Exception as e:
            self.logger.error(f'字段过滤预览失败: {e}')
            return pd.DataFrame(), f"❌ 预览失败: {str(e)}"
    
    def _select_all_dataset_fields(self) -> gr.CheckboxGroup:
        """全选所有字段"""
        try:
            all_fields = self.current_dataset_cache['fields']
            field_choices = [(field, field) for field in all_fields]
            return gr.CheckboxGroup(
                label="可用字段",
                choices=field_choices,
                value=all_fields,
                interactive=True,
                info=f"已选中所有 {len(all_fields)} 个字段"
            )
        except Exception as e:
            self.logger.error(f'全选字段失败: {e}')
            return gr.CheckboxGroup(choices=[], value=[])
    
    def _clear_all_dataset_fields(self) -> gr.CheckboxGroup:
        """清除所有字段选择"""
        try:
            all_fields = self.current_dataset_cache['fields']
            field_choices = [(field, field) for field in all_fields]
            return gr.CheckboxGroup(
                label="可用字段",
                choices=field_choices,
                value=[],
                interactive=True,
                info="已清除所有字段选择"
            )
        except Exception as e:
            self.logger.error(f'清除字段选择失败: {e}')
            return gr.CheckboxGroup(choices=[], value=[])
    
    def _select_common_dataset_fields(self) -> gr.CheckboxGroup:
        """选择常用字段"""
        try:
            all_fields = self.current_dataset_cache['fields']
            common_fields = self._identify_common_fields(all_fields)
            field_choices = [(field, field) for field in all_fields]
            return gr.CheckboxGroup(
                label="可用字段",
                choices=field_choices,
                value=common_fields,
                interactive=True,
                info=f"已选中 {len(common_fields)} 个常用字段: {', '.join(common_fields)}"
            )
        except Exception as e:
            self.logger.error(f'选择常用字段失败: {e}')
            return gr.CheckboxGroup(choices=[], value=[])
    
    def _filter_datasets_for_display(self, datasets: list) -> list:
        """
        过滤数据集列表，按提供商/数据集分层显示
        
        显示策略：
        1. 按数据源提供商分组（huggingface, modelscope等）
        2. 每个提供商下面显示具体的数据集
        3. 计算每个数据集的实际磁盘大小
        4. 路径指向数据集目录而不是meta.json文件
        """
        try:
            # 按提供商和数据集分组
            provider_groups = {}
            
            for dataset in datasets:
                file_path = dataset.get('path', '')
                path_norm = (file_path or '').replace('\\', '/')
                
                # 提取提供商和数据集名称
                provider = "其他"
                dataset_name = "未知数据集"
                dataset_dir = ""
                
                if '/raw/' in path_norm:
                    # 解析路径：data/raw/provider/dataset_name/...
                    parts = path_norm.split('/raw/')
                    if len(parts) > 1:
                        path_parts = parts[1].split('/')
                        if len(path_parts) >= 2:
                            provider = path_parts[0]  # huggingface, modelscope等
                            dataset_name = path_parts[1]  # 具体数据集名称
                            # 构建数据集目录路径
                            dataset_dir = f"{parts[0]}/raw/{provider}/{dataset_name}"
                        elif len(path_parts) == 1:
                            provider = path_parts[0]
                            dataset_name = "根目录文件"
                elif '/distilled/' in path_norm:
                    # 蒸馏数据：记录蒸馏信息
                    provider = "蒸馏数据"  
                    distilled_info = self._extract_distilled_info(file_path)
                    dataset_name = distilled_info['name']
                    # 构建蒸馏数据目录路径
                    from pathlib import Path
                    dataset_dir = str(Path(file_path).parent)
                elif '/processed/' in path_norm:
                    # 处理数据：记录处理动作和路径
                    provider = "处理数据"
                    # 尝试从路径中提取处理信息
                    processed_info = self._extract_processed_info(file_path)
                    dataset_name = processed_info['name']
                    # 构建处理数据目录路径
                    from pathlib import Path
                    dataset_dir = str(Path(file_path).parent)
                
                # 创建分组结构
                if provider not in provider_groups:
                    provider_groups[provider] = {}
                
                if dataset_name not in provider_groups[provider]:
                    provider_groups[provider][dataset_name] = {
                        'files': [],
                        'dataset_dir': dataset_dir,
                        'total_size': 0,
                        'latest_time': '',
                        'file_count': 0
                    }
                
                group = provider_groups[provider][dataset_name]
                group['files'].append(dataset)
                group['file_count'] += 1
                
                # 累计大小（如果有的话）
                if 'size_mb' in dataset:
                    group['total_size'] += dataset.get('size_mb', 0)
                
                # 更新最新时间
                file_time = dataset.get('create_time', '')
                if file_time > group['latest_time']:
                    group['latest_time'] = file_time
                
                # 如果没有设置数据集目录，使用文件路径的父目录
                if not group['dataset_dir'] and file_path:
                    from pathlib import Path
                    parent_path = str(Path(file_path).parent)
                    parent_norm = parent_path.replace('\\', '/')
                    if '/raw/' in parent_norm:
                        group['dataset_dir'] = parent_path
            
            # 构建分层显示列表
            filtered_list = []
            
            for provider, datasets in sorted(provider_groups.items()):
                # 提供商层级显示规则：
                # - 原始数据提供商（如 huggingface/modelscope）：多于1个数据集或本就应分组
                # - 处理数据/蒸馏数据：也显示顶层分组，便于分类浏览
                should_show_provider = (
                    len(datasets) > 1 or provider not in ["其他"] or provider in ["处理数据", "蒸馏数据"]
                )
                
                if should_show_provider:
                    provider_total_size = sum(group['total_size'] for group in datasets.values())
                    provider_file_count = sum(group['file_count'] for group in datasets.values())

                    # 计算提供商基础目录
                    if provider in ["处理数据"]:
                        provider_dir = str(self.root_dir / 'processed')
                    elif provider in ["蒸馏数据"]:
                        provider_dir = str(self.root_dir / 'distilled')
                    else:
                        # 原始数据的具体提供商（huggingface/modelscope等）
                        provider_dir = str(self.root_dir / 'raw' / provider)

                    # 计算实际大小
                    provider_actual_size = self._calculate_directory_size(provider_dir)

                    filtered_list.append({
                        'display_name': f"📁 {provider}",
                        'type': '提供商',
                        'size_mb': provider_actual_size,
                        'create_time': max(group['latest_time'] for group in datasets.values()),
                        'path': provider_dir,
                        'file_count': provider_file_count,
                        'is_provider': True,
                        'provider': provider
                    })
                
                # 添加数据集层级
                for dataset_name, group in sorted(datasets.items()):
                    # 计算数据集目录的实际大小
                    actual_size = self._calculate_directory_size(group['dataset_dir']) if group['dataset_dir'] else group['total_size']
                    
                    # 如果显示了提供商层级，数据集名称加缩进
                    display_name = f"  📊 {dataset_name}" if should_show_provider else f"📊 {dataset_name}"
                    
                    filtered_list.append({
                        'display_name': display_name,
                        'type': '数据集',
                        'size_mb': actual_size,
                        'create_time': group['latest_time'],
                        'path': group['dataset_dir'] or (group['files'][0]['path'] if group['files'] else ''),
                        'file_count': group['file_count'],
                        'is_dataset': True,
                        'provider': provider
                    })
            
            return filtered_list
            
        except Exception as e:
            self.logger.error(f'过滤数据集列表失败: {e}')
            return datasets  # 返回原始列表作为备用
    
    def _extract_dataset_name_from_path(self, file_path: str) -> str:
        """从文件路径中提取数据集名称"""
        try:
            from pathlib import Path
            path = Path(file_path)
            
            # 去除文件名，获取目录名
            if path.is_file() or '.' in path.name:
                path = path.parent
            
            # 获取最接近的有意义的目录名
            parts = path.parts
            for i in reversed(range(len(parts))):
                part = parts[i]
                if part not in ['processed', 'distilled', 'raw', 'data', 'cache', 'organized_files']:
                    return part
            
            return "未知数据集"
        except:
            return "未知数据集"
    
    def _extract_processed_info(self, file_path: str) -> dict:
        """从处理数据路径中提取处理信息"""
        try:
            from pathlib import Path
            path = Path(file_path)
            
            # 获取处理数据的目录结构信息
            parts = path.parts
            processed_idx = -1
            for i, part in enumerate(parts):
                if part == 'processed':
                    processed_idx = i
                    break
            
            if processed_idx >= 0 and processed_idx + 1 < len(parts):
                # 处理类型目录 (如 cleaned, merged, converted等)
                process_type = parts[processed_idx + 1]
                
                # 数据集名称
                if processed_idx + 2 < len(parts):
                    dataset_name = parts[processed_idx + 2]
                else:
                    dataset_name = process_type
                
                # 组合显示名称，包含处理动作
                display_name = f"{dataset_name} ({process_type})"
                
                return {
                    'name': display_name,
                    'process_type': process_type,
                    'dataset_name': dataset_name,
                    'action': self._get_process_action_name(process_type)
                }
            
            # 如果无法解析，返回基本信息
            return {
                'name': path.stem,
                'process_type': '未知处理',
                'dataset_name': path.stem,
                'action': '数据处理'
            }
            
        except Exception as e:
            return {
                'name': '未知处理数据',
                'process_type': '未知',
                'dataset_name': '未知',
                'action': '处理'
            }
    
    def _extract_distilled_info(self, file_path: str) -> dict:
        """从蒸馏数据路径中提取蒸馏信息"""
        try:
            from pathlib import Path
            path = Path(file_path)
            
            # 获取蒸馏数据的目录结构信息
            parts = path.parts
            distilled_idx = -1
            for i, part in enumerate(parts):
                if part == 'distilled':
                    distilled_idx = i
                    break
            
            if distilled_idx >= 0 and distilled_idx + 1 < len(parts):
                # 蒸馏任务目录
                task_name = parts[distilled_idx + 1]
                
                # 数据集名称
                if distilled_idx + 2 < len(parts):
                    dataset_name = parts[distilled_idx + 2]
                else:
                    dataset_name = task_name
                
                # 组合显示名称，包含蒸馏信息
                display_name = f"{dataset_name} (蒸馏-{task_name})"
                
                return {
                    'name': display_name,
                    'task_name': task_name,
                    'dataset_name': dataset_name,
                    'action': '数据蒸馏'
                }
            
            # 如果无法解析，返回基本信息
            return {
                'name': path.stem,
                'task_name': '未知任务',
                'dataset_name': path.stem,
                'action': '数据蒸馏'
            }
            
        except Exception as e:
            return {
                'name': '未知蒸馏数据',
                'task_name': '未知',
                'dataset_name': '未知',
                'action': '蒸馏'
            }
    
    def _get_process_action_name(self, process_type: str) -> str:
        """根据处理类型获取友好的动作名称"""
        action_map = {
            'cleaned': '数据清洗',
            'merged': '数据合并',
            'converted': '格式转换',
            'extracted': '字段提取',
            'filtered': '数据过滤',
            'augmented': '数据增强',
            'normalized': '数据标准化',
            'split': '数据分割'
        }
        return action_map.get(process_type.lower(), f'{process_type}处理')
    
    def _calculate_directory_size(self, dir_path: str) -> float:
        """计算目录的实际磁盘大小（MB）"""
        try:
            from pathlib import Path
            import os
            
            if not dir_path or not os.path.exists(dir_path):
                return 0.0
            
            total_size = 0
            for root, dirs, files in os.walk(dir_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        total_size += os.path.getsize(file_path)
                    except (OSError, IOError):
                        continue  # 跳过无法访问的文件
            
            # 转换为MB
            return total_size / (1024 * 1024)
        
        except Exception as e:
            self.logger.warning(f'计算目录大小失败 {dir_path}: {e}')
            return 0.0
    
    def _setup_auto_refresh(self, demo):
        """设置自动刷新"""
        def auto_refresh():
            """自动刷新任务状态"""
            while True:
                try:
                    time.sleep(self.update_interval)
                    
                    # 只在有组件实例时更新
                    if hasattr(self, 'components') and self.components:
                        # 这里可以实现定时更新逻辑
                        # 由于Gradio的限制，实际的自动刷新需要在前端实现
                        pass
                    
                except Exception as e:
                    self.logger.error(f'自动刷新失败: {e}')
                    time.sleep(10)  # 发生错误时延长等待时间
        
        # 启动后台刷新线程
        refresh_thread = threading.Thread(target=auto_refresh, daemon=True)
        refresh_thread.start()


# 全局UI启动器实例
ui_launcher = UILauncher()


if __name__ == "__main__":
    """
    命令行入口，用于启动UI界面
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='自动数据蒸馏软件UI启动器')
    parser.add_argument('--share', action='store_true', help='创建公共链接')
    parser.add_argument('--port', type=int, default=7860, help='服务器端口')
    
    args = parser.parse_args()
    
    print("🚀 启动自动数据蒸馏软件...")
    print(f"🌐 访问地址: http://localhost:{args.port}")
    
    ui_launcher.launch(share=args.share, server_port=args.port)
