import os
import json
import time
import threading
from datetime import datetime
from pathlib import Path
import gradio as gr
from typing import Dict, Any, Tuple, List, Optional
from ..dependencies import pd
from ..data_manager import data_manager
from ..dataset_previewer import DatasetPreviewer
from ..universal_field_extractor import get_field_names_universal, extract_fields_universal

class ManageTabManager:
    def __init__(self, launcher):
        self.launcher = launcher
        self.logger = launcher.logger
        self.root_dir = launcher.root_dir
        self.dataset_previewer = DatasetPreviewer()
        
        # 缓存当前数据集的数据和字段信息，避免重复读取
        self.current_dataset_cache = {
            'path': None,
            'data': None,
            'fields': [],
            'original_preview': None
        }
        
    def create_tab(self):
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
            
            search_dataset_btn = gr.Button("搜索", size="sm", scale=1)
        
        # 数据集列表区域
        gr.Markdown("### 数据集列表")
        gr.Markdown("💡 **操作提示**：点击表格行选择数据集 • 表格支持水平滚动查看完整内容 • 悬停单元格显示完整文本")
        gr.Markdown("⚠️ **删除警告**：删除数据会永久删除数据集文件或整个数据集目录，请谨慎操作！")
                
        dataset_list = gr.Dataframe(
            headers=["名称", "类型", "大小", "创建时间", "路径"],
            datatype=["str", "str", "str", "str", "str"],
            label="",
            interactive=False,
            wrap=True,
            elem_classes="dataset-list-table"
        )
        
        # 操作按钮区域 - 放在列表下方
        with gr.Row():
            refresh_data_btn = gr.Button("刷新列表", size="sm")
            preview_data_btn = gr.Button("预览数据", size="sm")
            delete_data_btn = gr.Button("危险删除", size="sm", variant="stop")
        
        selected_dataset = gr.Textbox(
            label="选中数据集",
            placeholder="点击数据集行选择",
            interactive=False
        )
        
        # 页面加载时自动刷新列表
        # 使用 Timer 触发一次，并在回调中关闭 Timer
        init_refresh_timer = gr.Timer(value=0.5)
        
        def _init_refresh():
            return self._refresh_datasets("全部", ""), gr.Timer(active=False)
            
        init_refresh_timer.tick(
            fn=_init_refresh,
            inputs=[],
            outputs=[dataset_list, init_refresh_timer]
        )
        
        # 数据预览区域 - 改为下方完整区域
        gr.Markdown("### 数据预览")
        
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
                gr.Markdown("#### 字段选择")
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
        
        # 预览结果显示 - 使用HTML组件替代Dataframe以获得更好的控制
        result_display_html = gr.HTML(
            label="数据内容预览",
            value="<div style='padding: 20px; text-align: center; color: #666;'>请选择数据集进行预览</div>",
            elem_classes="preview-table-container"
        )
        
        # 隐藏的原Dataframe组件，用于保持接口兼容（如果需要）
        result_display = gr.Dataframe(visible=False)
        
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
        self.launcher.components['manage'] = {
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
            'result_display_html': result_display_html,
            'data_status': data_status,
            'expanded_text': expanded_text,
            'expand_text_btn': expand_text_btn
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
            outputs=[result_display_html, data_status]
        )
        
        delete_data_btn.click(
            fn=self._delete_dataset,
            inputs=[selected_dataset, data_type_filter],
            outputs=[data_status, dataset_list, selected_dataset, dataset_info, result_display_html]
        )
        
        # 数据集列表点击事件 - 支持自动预览和字段加载
        dataset_list.select(
            fn=self._select_dataset_with_auto_preview,
            inputs=[auto_preview, preview_rows, text_truncation, max_text_length],
            outputs=[selected_dataset, dataset_info, result_display_html, field_selector, data_status]
        )
        
        # 预览行数变化事件，实时更新预览
        preview_rows.change(
            fn=self._update_preview_rows,
            inputs=[selected_dataset, preview_rows, text_truncation, max_text_length, field_selector],
            outputs=[result_display_html, data_status]
        )
        
        # 字段选择器变化事件 - 实时更新预览
        field_selector.change(
            fn=self._update_preview_by_fields,
            inputs=[field_selector, preview_rows, text_truncation, max_text_length],
            outputs=[result_display_html, data_status]
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

    def _filter_datasets(self, data_type: str, search_name: str = "") -> Any:
        """根据数据类型和名称筛选数据集"""
        return self._get_datasets_df(data_type, search_name)
    
    def _refresh_datasets(self, data_type: str, search_name: str = "") -> Any:
        """刷新数据集列表（优化版本）"""
        try:
            self.logger.info(f"开始刷新数据集列表: 类型={data_type}, 搜索={search_name}")
            
            # 直接在主线程执行，避免线程池带来的上下文问题和潜在的死锁
            # 对于文件系统操作，Python的GIL会释放，所以不会完全阻塞
            return self._get_datasets_df(data_type, search_name)
            
        except Exception as e:
            self.logger.error(f"刷新数据集列表失败: {e}")
            return pd.DataFrame(columns=["名称", "类型", "大小", "创建时间", "路径"])
            self.logger.error(f'刷新数据集列表失败: {e}')
            # 返回空的DataFrame，但包含正确的列名
            return pd.DataFrame(columns=["名称", "类型", "大小", "创建时间", "路径"])
    
    def _preview_dataset(self, dataset_path: str, rows: int, 
                        enable_truncation: bool = True, max_text_length: int = 300,
                        show_metadata: bool = True, show_stats: bool = True,
                        smart_columns: bool = True, show_all_columns: bool = False,
                        column_info_display: bool = False) -> Tuple[Any, str]:
        """预览数据集 - 使用新的增强预览器"""
        try:
            if not dataset_path.strip():
                return "", "❌ 请选择要预览的数据集"
            
            if not os.path.exists(dataset_path):
                return "", "❌ 数据集文件不存在"
            
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
                return "", f"❌ 预览失败: {preview_result.error_message}"
            
            if not preview_result.data:
                return "", "❌ 数据集为空"
            
            # 转换为HTML表格
            df = pd.DataFrame(preview_result.data)
            
            # 生成HTML表格，添加自定义类以便CSS控制
            html_table = df.to_html(classes="dataframe", index=False, escape=True)
            # 包装在div中以支持滚动
            html_content = f'<div class="dataframe-wrap">{html_table}</div>'
            
            # 构建详细状态信息
            status_parts = [f"✅ 预览成功！"]
            
            # 基础信息
            status_parts.append(f"📁 路径: {dataset_path}")
            status_parts.append(f"📊 格式: {preview_result.format.value.upper()}")
            
            total_rows_str = f"{preview_result.total_rows:,}" if preview_result.total_rows != -1 else "未知 (文件过大)"
            status_parts.append(f"📈 总行数: {total_rows_str}")
            
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
            
            return html_content, status_msg
            
        except Exception as e:
            self.logger.error(f'预览数据集失败: {e}')
            return "", f"❌ 预览失败: {str(e)}"
    
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
                                 column_info_display: bool) -> Tuple[str, Any, str]:
        """增强预览数据集（通过预览按钮触发）"""
        try:
            if not dataset_path.strip():
                return "*请先选择要预览的数据集*", "", "❌ 请选择要预览的数据集"
            
            # 调用增强预览功能
            preview_html, status_msg = self._preview_dataset(
                dataset_path, rows, enable_truncation, max_text_length, 
                show_metadata, show_stats, smart_columns, show_all_columns, column_info_display
            )
            
            # 生成数据集信息
            # 注意：这里我们不再有preview_df，所以需要调整_generate_dataset_info
            # 或者我们可以在_preview_dataset中返回df和html
            # 为了简化，我们这里只传递status_msg
            dataset_info = self._generate_dataset_info(dataset_path, None, status_msg)
            
            return dataset_info, preview_html, status_msg
            
        except Exception as e:
            self.logger.error(f'增强预览数据集失败: {e}')
            error_msg = f"❌ 预览失败: {str(e)}"
            return "*预览失败*", "", error_msg
    
    def _generate_dataset_info(self, dataset_path: str, preview_df: Any, status_msg: str) -> str:
        """生成数据集信息Markdown"""
        try:
            # if preview_df.empty:
            #     return "*暂无数据集信息*"
            
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
            if hasattr(preview_df, 'empty') and not preview_df.empty:
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
    
    def _delete_dataset(self, dataset_path: str, data_type: str) -> Tuple[str, Any, str, str, Any]:
        """删除数据集"""
        try:
            if not dataset_path.strip():
                return "❌ 请选择要删除的数据集", pd.DataFrame(), "", "", ""
            
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
            success = data_manager.delete_data(dataset_path)
            
            if success:
                # 删除成功后刷新当前选中的数据类型列表
                refreshed_df = self._get_datasets_df(data_type)
                
                # 清空选中状态和显示内容
                return (f"✅ 删除{path_type}完成！\n{path_type}: {dataset_path}", 
                       refreshed_df, 
                       "",  # 清空selected_dataset
                       "",  # 清空dataset_info
                       "")  # 清空result_display用空字符串
            else:
                return f"❌ 删除{path_type}失败：可能文件不存在或权限不足", pd.DataFrame(), dataset_path, "", ""
                
        except Exception as e:
            self.logger.error(f'删除数据集失败: {e}')
            return f"❌ 删除失败: {str(e)}", pd.DataFrame(), dataset_path, "", ""
    
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
                                            column_info_display: bool) -> Tuple[str, str, Any, str]:
        """选择数据集并使用增强预览"""
        try:
            # 获取当前行的所有数据
            row_data = evt.row_value
            if not row_data or len(row_data) < 5:
                return "", "*请选择有效的数据集*", "", ""
            
            # 提取数据集信息
            dataset_name = row_data[0]
            dataset_type = row_data[1]
            dataset_size = row_data[2]
            dataset_time = row_data[3]
            dataset_path = row_data[4]
            
            # 如果启用自动预览，则执行增强预览
            if auto_preview and dataset_path:
                preview_html, status_msg = self._preview_dataset(
                    dataset_path, preview_rows, enable_truncation, max_text_length, 
                    show_metadata, show_stats, smart_columns, show_all_columns, column_info_display
                )
                dataset_info = self._generate_dataset_info(dataset_path, preview_html, status_msg)
                return dataset_path, dataset_info, preview_html, status_msg
            else:
                # 构建基础数据集信息显示
                info_text = f"""**📊 数据集信息**
- **名称**: {dataset_name}
- **类型**: {dataset_type}
- **大小**: {dataset_size}
- **创建时间**: {dataset_time}
- **路径**: `{os.path.basename(dataset_path)}`"""
                
                return dataset_path, info_text, "", "✅ 数据集已选择，点击预览按钮查看数据内容"
                
        except Exception as e:
            self.logger.error(f'选择数据集失败: {e}')
            return "", f"❌ 选择失败: {str(e)}", "", ""
    
    def _get_datasets_df(self, data_type: str = "全部", search_name: str = "") -> Any:
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
                                        max_text_length: int) -> Tuple[str, str, Any, gr.CheckboxGroup, str]:
        """选择数据集并自动预览（新版本，包含字段选择）"""
        try:
            # 获取当前行的所有数据
            row_data = evt.row_value
            if not row_data or len(row_data) < 5:
                return "", "*请选择有效的数据集*", "", gr.CheckboxGroup(choices=[], value=[]), ""
            
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
                
                return dataset_path, info_text, "", gr.CheckboxGroup(choices=[], value=[]), "✅ 数据集已选择，等待预览..."
                
        except Exception as e:
            self.logger.error(f'选择数据集失败: {e}')
            return "", f"❌ 选择失败: {str(e)}", "", gr.CheckboxGroup(choices=[], value=[]), ""
    
    def _load_dataset_with_fields(self, dataset_path: str, preview_rows: int, 
                                enable_truncation: bool, max_text_length: int) -> Tuple[str, str, Any, gr.CheckboxGroup, str]:
        """加载数据集并分析字段（核心功能）"""
        try:
            if not dataset_path.strip():
                return "", "*请选择要预览的数据集*", "", gr.CheckboxGroup(choices=[], value=[]), "❌ 请选择要预览的数据集"
            
            if not os.path.exists(dataset_path):
                return "", "*数据集文件不存在*", "", gr.CheckboxGroup(choices=[], value=[]), "❌ 数据集文件不存在"
            
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
                    return "", "*预览失败*", "", gr.CheckboxGroup(choices=[], value=[]), f"❌ 预览失败: {preview_result.error_message}"
                
                if not preview_result.data:
                    return "", "*数据集为空*", "", gr.CheckboxGroup(choices=[], value=[]), "❌ 数据集为空"
                
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
            
            # 转换为HTML
            if not preview_df.empty:
                html_table = preview_df.to_html(classes="dataframe", index=False, escape=True)
                html_content = f'<div class="dataframe-wrap">{html_table}</div>'
            else:
                html_content = ""
            
            return dataset_path, dataset_info, html_content, field_selector, status_msg
            
        except Exception as e:
            self.logger.error(f'加载数据集和字段失败: {e}')
            error_msg = f"❌ 加载失败: {str(e)}"
            return "", "*加载失败*", "", gr.CheckboxGroup(choices=[], value=[]), error_msg
    
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
                                enable_truncation: bool, max_text_length: int) -> Tuple[Any, str]:
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
                            from ..universal_field_extractor import _extractor
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
    
    def _generate_dataset_info_from_cache(self, dataset_path: str, preview_df: Any) -> str:
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

    def _preview_dataset_with_field_filter(self, dataset_path: str, preview_rows: int,
                                         enable_truncation: bool, max_text_length: int,
                                         selected_fields: List[str]) -> Tuple[str, str]:
        """使用字段过滤器预览数据集（手动预览按钮）"""
        try:
            if not dataset_path.strip():
                return "", "❌ 请选择要预览的数据集"
            
            # 如果不是当前缓存的数据集，重新加载
            if self.current_dataset_cache['path'] != dataset_path:
                # _load_dataset_with_fields returns: dataset_path, dataset_info, html_content, field_selector, status_msg
                _, _, html_content, _, status_msg = self._load_dataset_with_fields(
                    dataset_path, preview_rows, enable_truncation, max_text_length
                )
                return html_content, status_msg
            
            # 使用选中的字段过滤预览
            if selected_fields:
                # _filter_preview_by_fields returns: df, status_msg
                df, status_msg = self._filter_preview_by_fields(selected_fields, enable_truncation, max_text_length)
                if not df.empty:
                    html_table = df.to_html(classes="dataframe", index=False, escape=True)
                    html_content = f'<div class="dataframe-wrap">{html_table}</div>'
                    return html_content, status_msg
                return "", status_msg
            else:
                return "", "💡 请选择要显示的字段"
        except Exception as e:
            self.logger.error(f'预览数据集失败: {e}')
            return "", f"❌ 预览失败: {str(e)}"
    
    def _update_preview_rows(self, dataset_path: str, preview_rows: int, 
                           enable_truncation: bool, max_text_length: int,
                           selected_fields: List[str]) -> Tuple[Any, str]:
        """更新预览行数"""
        try:
            if not dataset_path or not dataset_path.strip():
                return "", "💡 请先选择数据集"
            
            # 强制重新加载数据（因为行数变了，缓存的数据可能不够）
            # 清空当前缓存的数据，迫使 _load_dataset_with_fields 重新读取
            self.current_dataset_cache['data'] = None
            
            # 重新加载并应用字段过滤
            _, _, preview_df, _, status_msg = self._load_dataset_with_fields(
                dataset_path, preview_rows, enable_truncation, max_text_length
            )
            
            # 如果有选中的字段，应用过滤
            if selected_fields:
                # 注意：_load_dataset_with_fields 返回的是 HTML 字符串 preview_df
                # 但我们需要重新过滤，所以这里逻辑稍微复杂点
                # 实际上 _load_dataset_with_fields 内部已经更新了 cache['data']
                # 我们直接调用 _filter_preview_by_fields 即可
                df, msg = self._filter_preview_by_fields(selected_fields, enable_truncation, max_text_length)
                if not df.empty:
                    html_table = df.to_html(classes="dataframe", index=False, escape=True)
                    html_content = f'<div class="dataframe-wrap">{html_table}</div>'
                    return html_content, msg
            
            # 如果没有选中字段或过滤失败，返回默认加载的结果
            return preview_df, status_msg
            
        except Exception as e:
            self.logger.error(f'更新预览行数失败: {e}')
            return "", f"❌ 更新失败: {str(e)}"

    def _update_preview_by_fields(self, selected_fields: List[str], 
                                preview_rows: int, enable_truncation: bool, 
                                max_text_length: int) -> Tuple[str, str]:
        """根据字段选择更新预览（不重新加载文件）"""
        try:
            # 检查是否有缓存数据
            if not self.current_dataset_cache['data']:
                return "", "❌ 请先选择数据集"
            
            # 调用过滤逻辑
            df, status_msg = self._filter_preview_by_fields(selected_fields, enable_truncation, max_text_length)
            
            if not df.empty:
                html_table = df.to_html(classes="dataframe", index=False, escape=True)
                html_content = f'<div class="dataframe-wrap">{html_table}</div>'
                return html_content, status_msg
            else:
                return "", status_msg
                
        except Exception as e:
            self.logger.error(f'更新字段预览失败: {e}')
            return "", f"❌ 更新失败: {str(e)}"

    def _select_all_dataset_fields(self) -> Dict[str, Any]:
        """全选所有字段"""
        try:
            all_fields = self.current_dataset_cache['fields']
            field_choices = [(field, field) for field in all_fields]
            return gr.update(
                choices=field_choices,
                value=all_fields,
                interactive=True
            )
        except Exception as e:
            self.logger.error(f'全选字段失败: {e}')
            return gr.update(choices=[], value=[])
    
    def _clear_all_dataset_fields(self) -> Dict[str, Any]:
        """清除所有字段选择"""
        try:
            all_fields = self.current_dataset_cache['fields']
            field_choices = [(field, field) for field in all_fields]
            return gr.update(
                choices=field_choices,
                value=[],
                interactive=True
            )
        except Exception as e:
            self.logger.error(f'清除字段选择失败: {e}')
            return gr.update(choices=[], value=[])
    
    def _select_common_dataset_fields(self) -> Dict[str, Any]:
        """选择常用字段"""
        try:
            all_fields = self.current_dataset_cache['fields']
            common_fields = self._identify_common_fields(all_fields)
            field_choices = [(field, field) for field in all_fields]
            return gr.update(
                choices=field_choices,
                value=common_fields,
                interactive=True
            )
        except Exception as e:
            self.logger.error(f'选择常用字段失败: {e}')
            return gr.update(choices=[], value=[])

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

def create_manage_tab(launcher):
    """创建数据管理标签页"""
    manager = ManageTabManager(launcher)
    manager.create_tab()
    return manager

