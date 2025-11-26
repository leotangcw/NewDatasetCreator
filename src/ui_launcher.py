#!/usr/bin/env python3
"""
网页UI启动器

本模块基于Gradio实现网页界面，提供数据集下载、数据加工、模型配置、蒸馏生成、数据管理等功能的可视化操作入口。
"""

import os
import threading
import time
from pathlib import Path
import gradio as gr

# 基础支撑层导入
from .config_manager import config_manager
from .log_manager import log_manager
from .dataset_previewer import DatasetPreviewer, PreviewConfig
from .dataset_downloader import DatasetDownloader
from .format_converter import FormatConverter
from .field_extractor import FieldExtractor
from .data_merger import DataMerger

# 导入UI标签页管理器
from .ui.download_tab import create_download_tab
from .ui.process_tab import create_process_tab
from .ui.model_tab import create_model_tab
from .ui.distill_tab import create_distill_tab
from .ui.manage_tab import create_manage_tab

class UILauncher:
    """
    UI启动器类
    
    负责构建Gradio界面，对接所有核心功能模块。
    """
    
    def __init__(self):
        """初始化UI启动器"""
        self.logger = log_manager.get_logger('ui_launcher')
        self.config_manager = config_manager
        
        # 获取配置
        self.root_dir = Path(config_manager.get_config('base.root_dir', './data'))
        self.update_interval = 2  # 状态更新间隔（秒）
        
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
        self.dataset_downloader = DatasetDownloader()
        self.format_converter = FormatConverter()
        self.field_extractor = FieldExtractor()
        self.data_merger = DataMerger()
        
        # 界面组件存储
        self.components = {}
        
        # 状态管理
        self.merge_file_paths = []  # 存储待合并的文件路径
        
        self.logger.info('UI启动器初始化完成')
    
    def _get_saved_token(self, platform: str) -> str:
        """获取保存的token"""
        try:
            config_key = f'tokens.{platform}'
            return config_manager.get_config(config_key, '')
        except Exception as e:
            self.logger.warning(f'获取{platform} token失败: {e}')
            return ''
    
    def _save_token(self, platform: str, token: str):
        """保存token到配置"""
        try:
            if token.strip():
                config_key = f'tokens.{platform}'
                config_manager.update_config(config_key, token.strip())
                self.logger.info(f'{platform} token已保存到配置')
            else:
                config_key = f'tokens.{platform}'
                config_manager.update_config(config_key, '')
        except Exception as e:
            self.logger.error(f'保存{platform} token失败: {e}')
    
    def launch(self, share: bool = False, server_port: int = 7860):
        """启动Gradio界面"""
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
                    self.download_manager = create_download_tab(self)
                
                # 标签页2：数据加工
                with gr.TabItem("🔧 数据加工", id="process"):
                    self.process_manager = create_process_tab(self)
                
                # 标签页3：模型配置
                with gr.TabItem("⚙️ 模型配置", id="model"):
                    self.model_manager = create_model_tab(self)
                
                # 标签页4：蒸馏生成
                with gr.TabItem("🧠 蒸馏生成", id="distill"):
                    self.distill_manager = create_distill_tab(self)
                
                # 标签页5：数据管理
                with gr.TabItem("📊 数据管理", id="manage"):
                    self.manage_manager = create_manage_tab(self)
            
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
        /* 全局字体设置 */
        body, button, input, select, textarea, .gradio-container {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif !important;
        }
        
        /* 标题样式优化 */
        h1, h2, h3, h4, h5, h6 {
            font-weight: 600 !important;
            color: var(--body-text-color);
        }
        
        /* 按钮样式微调 */
        button.primary {
            font-weight: 500 !important;
        }
        
        /* 表格样式优化 */
        .dataframe-wrap {
            border: 1px solid #e5e7eb;
            border-radius: 6px;
        }
        
        /* 滚动条美化 */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: transparent; 
        }
        ::-webkit-scrollbar-thumb {
            background: #d1d5db; 
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #9ca3af; 
        }
        
        /* 特定表格高度控制 */
        .dataset-list-table .dataframe-wrap {
            max_height: 400px !important;
        }
        
        .convert-task-table .dataframe-wrap {
            max_height: 300px !important;
        }
        
        /* 预览表格容器 */
        .preview-table-container {
            margin-top: 12px;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 0;
            overflow: hidden;
            background-color: var(--background-fill-primary);
        }
        
        /* 隐藏 Gradio Footer */
        footer {
            display: none !important;
        }
        """

    def _setup_auto_refresh(self, demo):
        """设置自动刷新"""
        def auto_refresh():
            """自动刷新任务状态"""
            while True:
                try:
                    time.sleep(self.update_interval)
                    # 只在有组件实例时更新
                    if hasattr(self, 'components') and self.components:
                        pass
                except Exception as e:
                    self.logger.error(f'自动刷新失败: {e}')
                    time.sleep(10)
        
        # 启动后台刷新线程
        refresh_thread = threading.Thread(target=auto_refresh, daemon=True)
        refresh_thread.start()

# 全局UI启动器实例
ui_launcher = UILauncher()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='自动数据蒸馏软件UI启动器')
    parser.add_argument('--share', action='store_true', help='创建公共链接')
    parser.add_argument('--port', type=int, default=7860, help='服务器端口')
    args = parser.parse_args()
    
    print("🚀 启动自动数据蒸馏软件...")
    print(f"🌐 访问地址: http://localhost:{args.port}")
    
    ui_launcher.launch(share=args.share, server_port=args.port)
