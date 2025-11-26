import gradio as gr
from datetime import datetime
from typing import Dict, Any, Tuple, List
from ..dependencies import pd

class DownloadTabManager:
    def __init__(self, launcher):
        self.launcher = launcher
        self.logger = launcher.logger
        self.dataset_downloader = launcher.dataset_downloader
        
    def create_tab(self):
        """创建数据集下载标签页"""
        gr.Markdown("## 数据集下载管理")
        gr.Markdown("支持从 Hugging Face、ModelScope 等平台下载数据集")
        
        with gr.Row():
            with gr.Column(scale=1):
                # 下载配置区域
                gr.Markdown("### 下载配置")
                
                source_type = gr.Dropdown(
                    choices=["huggingface", "modelscope", "url"],
                    value=self.launcher.config_manager.get_config("ui_state.download.source_type", "huggingface"),
                    label="数据源类型",
                    info="选择数据集来源平台"
                )
                source_type.change(lambda x: self.launcher.config_manager.update_config("ui_state.download.source_type", x), inputs=[source_type], outputs=[])
                
                dataset_name = gr.Textbox(
                    label="数据集名称/URL",
                    placeholder="例如：squad 或 https://example.com/data.zip",
                    info="输入数据集名称或下载链接",
                    value=self.launcher.config_manager.get_config("ui_state.download.dataset_name", "")
                )
                dataset_name.change(lambda x: self.launcher.config_manager.update_config("ui_state.download.dataset_name", x), inputs=[dataset_name], outputs=[])
                
                # 分别配置不同平台的API密钥
                with gr.Row():
                    huggingface_token = gr.Textbox(
                        label="Hugging Face Token",
                        type="password",
                        placeholder="输入Hugging Face访问token",
                        info="用于访问私有Hugging Face数据集",
                        value=self.launcher._get_saved_token('huggingface')
                    )
                    
                    modelscope_token = gr.Textbox(
                        label="ModelScope Token", 
                        type="password",
                        placeholder="输入ModelScope访问token",
                        info="用于访问私有ModelScope数据集",
                        value=self.launcher._get_saved_token('modelscope')
                    )
                
                # 镜像站点选项
                use_hf_mirror = gr.Checkbox(
                    label="使用 Hugging Face 镜像站点 (hf-mirror.com)",
                    value=self.launcher.config_manager.get_config("ui_state.download.use_hf_mirror", False),
                    info="勾选后将通过国内镜像站点下载，解决无法访问 Hugging Face 的问题"
                )
                use_hf_mirror.change(lambda x: self.launcher.config_manager.update_config("ui_state.download.use_hf_mirror", x), inputs=[use_hf_mirror], outputs=[])

                # 绑定Token变更事件，自动保存
                huggingface_token.change(
                    fn=lambda x: self.launcher._save_token('huggingface', x),
                    inputs=[huggingface_token],
                    outputs=[]
                )
                
                modelscope_token.change(
                    fn=lambda x: self.launcher._save_token('modelscope', x),
                    inputs=[modelscope_token],
                    outputs=[]
                )
                
                save_dir = gr.Textbox(
                    label="保存目录",
                    value=self.launcher.config_manager.get_config("ui_state.download.save_dir", str(self.launcher.root_dir / "raw")),
                    info="数据集保存路径"
                )
                save_dir.change(lambda x: self.launcher.config_manager.update_config("ui_state.download.save_dir", x), inputs=[save_dir], outputs=[])
                
                with gr.Row():
                    add_task_btn = gr.Button("添加下载任务", variant="primary")
                    refresh_status_btn = gr.Button("刷新状态", variant="secondary")
            
            with gr.Column(scale=2):
                # 任务列表区域
                gr.Markdown("### 下载任务列表")
                
                # 任务信息显示表格（仅显示，不可选择）
                task_list = gr.Dataframe(
                    headers=["选择", "任务ID", "数据集名称", "状态", "进度", "开始时间"],
                    datatype=["bool", "str", "str", "str", "str", "str"],
                    label="",
                    interactive=True,
                    wrap=False,
                    column_widths=["60px", "200px", "200px", "100px", "100px", "150px"],
                    value=self._get_download_tasks_df()
                )
                
                # 任务选择区域
                with gr.Row():
                    start_task_btn = gr.Button("启动", size="sm", variant="primary")
                    pause_task_btn = gr.Button("暂停", size="sm", variant="secondary")
                    delete_task_btn = gr.Button("删除", size="sm", variant="stop")
                    refresh_list_btn = gr.Button("刷新", size="sm")
        
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
            auto_refresh_timer = gr.Timer(value=2)
        
        # 使用 State 存储选中的任务ID，避免 Dataframe 输入问题
        selected_tasks_state = gr.State(value=set())

        # 使用 State 存储当前 Dataframe 数据，供 select 事件使用
        current_df_state = gr.State(value=pd.DataFrame())

        # 绑定 Dataframe 选择事件，更新 State
        def _on_df_select(evt: gr.SelectData, df_value, current_selection):
            try:
                row_index = evt.index[0]
                col_index = evt.index[1]

                if col_index == 0:  # 复选框列
                    if df_value is None or not hasattr(df_value, 'empty') or df_value.empty:
                        return current_selection

                    task_id = str(df_value.iloc[row_index, 1])  # 确保ID是字符串

                    new_selection = set(current_selection or set())
                    if task_id in new_selection:
                        new_selection.discard(task_id)
                    else:
                        new_selection.add(task_id)

                    return new_selection

                return current_selection
            except Exception as e:
                self.logger.error(f"Selection error: {e}")
                return current_selection

        task_list.select(
            fn=_on_df_select,
            inputs=[current_df_state, selected_tasks_state],
            outputs=[selected_tasks_state]
        )
        
        # 刷新逻辑改为读取 State
        def _refresh_with_state(selected_ids: set):
            new_df = self._refresh_download_tasks_logic_with_state(selected_ids)
            return new_df, new_df

        # 存储组件引用
        self.launcher.components['download'] = {
            'source_type': source_type,
            'dataset_name': dataset_name,
            'huggingface_token': huggingface_token,
            'modelscope_token': modelscope_token,
            'use_hf_mirror': use_hf_mirror,
            'save_dir': save_dir,
            'task_list': task_list,
            'status': download_status,
            'auto_refresh_timer': auto_refresh_timer,
            'selected_tasks_state': selected_tasks_state,
            'current_df_state': current_df_state
        }
        
        # 绑定事件处理器
        add_task_btn.click(
            fn=self._add_download_task,
            inputs=[source_type, dataset_name, huggingface_token, modelscope_token, use_hf_mirror, save_dir],
            outputs=[download_status, task_list, current_df_state]
        )
        
        # 自动刷新
        auto_refresh_timer.tick(
            fn=_refresh_with_state,
            inputs=[selected_tasks_state],
            outputs=[task_list, current_df_state]
        )
        
        # 左侧刷新（配置区）
        refresh_status_btn.click(
            fn=_refresh_with_state,
            inputs=[selected_tasks_state],
            outputs=[task_list, current_df_state]
        )
        
        # 右侧刷新（任务区）
        try:
            refresh_list_btn.click(
                fn=_refresh_with_state,
                inputs=[selected_tasks_state],
                outputs=[task_list, current_df_state]
            )
        except Exception:
            pass
        
        # 批量任务操作
        start_task_btn.click(
            fn=self._start_multiple_tasks,
            inputs=[current_df_state],
            outputs=[download_status, task_list, current_df_state]
        )
        
        pause_task_btn.click(
            fn=self._pause_multiple_tasks,
            inputs=[current_df_state],
            outputs=[download_status, task_list, current_df_state]
        )
        
        delete_task_btn.click(
            fn=self._delete_multiple_tasks,
            inputs=[current_df_state],
            outputs=[download_status, task_list, current_df_state]
        )

    def _add_download_task(self, source_type: str, dataset_name: str, 
                          huggingface_token: str, modelscope_token: str, 
                          use_hf_mirror: bool, save_dir: str) -> Tuple[str, Any, Any]:
        """添加下载任务"""
        try:
            if not dataset_name.strip():
                df = self._get_download_tasks_df()
                return "请输入数据集名称或URL", df, df
            
            # 构建下载参数
            params = {
                'source_type': (source_type or '').strip().lower(),
                'dataset_name': dataset_name.strip(),
                'save_dir': (save_dir.strip() if save_dir else str(self.launcher.root_dir / "raw")),
                'extra_params': {}
            }
            
            # 镜像站点配置
            if use_hf_mirror:
                params['extra_params']['use_hf_mirror'] = True
            
            # 根据source_type选择合适的token并保存
            if params['source_type'] == 'huggingface' and huggingface_token.strip():
                params['token'] = huggingface_token.strip()
                self.launcher._save_token('huggingface', huggingface_token.strip())
            elif params['source_type'] == 'modelscope' and modelscope_token.strip():
                params['token'] = modelscope_token.strip()
                self.launcher._save_token('modelscope', modelscope_token.strip())
            
            # 调用核心模块添加任务（解包参数）
            task_id = self.dataset_downloader.add_download_task(**params)
            
            df = self._get_download_tasks_df()
            return f"下载任务已添加: {task_id}", df, df
            
        except Exception as e:
            self.logger.error(f'添加下载任务失败: {e}')
            df = self._get_download_tasks_df()
            return f"添加任务失败: {str(e)}", df, df
    
    def _refresh_download_tasks(self) -> Any:
        """刷新下载任务列表"""
        return self._get_download_tasks_df()
    
    def _refresh_download_tasks_logic(self, current_df: pd.DataFrame = None) -> pd.DataFrame:
        """刷新下载任务列表，同时保留用户的选择状态"""
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
            new_df = self._get_download_tasks_df()
            
            # 如果有之前的选择，重新应用到新数据上
            if selected_ids and not new_df.empty:
                # 使用 apply 函数更新第一列
                # 注意：这里假设第1列是任务ID
                new_df.iloc[:, 0] = new_df.iloc[:, 1].apply(lambda x: x in selected_ids)
                
            return new_df
        except Exception as e:
            self.logger.error(f'刷新下载任务列表失败: {e}')
            return self._get_download_tasks_df()

    def _refresh_download_tasks_logic_with_state(self, selected_ids: set) -> pd.DataFrame:
        """刷新下载任务列表，并应用 State 中的选择状态"""
        try:
            # 获取最新的任务列表数据
            new_df = self._get_download_tasks_df()
            
            # 如果有之前的选择，重新应用到新数据上
            if selected_ids and not new_df.empty:
                # 使用 apply 函数更新第一列
                # 注意：这里假设第1列是任务ID
                new_df.iloc[:, 0] = new_df.iloc[:, 1].apply(lambda x: str(x) in selected_ids)
                
            return new_df
        except Exception as e:
            self.logger.error(f'刷新下载任务列表失败: {e}')
            return self._get_download_tasks_df()

    def _start_multiple_tasks(self, task_df: pd.DataFrame) -> Tuple[str, Any, Any]:
        """批量开始任务"""
        try:
            if task_df is None or task_df.empty:
                df = self._get_download_tasks_df()
                return "请先选择要开始的任务", df, df
            
            # 获取选中的任务ID (第一列为True的行)
            selected_rows = task_df[task_df.iloc[:, 0] == True]
            if selected_rows.empty:
                return "请先选择要开始的任务", df, df
                
            selected_tasks = selected_rows.iloc[:, 1].tolist()
            
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
            
            summary = f"批量启动完成: {success_count}个成功, {failed_count}个失败"
            details = "\n".join(results)
            
            df = self._get_download_tasks_df()
            return f"{summary}\n\n详情:\n{details}", df, df
                
        except Exception as e:
            self.logger.error(f'批量启动任务失败: {e}')
            df = self._get_download_tasks_df()
            return f"批量启动失败: {str(e)}", df, df

    def _pause_multiple_tasks(self, task_df: pd.DataFrame) -> Tuple[str, Any, Any]:
        """批量暂停任务"""
        try:
            if task_df is None or task_df.empty:
                df = self._get_download_tasks_df()
                return "请先选择要暂停的任务", df, df
            
            # 获取选中的任务ID (第一列为True的行)
            selected_rows = task_df[task_df.iloc[:, 0] == True]
            if selected_rows.empty:
                return "请先选择要暂停的任务", df, df
                
            selected_tasks = selected_rows.iloc[:, 1].tolist()
            
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
            
            df = self._get_download_tasks_df()
            return f"{summary}\n\n详情:\n{details}", df, df
                
        except Exception as e:
            self.logger.error(f'批量暂停任务失败: {e}')
            df = self._get_download_tasks_df()
            return f"批量暂停失败: {str(e)}", df, df

    def _delete_multiple_tasks(self, task_df: pd.DataFrame) -> Tuple[str, Any, Any]:
        """批量删除任务"""
        try:
            if task_df is None or task_df.empty:
                df = self._get_download_tasks_df()
                return "请先选择要删除的任务", df, df
            
            # 获取选中的任务ID (第一列为True的行)
            selected_rows = task_df[task_df.iloc[:, 0] == True]
            if selected_rows.empty:
                return "请先选择要删除的任务", df, df
                
            selected_tasks = selected_rows.iloc[:, 1].tolist()
            
            success_count = 0
            failed_count = 0
            results = []
            
            for task_id in selected_tasks:
                try:
                    success = self.dataset_downloader.delete_task(task_id)
                    if success:
                        success_count += 1
                        results.append(f"✅ {task_id}")
                    else:
                        failed_count += 1
                        results.append(f"❌ {task_id}")
                except Exception as e:
                    failed_count += 1
                    results.append(f"❌ {task_id}: {str(e)}")
            
            summary = f"批量删除完成: {success_count}个成功, {failed_count}个失败"
            details = "\n".join(results)
            
            df = self._get_download_tasks_df()
            return f"{summary}\n\n详情:\n{details}", df, df
                
        except Exception as e:
            self.logger.error(f'批量删除任务失败: {e}')
            df = self._get_download_tasks_df()
            return f"批量删除失败: {str(e)}", df, df

    def _get_download_tasks_df(self) -> Any:
        """获取下载任务列表数据框"""
        try:
            # 获取所有下载任务
            tasks = self.dataset_downloader.list_tasks()
            
            if not tasks:
                return pd.DataFrame(columns=["选择", "任务ID", "数据集名称", "状态", "进度", "开始时间"])
            
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
                    False, # 默认不选中
                    task_id,
                    dataset_name,
                    status_cn,
                    progress_str,
                    start_time_str
                ])
            
            return pd.DataFrame(rows, columns=["选择", "任务ID", "数据集名称", "状态", "进度", "开始时间"])
            
        except Exception as e:
            self.logger.error(f'获取下载任务列表失败: {e}')
            return pd.DataFrame(columns=["选择", "任务ID", "数据集名称", "状态", "进度", "开始时间"])

def create_download_tab(launcher):
    manager = DownloadTabManager(launcher)
    manager.create_tab()
    return manager
