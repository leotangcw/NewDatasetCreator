import gradio as gr
from typing import Dict, Any, Tuple, List
from ..dependencies import pd
from ..model_manager import model_manager

class ModelTabManager:
    def __init__(self, launcher):
        self.launcher = launcher
        self.logger = launcher.logger
        
    def create_tab(self):
        """创建模型配置标签页"""
        gr.Markdown("## 模型配置管理")
        gr.Markdown("支持 vLLM、OpenAI、SGlang、Ollama 等多种模型类型")
        
        with gr.Row():
            with gr.Column(scale=1):
                # 模型配置区域
                gr.Markdown("### 添加新模型")
                
                model_name = gr.Textbox(
                    label="模型名称",
                    placeholder="例如：gpt-4 或 local-llama",
                    info="为模型设置一个唯一标识名称",
                    value=self.launcher.config_manager.get_config("ui_state.model.model_name", "")
                )
                model_name.change(lambda x: self.launcher.config_manager.update_config("ui_state.model.model_name", x), inputs=[model_name], outputs=[])
                
                model_type = gr.Dropdown(
                    choices=["vllm", "openai", "sglang", "ollama"],
                    value=self.launcher.config_manager.get_config("ui_state.model.model_type", "openai"),
                    label="模型类型",
                    info="选择模型的部署类型"
                )
                model_type.change(lambda x: self.launcher.config_manager.update_config("ui_state.model.model_type", x), inputs=[model_type], outputs=[])
                
                model_url = gr.Textbox(
                    label="模型URL",
                    placeholder="例如：http://localhost:8000/v1 或 https://api.openai.com/v1",
                    info="模型服务的API地址",
                    value=self.launcher.config_manager.get_config("ui_state.model.model_url", "")
                )
                model_url.change(lambda x: self.launcher.config_manager.update_config("ui_state.model.model_url", x), inputs=[model_url], outputs=[])
                
                model_api_key = gr.Textbox(
                    label="API密钥（可选）",
                    type="password",
                    placeholder="输入API密钥",
                    info="某些模型需要API密钥认证",
                    value=self.launcher.config_manager.get_config("ui_state.model.model_api_key", "")
                )
                model_api_key.change(lambda x: self.launcher.config_manager.update_config("ui_state.model.model_api_key", x), inputs=[model_api_key], outputs=[])
                
                model_model_name = gr.Textbox(
                    label="实际模型名",
                    placeholder="例如：gpt-4 或 llama-2-7b",
                    info="API调用时使用的模型名称",
                    value=self.launcher.config_manager.get_config("ui_state.model.model_model_name", "")
                )
                model_model_name.change(lambda x: self.launcher.config_manager.update_config("ui_state.model.model_model_name", x), inputs=[model_model_name], outputs=[])
                
                with gr.Row():
                    add_model_btn = gr.Button("添加模型", variant="primary")
                    test_all_btn = gr.Button("测试所有模型", variant="secondary")
            
            with gr.Column(scale=2):
                # 模型列表区域
                gr.Markdown("### 已配置模型列表")
                
                model_list = gr.Dataframe(
                    headers=["模型名称", "类型", "状态", "URL", "响应时间", "操作"],
                    datatype=["str", "str", "str", "str", "str", "str"],
                    label="",
                    interactive=False,
                    wrap=True
                )
                
                with gr.Row():
                    test_model_btn = gr.Button("测试模型", size="sm")
                    delete_model_btn = gr.Button("删除模型", size="sm", variant="stop")
                    refresh_model_btn = gr.Button("刷新列表", size="sm")
                
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
        
        # 自动刷新定时器
        auto_refresh_timer = gr.Timer(value=2)
        auto_refresh_timer.tick(
            fn=self._get_models_df,
            outputs=[model_list]
        )
        
        # 存储组件引用
        self.launcher.components['model'] = {
            'auto_refresh_timer': auto_refresh_timer,
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

    def _add_model(self, name: str, model_type: str, url: str, 
                  api_key: str, model_name: str) -> Tuple[str, Any]:
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
    
    def _test_all_models(self) -> Tuple[str, Any]:
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
    
    def _test_model(self, model_name: str) -> Tuple[str, Any]:
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
    
    def _delete_model(self, model_name: str) -> Tuple[str, Any]:
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
    
    def _refresh_models(self) -> Any:
        """刷新模型列表"""
        return self._get_models_df()
    
    def _select_model(self, evt: gr.SelectData) -> str:
        """选择模型"""
        if evt.index[1] == 0:  # 点击模型名称列
            return evt.value
        return ""
    
    def _get_models_df(self) -> Any:
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

def create_model_tab(launcher):
    manager = ModelTabManager(launcher)
    manager.create_tab()
    return manager
