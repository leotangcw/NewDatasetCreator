#!/usr/bin/env python3
"""
模型管理模块

本模块负责管理所有可用模型（vLLM/OpenAI/SGlang/Ollama等），提供统一的模型配置、连接测试、状态查询功能。
功能特点：
- 支持多种模型类型（vLLM、OpenAI、SGlang、Ollama）
- 模型配置的增删改查操作
- 自动连接测试和状态监控
- API密钥加密存储
- 统一的模型调用接口

设计原则：
- 统一的模型接口，屏蔽不同模型的API差异
- 安全的配置管理，敏感信息加密存储
- 可靠的连接测试，确保模型可用性
- 扩展性设计，便于新增模型类型

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import json
import time
from .dependencies import requests
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
from pathlib import Path
from enum import Enum

# 导入统一异常类
try:
    from .exceptions import ModelError, ModelNotFoundError, ModelConnectionError, ModelTimeoutError
except ImportError:
    # 如果导入失败，使用本地定义（向后兼容）
    class ModelError(Exception):
        """模型管理相关异常类"""
        pass
    
    class ModelNotFoundError(ModelError):
        """模型不存在异常"""
        pass
    
    class ModelConnectionError(ModelError):
        """模型连接失败异常"""
        pass
    
    class ModelTimeoutError(ModelError):
        """模型调用超时异常"""
        pass

# 基础支撑层导入
try:
    # 作为模块导入时使用相对导入
    from .config_manager import config_manager
    from .log_manager import log_manager
    from .utils import NetworkUtils, SecurityUtils
except ImportError:
    # 直接运行时使用绝对导入
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from config_manager import config_manager
    from log_manager import log_manager
    from utils import NetworkUtils, SecurityUtils


class ModelType(Enum):
    """支持的模型类型枚举"""
    VLLM = "vllm"
    OPENAI = "openai"
    SGLANG = "sglang"
    OLLAMA = "ollama"


class ModelStatus(Enum):
    """模型状态枚举"""
    UNKNOWN = "unknown"        # 未测试
    ONLINE = "online"          # 在线可用
    OFFLINE = "offline"        # 离线不可用
    ERROR = "error"            # 连接错误


class ModelManager:
    """
    模型管理器
    
    负责管理所有可用模型的配置、连接测试、状态监控等功能。
    """
    
    def __init__(self):
        """初始化模型管理器"""
        self.logger = log_manager.get_logger('model_manager')
        self.config_key = 'models'
        
        # 支持的模型类型
        self.supported_types = config_manager.get_config('model.supported_types', 
                                                        ['vllm', 'openai', 'sglang', 'ollama'])
        
        # 默认配置
        self.default_timeout = config_manager.get_config('model.default_timeout', 600)
        self.test_prompt = config_manager.get_config('model.test_prompt', '测试连接')
        
        # 初始化模型配置
        self._init_models_config()
        
        self.logger.info('模型管理器初始化完成')
    
    def _init_models_config(self) -> None:
        """初始化模型配置节点"""
        try:
            models = config_manager.get_config(self.config_key)
            if models is None:
                config_manager.update_config(self.config_key, {})
                self.logger.info('初始化空的模型配置')
            elif not isinstance(models, dict):
                self.logger.warning(f'模型配置格式错误 (期望dict, 实际{type(models)})，保留原配置')
                # 不覆盖原有配置，防止数据丢失
        except Exception as e:
            self.logger.warning(f'模型配置初始化异常: {e}')
            # 异常时不覆盖配置
    
    def add_model(self, model_info: Dict[str, Any]) -> bool:
        """
        添加新模型配置
        
        Args:
            model_info (dict): 模型信息，包含：
                - name (str): 模型名称
                - type (str): 模型类型 (vllm/openai/sglang/ollama)
                - url (str): 模型API地址
                - api_key (str, optional): API密钥
                - timeout (int, optional): 超时时间
                - model_name (str, optional): 具体模型名称（如gpt-4）
                - custom_headers (dict, optional): 自定义请求头
                
        Returns:
            bool: 添加成功返回True，失败返回False
        """
        try:
            # 验证必填字段
            required_fields = ['name', 'type', 'url']
            for field in required_fields:
                if field not in model_info:
                    raise ValueError(f'缺少必填字段: {field}')
            
            model_name = model_info['name']
            model_type = model_info['type'].lower()
            
            # 验证模型类型
            if model_type not in self.supported_types:
                raise ValueError(f'不支持的模型类型: {model_type}，支持的类型: {self.supported_types}')
            
            # 验证模型名称不重复
            if self._model_exists(model_name):
                raise ValueError(f'模型名称已存在: {model_name}')
            
            # 验证配置格式
            self._validate_model_config(model_info)
            
            # 构建模型配置
            model_config = {
                'type': model_type,
                'url': model_info['url'].rstrip('/'),  # 移除末尾斜杠
                'timeout': model_info.get('timeout', self.default_timeout),
                'model_name': model_info.get('model_name', ''),
                'custom_headers': model_info.get('custom_headers', {}),
                'status': ModelStatus.UNKNOWN.value,
                'last_test_time': '',
                'response_time': 0,
                'error_msg': '',
                'created_time': datetime.now().isoformat()
            }
            
            # 处理API密钥（加密存储）
            api_key = model_info.get('api_key', '')
            if api_key:
                if config_manager.get_config('download.encrypt_api_key', True):
                    encrypted_key = SecurityUtils.encrypt_text(api_key)
                    model_config['api_key'] = f"encrypted:{encrypted_key}"
                else:
                    model_config['api_key'] = api_key
            else:
                model_config['api_key'] = ''
            
            # 保存到配置
            models = config_manager.get_config(self.config_key, {})
            models[model_name] = model_config
            config_manager.update_config(self.config_key, models)
            
            self.logger.info(f'添加模型成功: {model_name} ({model_type})')
            return True
            
        except Exception as e:
            self.logger.error(f'添加模型失败: {e}')
            return False
    
    def _validate_model_config(self, model_info: Dict[str, Any]) -> None:
        """验证模型配置格式"""
        model_type = model_info['type'].lower()
        url = model_info['url']
        
        # URL格式验证
        if not (url.startswith('http://') or url.startswith('https://')):
            raise ValueError('URL必须以http://或https://开头')
        
        # 特定模型类型的验证
        if model_type == ModelType.OPENAI.value:
            if 'api_key' not in model_info or not model_info['api_key']:
                raise ValueError('OpenAI模型必须提供API密钥')
        
        elif model_type == ModelType.OLLAMA.value:
            if not url.endswith('/api/generate') and not url.endswith('/api/chat'):
                self.logger.warning(f'Ollama URL建议使用 /api/generate 或 /api/chat 结尾: {url}')
    
    def _model_exists(self, model_name: str) -> bool:
        """检查模型是否已存在"""
        models = config_manager.get_config(self.config_key, {})
        return model_name in models
    
    def get_model_config(self, model_name: str) -> Optional[Dict[str, Any]]:
        """
        获取模型配置（自动解密API密钥）
        
        Args:
            model_name (str): 模型名称
            
        Returns:
            dict: 模型配置字典，如果模型不存在返回None
        """
        try:
            models = config_manager.get_config(self.config_key, {})
            if model_name not in models:
                return None
            
            config = models[model_name].copy()
            
            # 解密API密钥
            api_key = config.get('api_key', '')
            if api_key.startswith('encrypted:'):
                encrypted_key = api_key[10:]  # 移除 "encrypted:" 前缀
                try:
                    decrypted_key = SecurityUtils.decrypt_text(encrypted_key)
                    config['api_key'] = decrypted_key
                except Exception as e:
                    self.logger.error(f'解密API密钥失败: {e}')
                    config['api_key'] = ''
            
            return config
            
        except Exception as e:
            self.logger.error(f'获取模型配置失败: {e}')
            return None
    
    def update_model(self, model_name: str, updates: Dict[str, Any]) -> bool:
        """
        更新模型配置
        
        Args:
            model_name (str): 模型名称
            updates (dict): 要更新的配置项
            
        Returns:
            bool: 更新成功返回True，失败返回False
        """
        try:
            if not self._model_exists(model_name):
                raise ValueError(f'模型不存在: {model_name}')
            
            models = config_manager.get_config(self.config_key, {})
            current_config = models[model_name]
            
            # 更新配置
            for key, value in updates.items():
                if key == 'api_key' and value:
                    # 重新加密API密钥
                    if config_manager.get_config('download.encrypt_api_key', True):
                        encrypted_key = SecurityUtils.encrypt_text(value)
                        current_config['api_key'] = f"encrypted:{encrypted_key}"
                    else:
                        current_config['api_key'] = value
                elif key in ['url', 'timeout', 'model_name', 'custom_headers']:
                    current_config[key] = value
                elif key == 'url' and value:
                    current_config['url'] = value.rstrip('/')
            
            # 保存更新
            config_manager.update_config(self.config_key, models)
            
            self.logger.info(f'更新模型配置成功: {model_name}')
            return True
            
        except Exception as e:
            self.logger.error(f'更新模型配置失败: {e}')
            return False
    
    def delete_model(self, model_name: str) -> bool:
        """
        删除模型配置
        
        Args:
            model_name (str): 模型名称
            
        Returns:
            bool: 删除成功返回True，失败返回False
        """
        try:
            if not self._model_exists(model_name):
                raise ValueError(f'模型不存在: {model_name}')
            
            models = config_manager.get_config(self.config_key, {})
            del models[model_name]
            config_manager.update_config(self.config_key, models)
            
            self.logger.info(f'删除模型成功: {model_name}')
            return True
            
        except Exception as e:
            self.logger.error(f'删除模型失败: {e}')
            return False
    
    def test_model(self, model_name: str) -> Dict[str, Any]:
        """
        测试模型连接性
        
        Args:
            model_name (str): 模型名称
            
        Returns:
            dict: 测试结果，包含success、response_time、error_msg等字段
        """
        result = {
            'success': False,
            'response_time': 0,
            'error_msg': '',
            'status': ModelStatus.OFFLINE.value
        }
        
        try:
            config = self.get_model_config(model_name)
            if not config:
                result['error_msg'] = f'模型不存在: {model_name}'
                return result
            
            model_type = config['type']
            url = config['url']
            api_key = config['api_key']
            timeout = config.get('timeout', self.default_timeout)
            
            self.logger.info(f'开始测试模型连接: {model_name} ({model_type})')
            
            start_time = time.time()
            
            # 根据模型类型调用不同的测试方法
            if model_type == ModelType.VLLM.value:
                result = self._test_vllm_connection(url, api_key, timeout)
            elif model_type == ModelType.OPENAI.value:
                # 传入配置中的实际后端模型名，以兼容 OpenAI 兼容端点（如 SiliconFlow 等）
                result = self._test_openai_connection(url, api_key, timeout, config.get('model_name') or '')
            elif model_type == ModelType.SGLANG.value:
                result = self._test_sglang_connection(url, api_key, timeout)
            elif model_type == ModelType.OLLAMA.value:
                result = self._test_ollama_connection(url, timeout)
            else:
                result['error_msg'] = f'不支持的模型类型: {model_type}'
            
            # 计算响应时间
            result['response_time'] = round((time.time() - start_time) * 1000, 2)  # 毫秒
            
            # 更新模型状态
            self._update_model_status(model_name, result)
            
            if result['success']:
                self.logger.info(f'模型连接测试成功: {model_name}, 响应时间: {result["response_time"]}ms')
            else:
                self.logger.warning(f'模型连接测试失败: {model_name}, 错误: {result["error_msg"]}')
            
            return result
            
        except Exception as e:
            result['error_msg'] = str(e)
            result['response_time'] = round((time.time() - start_time) * 1000, 2) if 'start_time' in locals() else 0
            self._update_model_status(model_name, result)
            self.logger.error(f'模型连接测试异常: {model_name}, 错误: {e}')
            return result
    
    def _test_vllm_connection(self, url: str, api_key: str, timeout: int) -> Dict[str, Any]:
        """测试vLLM模型连接"""
        headers = {'Content-Type': 'application/json'}
        if api_key:
            headers['Authorization'] = f'Bearer {api_key}'
        
        # vLLM生成接口测试
        data = {
            'prompt': self.test_prompt,
            'max_tokens': 10,
            'temperature': 0.1
        }
        
        try:
            response = requests.post(
                f"{url}/generate" if not url.endswith('/generate') else url,
                headers=headers,
                json=data,
                timeout=timeout
            )
            
            if response.status_code == 200:
                return {
                    'success': True,
                    'error_msg': '',
                    'status': ModelStatus.ONLINE.value
                }
            else:
                return {
                    'success': False,
                    'error_msg': f'HTTP {response.status_code}: {response.text}',
                    'status': ModelStatus.ERROR.value
                }
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error_msg': f'网络请求失败: {str(e)}',
                'status': ModelStatus.OFFLINE.value
            }
    
    def _test_openai_connection(self, url: str, api_key: str, timeout: int, backend_model: str = '') -> Dict[str, Any]:
        """
        测试OpenAI 或 OpenAI 兼容 API 的连接
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }

        # OpenAI 聊天接口测试 payload
        data = {
            'model': backend_model or 'gpt-3.5-turbo',
            'messages': [{'role': 'user', 'content': self.test_prompt}],
            'max_tokens': 10,
            'temperature': 0.1
        }

        # 规范化 URL：
        # - 若以 /chat/completions 结尾，直接使用
        # - 若以 /v1 结尾，拼接 /chat/completions
        # - 其他情况，拼接 /v1/chat/completions
        if url.endswith('/chat/completions'):
            api_url = url
        elif url.endswith('/v1'):
            api_url = f"{url}/chat/completions"
        else:
            api_url = f"{url}/v1/chat/completions"

        try:
            response = requests.post(api_url, headers=headers, json=data, timeout=timeout)
            if response.status_code == 200:
                return {
                    'success': True,
                    'error_msg': '',
                    'status': ModelStatus.ONLINE.value
                }
            else:
                return {
                    'success': False,
                    'error_msg': f'HTTP {response.status_code}: {response.text}',
                    'status': ModelStatus.ERROR.value
                }
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error_msg': f'网络请求失败: {str(e)}',
                'status': ModelStatus.OFFLINE.value
            }
    
    def _test_sglang_connection(self, url: str, api_key: str, timeout: int) -> Dict[str, Any]:
        """测试SGlang模型连接"""
        headers = {'Content-Type': 'application/json'}
        if api_key:
            headers['Authorization'] = f'Bearer {api_key}'
        
        # SGlang生成接口测试
        data = {
            'text': self.test_prompt,
            'sampling_params': {
                'max_new_tokens': 10,
                'temperature': 0.1
            }
        }
        
        try:
            response = requests.post(
                f"{url}/generate" if not url.endswith('/generate') else url,
                headers=headers,
                json=data,
                timeout=timeout
            )
            
            if response.status_code == 200:
                return {
                    'success': True,
                    'error_msg': '',
                    'status': ModelStatus.ONLINE.value
                }
            else:
                return {
                    'success': False,
                    'error_msg': f'HTTP {response.status_code}: {response.text}',
                    'status': ModelStatus.ERROR.value
                }
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error_msg': f'网络请求失败: {str(e)}',
                'status': ModelStatus.OFFLINE.value
            }
    
    def _test_ollama_connection(self, url: str, timeout: int) -> Dict[str, Any]:
        """测试Ollama模型连接"""
        headers = {'Content-Type': 'application/json'}
        
        # Ollama生成接口测试
        data = {
            'model': 'llama3:8b',  # 默认测试模型
            'prompt': self.test_prompt,
            'stream': False
        }
        
        try:
            api_url = f"{url}/api/generate" if not url.endswith('/generate') else url
            response = requests.post(
                api_url,
                headers=headers,
                json=data,
                timeout=timeout
            )
            
            if response.status_code == 200:
                return {
                    'success': True,
                    'error_msg': '',
                    'status': ModelStatus.ONLINE.value
                }
            else:
                return {
                    'success': False,
                    'error_msg': f'HTTP {response.status_code}: {response.text}',
                    'status': ModelStatus.ERROR.value
                }
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'error_msg': f'网络请求失败: {str(e)}',
                'status': ModelStatus.OFFLINE.value
            }
    
    def _update_model_status(self, model_name: str, test_result: Dict[str, Any]) -> None:
        """更新模型状态"""
        try:
            models = config_manager.get_config(self.config_key, {})
            if model_name in models:
                models[model_name]['status'] = test_result['status']
                models[model_name]['last_test_time'] = datetime.now().isoformat()
                models[model_name]['response_time'] = test_result.get('response_time', 0)
                models[model_name]['error_msg'] = test_result.get('error_msg', '')
                config_manager.update_config(self.config_key, models)
        except Exception as e:
            self.logger.error(f'更新模型状态失败: {e}')
    
    def get_all_models(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有模型配置
        
        Returns:
            dict: 所有模型的配置字典（不包含解密的API密钥）
        """
        try:
            models = config_manager.get_config(self.config_key, {})
            
            # 处理API密钥显示
            result = {}
            for name, config in models.items():
                config_copy = config.copy()
                api_key = config_copy.get('api_key', '')
                if api_key:
                    if api_key.startswith('encrypted:'):
                        config_copy['api_key'] = '***encrypted***'
                    else:
                        config_copy['api_key'] = '***hidden***'
                result[name] = config_copy
            
            return result
            
        except Exception as e:
            self.logger.error(f'获取所有模型失败: {e}')
            return {}
    
    def get_active_models(self) -> List[Dict[str, Any]]:
        """
        获取所有在线可用的模型
        
        Returns:
            list: 在线模型列表
        """
        try:
            all_models = self.get_all_models()
            active_models = []
            
            for name, config in all_models.items():
                if config.get('status') == ModelStatus.ONLINE.value:
                    model_info = {
                        'name': name,
                        'type': config.get('type'),
                        'url': config.get('url'),
                        'response_time': config.get('response_time', 0),
                        'last_test_time': config.get('last_test_time', '')
                    }
                    active_models.append(model_info)
            
            return active_models
            
        except Exception as e:
            self.logger.error(f'获取活跃模型失败: {e}')
            return []
    
    def test_all_models(self) -> Dict[str, Dict[str, Any]]:
        """
        测试所有模型的连接性
        
        Returns:
            dict: 所有模型的测试结果
        """
        results = {}
        models = config_manager.get_config(self.config_key, {})
        
        self.logger.info('开始批量测试所有模型连接')
        
        for model_name in models.keys():
            results[model_name] = self.test_model(model_name)
        
        online_count = sum(1 for r in results.values() if r['success'])
        total_count = len(results)
        
        self.logger.info(f'批量测试完成: {online_count}/{total_count} 个模型在线')
        
        return results
    
    def get_model_statistics(self) -> Dict[str, Any]:
        """
        获取模型统计信息
        
        Returns:
            dict: 模型统计信息
        """
        try:
            models = self.get_all_models()
            stats = {
                'total_models': len(models),
                'online_models': 0,
                'offline_models': 0,
                'error_models': 0,
                'unknown_models': 0,
                'by_type': {},
                'average_response_time': 0
            }
            
            response_times = []
            
            for config in models.values():
                status = config.get('status', ModelStatus.UNKNOWN.value)
                model_type = config.get('type', 'unknown')
                response_time = config.get('response_time', 0)
                
                # 统计状态
                if status == ModelStatus.ONLINE.value:
                    stats['online_models'] += 1
                    response_times.append(response_time)
                elif status == ModelStatus.OFFLINE.value:
                    stats['offline_models'] += 1
                elif status == ModelStatus.ERROR.value:
                    stats['error_models'] += 1
                else:
                    stats['unknown_models'] += 1
                
                # 按类型统计
                if model_type not in stats['by_type']:
                    stats['by_type'][model_type] = {'total': 0, 'online': 0}
                stats['by_type'][model_type]['total'] += 1
                if status == ModelStatus.ONLINE.value:
                    stats['by_type'][model_type]['online'] += 1
            
            # 计算平均响应时间
            if response_times:
                stats['average_response_time'] = round(sum(response_times) / len(response_times), 2)
            
            return stats
            
        except Exception as e:
            self.logger.error(f'获取模型统计失败: {e}')
            return {}

    def generate_text(self, model_name: str, prompt: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        统一的文本生成接口

        Args:
            model_name: 在配置中注册的模型名称（非具体后端模型名）
            prompt: 输入提示词
            params: 生成参数，如 max_tokens / temperature / top_p 等

        Returns:
            dict: { success: bool, content: str, error?: str }
        """
        try:
            config = self.get_model_config(model_name)
            if not config:
                return {"success": False, "error": f"模型不存在: {model_name}"}

            model_type = config.get('type')
            base_url = config.get('url', '').rstrip('/')
            api_key = config.get('api_key', '')
            # 优先使用 params 中的 timeout，其次是 config 中的，最后是默认值
            timeout = params.get('timeout') or config.get('timeout', self.default_timeout)
            backend_model = config.get('model_name') or model_name

            max_tokens = params.get('max_tokens', 2048)
            temperature = params.get('temperature', 0.7)
            top_p = params.get('top_p', 0.9)
            top_k = params.get('top_k', None)

            headers = {'Content-Type': 'application/json'}
            if api_key:
                headers['Authorization'] = f'Bearer {api_key}'

            # 不同后端分支
            if model_type == ModelType.OPENAI.value:
                # 兼容 OpenAI 或 OpenAI 兼容的代理（如 OpenAI-compatible）
                api_url = base_url
                if not api_url.endswith('/chat/completions'):
                    # 若传入的是 v1 根路径，拼接标准路径
                    if api_url.endswith('/v1'):
                        api_url = f"{api_url}/chat/completions"
                    else:
                        api_url = f"{api_url}/v1/chat/completions"
                
                messages = []
                if params.get('system_prompt'):
                    messages.append({'role': 'system', 'content': params['system_prompt']})
                messages.append({'role': 'user', 'content': prompt})

                payload = {
                    'model': backend_model or 'gpt-3.5-turbo',
                    'messages': messages,
                    'max_tokens': max_tokens,
                    'temperature': temperature,
                    'top_p': top_p
                }
                resp = requests.post(api_url, headers=headers, json=payload, timeout=timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    content = ''
                    try:
                        message = data['choices'][0]['message']
                        content = message.get('content') or ''
                        
                        # 兼容 DeepSeek R1 等推理模型的思维链输出 (reasoning_content)
                        reasoning = message.get('reasoning_content') or ''
                        if reasoning:
                            # 将思维链用 <think> 标签包裹，拼接到内容前，保留完整思考过程
                            content = f"<think>\n{reasoning}\n</think>\n\n{content}"
                            
                    except Exception:
                        content = json.dumps(data, ensure_ascii=False)
                    return {"success": True, "content": content}
                else:
                    return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}

            elif model_type == ModelType.VLLM.value:
                # vLLM RESTful /generate 接口
                api_url = base_url if base_url.endswith('/generate') else f"{base_url}/generate"
                payload = {
                    'prompt': prompt,
                    'max_tokens': max_tokens,
                    'temperature': temperature,
                    'top_p': top_p,
                }
                if top_k is not None:
                    try:
                        if int(top_k) > 0:
                            payload['top_k'] = int(top_k)
                    except Exception:
                        pass
                resp = requests.post(api_url, headers=headers, json=payload, timeout=timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    # 常见字段：text / output / outputs[0].text
                    content = data.get('text') or data.get('output')
                    if not content:
                        outputs = data.get('outputs') or []
                        if outputs and isinstance(outputs, list):
                            content = outputs[0].get('text', '')
                    if not content:
                        content = json.dumps(data, ensure_ascii=False)
                    return {"success": True, "content": content}
                else:
                    return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}

            elif model_type == ModelType.SGLANG.value:
                api_url = base_url if base_url.endswith('/generate') else f"{base_url}/generate"
                payload = {
                    'text': prompt,
                    'sampling_params': {
                        'max_new_tokens': max_tokens,
                        'temperature': temperature,
                        'top_p': top_p
                    }
                }
                # 仅当有效时传递 top_k
                try:
                    if top_k is not None and int(top_k) > 0:
                        payload['sampling_params']['top_k'] = int(top_k)
                except Exception:
                    pass
                resp = requests.post(api_url, headers=headers, json=payload, timeout=timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get('text') or data.get('output_text') or json.dumps(data, ensure_ascii=False)
                    return {"success": True, "content": content}
                else:
                    return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}

            elif model_type == ModelType.OLLAMA.value:
                # Ollama /api/generate 接口
                api_url = base_url if base_url.endswith('/api/generate') else f"{base_url}/api/generate"
                payload = {
                    'model': backend_model or 'llama3:8b',
                    'prompt': prompt,
                    'stream': False,
                    'options': {
                        'temperature': temperature,
                        'top_p': top_p
                    }
                }
                try:
                    if top_k is not None and int(top_k) > 0:
                        payload['options']['top_k'] = int(top_k)
                except Exception:
                    pass
                resp = requests.post(api_url, headers=headers, json=payload, timeout=timeout)
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get('response') or data.get('text') or json.dumps(data, ensure_ascii=False)
                    return {"success": True, "content": content}
                else:
                    return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}

            else:
                return {"success": False, "error": f"不支持的模型类型: {model_type}"}

        except requests.exceptions.RequestException as e:
            return {"success": False, "error": f"网络请求失败: {str(e)}"}
        except Exception as e:
            return {"success": False, "error": str(e)}


# 全局模型管理器实例
model_manager = ModelManager()


if __name__ == "__main__":
    """
    命令行入口，用于模型管理操作
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='模型管理工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # add命令
    add_parser = subparsers.add_parser('add', help='添加新模型')
    add_parser.add_argument('--name', required=True, help='模型名称')
    add_parser.add_argument('--type', required=True, choices=['vllm', 'openai', 'sglang', 'ollama'], help='模型类型')
    add_parser.add_argument('--url', required=True, help='模型API地址')
    add_parser.add_argument('--api-key', help='API密钥')
    add_parser.add_argument('--model-name', help='具体模型名称')
    add_parser.add_argument('--timeout', type=int, help='超时时间（秒）')
    
    # test命令
    test_parser = subparsers.add_parser('test', help='测试模型连接')
    test_parser.add_argument('--name', help='模型名称（不指定则测试所有模型）')
    
    # list命令
    list_parser = subparsers.add_parser('list', help='列出所有模型')
    list_parser.add_argument('--active-only', action='store_true', help='只显示在线模型')
    
    # delete命令
    delete_parser = subparsers.add_parser('delete', help='删除模型')
    delete_parser.add_argument('--name', required=True, help='模型名称')
    
    # stats命令
    stats_parser = subparsers.add_parser('stats', help='显示模型统计信息')
    
    args = parser.parse_args()
    
    if args.command == 'add':
        model_info = {
            'name': args.name,
            'type': args.type,
            'url': args.url
        }
        if args.api_key:
            model_info['api_key'] = args.api_key
        if args.model_name:
            model_info['model_name'] = args.model_name
        if args.timeout:
            model_info['timeout'] = args.timeout
        
        if model_manager.add_model(model_info):
            print(f"✓ 添加模型成功: {args.name}")
        else:
            print(f"✗ 添加模型失败: {args.name}")
    
    elif args.command == 'test':
        if args.name:
            result = model_manager.test_model(args.name)
            if result['success']:
                print(f"✓ {args.name}: 在线 (响应时间: {result['response_time']}ms)")
            else:
                print(f"✗ {args.name}: {result['error_msg']}")
        else:
            results = model_manager.test_all_models()
            print("模型连接测试结果:")
            for name, result in results.items():
                status = "✓ 在线" if result['success'] else "✗ 离线"
                response_time = f" ({result['response_time']}ms)" if result['success'] else ""
                print(f"  {name}: {status}{response_time}")
                if not result['success']:
                    print(f"    错误: {result['error_msg']}")
    
    elif args.command == 'list':
        if args.active_only:
            models = model_manager.get_active_models()
            print("在线模型列表:")
            for model in models:
                print(f"  {model['name']} ({model['type']}) - {model['url']}")
        else:
            models = model_manager.get_all_models()
            print("所有模型列表:")
            for name, config in models.items():
                status_icon = {"online": "✓", "offline": "✗", "error": "!", "unknown": "?"}.get(config.get('status'), '?')
                print(f"  {status_icon} {name} ({config.get('type')}) - {config.get('url')}")
    
    elif args.command == 'delete':
        if model_manager.delete_model(args.name):
            print(f"✓ 删除模型成功: {args.name}")
        else:
            print(f"✗ 删除模型失败: {args.name}")
    
    elif args.command == 'stats':
        stats = model_manager.get_model_statistics()
        print("模型统计信息:")
        print(f"  总模型数: {stats['total_models']}")
        print(f"  在线模型: {stats['online_models']}")
        print(f"  离线模型: {stats['offline_models']}")
        print(f"  错误模型: {stats['error_models']}")
        print(f"  未知状态: {stats['unknown_models']}")
        print(f"  平均响应时间: {stats['average_response_time']}ms")
        
        print("\n按类型统计:")
        for model_type, type_stats in stats['by_type'].items():
            print(f"  {model_type}: {type_stats['online']}/{type_stats['total']} 在线")
    
    else:
        parser.print_help()
