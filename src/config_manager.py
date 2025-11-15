#!/usr/bin/env python3
"""
配置管理模块

本模块提供全局配置管理功能，统一管理所有模块的配置参数。
功能特点：
- 支持YAML格式配置文件的加载和保存
- 提供默认配置和动态配置更新
- 支持敏感配置的加密存储（如API密钥）
- 分模块配置管理，避免全局混杂
- 配置项嵌套路径访问（如 distill.batch_size）

设计原则：
- 集中式配置管理，确保所有模块使用一致的配置
- 安全的敏感信息处理
- 配置更新的实时生效
- 合理的默认值，确保首次运行无需手动配置

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import os
import yaml
import json
import base64
from pathlib import Path
from typing import Dict, Any, Optional, Union
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import secrets


class ConfigManager:
    """
    配置管理器类
    
    提供全局配置的加载、保存、更新和加密功能。
    支持分模块配置和嵌套路径访问。
    
    使用方法：
        config_manager = ConfigManager()
        value = config_manager.get_config("download.timeout")
        config_manager.update_config("download.timeout", 600)
    """
    
    def __init__(self, config_file: str = None):
        """
        初始化配置管理器
        
        Args:
            config_file (str, optional): 配置文件路径，默认为项目根目录下的config.yaml
        """
        if config_file is None:
            # 默认配置文件路径为项目根目录
            project_root = Path(__file__).parent.parent
            config_file = project_root / "config.yaml"
        
        self.config_file = Path(config_file)
        self.config = {}
        self._encryption_key = None
        
        # 确保配置文件目录存在
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 加载配置
        self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """
        从配置文件加载配置到内存
        
        Returns:
            dict: 配置字典
            
        Raises:
            FileNotFoundError: 配置文件不存在时（会自动创建默认配置）
            yaml.YAMLError: YAML格式错误时
        """
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.config = yaml.safe_load(f) or {}
            else:
                # 如果配置文件不存在，创建默认配置
                self.config = self._get_default_config()
                self.save_config()
                
            return self.config
            
        except yaml.YAMLError as e:
            print(f"配置文件格式错误: {e}")
            # 备份错误的配置文件
            backup_file = self.config_file.with_suffix('.yaml.backup')
            if self.config_file.exists():
                self.config_file.rename(backup_file)
                print(f"错误配置已备份到: {backup_file}")
            
            # 使用默认配置
            self.config = self._get_default_config()
            self.save_config()
            return self.config
            
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            self.config = self._get_default_config()
            return self.config
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        获取指定配置项（支持嵌套路径）
        
        Args:
            key (str): 配置项路径，支持点号分隔的嵌套路径（如"distill.batch_size"）
            default (Any, optional): 默认值，当配置项不存在时返回
            
        Returns:
            Any: 配置值（任意类型）
            
        Examples:
            >>> config_manager.get_config("base.root_dir")
            "./data"
            >>> config_manager.get_config("download.timeout", 300)
            300
        """
        try:
            keys = key.split('.')
            value = self.config
            
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    return default
                    
            return value
            
        except Exception:
            return default
    
    def update_config(self, key: str, value: Any) -> bool:
        """
        更新配置项并写入文件
        
        Args:
            key (str): 配置项路径，支持嵌套路径（如"distill.batch_size"）
            value (Any): 配置值
            
        Returns:
            bool: 更新成功与否
            
        Examples:
            >>> config_manager.update_config("download.timeout", 600)
            True
            >>> config_manager.update_config("distill.batch_size", 20)
            True
        """
        try:
            keys = key.split('.')
            config_ref = self.config
            
            # 导航到目标层级，创建不存在的中间层级
            for k in keys[:-1]:
                if k not in config_ref:
                    config_ref[k] = {}
                elif not isinstance(config_ref[k], dict):
                    # 如果中间路径不是字典，则无法继续嵌套
                    return False
                config_ref = config_ref[k]
            
            # 设置最终值
            config_ref[keys[-1]] = value
            
            # 保存到文件
            return self.save_config()
            
        except Exception as e:
            print(f"更新配置失败: {e}")
            return False
    
    def save_config(self) -> bool:
        """
        保存配置到文件
        
        Returns:
            bool: 保存成功与否
        """
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, 
                         allow_unicode=True, indent=2, sort_keys=False)
            return True
            
        except Exception as e:
            print(f"保存配置文件失败: {e}")
            return False
    
    def encrypt_config(self, key: str) -> Dict[str, Any]:
        """
        加密指定敏感配置（如API密钥）
        
        Args:
            key (str): 敏感配置项路径
            
        Returns:
            dict: 加密后的配置字典，包含加密状态信息
            
        Examples:
            >>> result = config_manager.encrypt_config("download.api_keys.huggingface")
            >>> print(result)
            {'encrypted': True, 'message': '配置项已加密'}
        """
        try:
            # 获取原始值
            original_value = self.get_config(key)
            if original_value is None:
                return {'encrypted': False, 'message': '配置项不存在'}
            
            # 如果已经是加密格式，跳过
            if isinstance(original_value, dict) and original_value.get('_encrypted') is True:
                return {'encrypted': True, 'message': '配置项已经是加密状态'}
            
            # 加密值
            encrypted_value = self._encrypt_value(str(original_value))
            
            # 更新配置为加密格式
            encrypted_config = {
                '_encrypted': True,
                'value': encrypted_value,
                'timestamp': datetime.now().isoformat()
            }
            
            self.update_config(key, encrypted_config)
            
            return {'encrypted': True, 'message': '配置项已加密'}
            
        except Exception as e:
            return {'encrypted': False, 'message': f'加密失败: {str(e)}'}
    
    def decrypt_config(self, key: str) -> Union[str, None]:
        """
        解密指定敏感配置
        
        Args:
            key (str): 加密配置项路径
            
        Returns:
            str or None: 解密后的原始值，失败时返回None
            
        Examples:
            >>> original_value = config_manager.decrypt_config("download.api_keys.huggingface")
            >>> print(original_value)
            "hf_your_actual_token"
        """
        try:
            encrypted_config = self.get_config(key)
            
            if not isinstance(encrypted_config, dict):
                # 如果不是字典，可能是未加密的值
                return str(encrypted_config) if encrypted_config is not None else None
            
            if not encrypted_config.get('_encrypted'):
                # 如果未标记为加密，直接返回值
                return str(encrypted_config)
            
            # 解密值
            encrypted_value = encrypted_config.get('value')
            if encrypted_value:
                return self._decrypt_value(encrypted_value)
            else:
                return None
                
        except Exception as e:
            print(f"解密配置失败: {e}")
            return None
    
    def _get_default_config(self) -> Dict[str, Any]:
        """
        获取默认配置
        
        Returns:
            dict: 默认配置字典
        """
        return {
            # 基础配置（所有模块共用）
            "base": {
                "root_dir": "./data",
                "encoding": "utf-8",
                "max_workers": 4,
                "chunk_size": 1000
            },
            
            # 下载模块配置
            "download": {
                "timeout": 300,
                "max_retries": 3,
                "retry_delay": 2,
                "encrypt_api_key": True,
                "api_keys": {
                    "huggingface": "",
                    "modelscope": ""
                }
            },
            
            # 加工模块配置（格式转换/字段提取/合并/清洗）
            "process": {
                "excel_engine": "openpyxl",
                "csv_delimiter": ",",
                "json_indent": 2,
                "dedup_threshold": 0.95,
                "max_file_size": 1073741824  # 1GB
            },
            
            # 模型管理模块配置
            "model": {
                "default_timeout": 600,
                "supported_types": ["vllm", "openai", "sglang", "ollama"],
                "max_batch_size": 100,
                "api_keys": {}
            },
            
            # 蒸馏生成模块配置
            "distill": {
                "batch_size": 10,
                "save_interval": 100,
                "max_prompt_length": 2048,
                "temperature": 0.7,
                "max_tokens": 512
            },
            
            # 日志模块配置
            "log": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "max_file_size": 10485760,  # 10MB
                "backup_count": 5,
                "console_output": True
            },
            
            # 状态管理配置
            "state": {
                "auto_save": True,
                "save_interval": 30,
                "max_history": 1000
            }
        }
    
    def _get_encryption_key(self) -> bytes:
        """
        获取或生成加密密钥
        
        Returns:
            bytes: 加密密钥
        """
        if self._encryption_key is not None:
            return self._encryption_key
        
        key_file = self.config_file.parent / ".encryption_key"
        
        if key_file.exists():
            # 从文件读取密钥
            try:
                with open(key_file, 'rb') as f:
                    self._encryption_key = f.read()
                return self._encryption_key
            except Exception:
                # 如果读取失败，重新生成
                pass
        
        # 生成新密钥
        password = secrets.token_bytes(32)
        salt = secrets.token_bytes(16)
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        
        key = base64.urlsafe_b64encode(kdf.derive(password))
        
        # 保存密钥到文件
        try:
            with open(key_file, 'wb') as f:
                f.write(key)
            # 设置文件权限（仅所有者可读写）
            os.chmod(key_file, 0o600)
        except Exception as e:
            print(f"保存加密密钥失败: {e}")
        
        self._encryption_key = key
        return key
    
    def _encrypt_value(self, value: str) -> str:
        """
        加密字符串值
        
        Args:
            value (str): 要加密的字符串
            
        Returns:
            str: Base64编码的加密值
        """
        key = self._get_encryption_key()
        f = Fernet(key)
        encrypted_bytes = f.encrypt(value.encode('utf-8'))
        return base64.b64encode(encrypted_bytes).decode('utf-8')
    
    def _decrypt_value(self, encrypted_value: str) -> str:
        """
        解密字符串值
        
        Args:
            encrypted_value (str): Base64编码的加密值
            
        Returns:
            str: 解密后的原始字符串
        """
        key = self._get_encryption_key()
        f = Fernet(key)
        encrypted_bytes = base64.b64decode(encrypted_value.encode('utf-8'))
        decrypted_bytes = f.decrypt(encrypted_bytes)
        return decrypted_bytes.decode('utf-8')
    
    def get_all_config(self) -> Dict[str, Any]:
        """
        获取完整配置字典
        
        Returns:
            dict: 完整配置字典的副本
        """
        return self.config.copy()
    
    def reset_config(self, section: str = None) -> bool:
        """
        重置配置为默认值
        
        Args:
            section (str, optional): 要重置的配置节，为None时重置全部配置
            
        Returns:
            bool: 重置成功与否
        """
        try:
            default_config = self._get_default_config()
            
            if section is None:
                # 重置全部配置
                self.config = default_config
            else:
                # 重置指定节
                if section in default_config:
                    self.config[section] = default_config[section]
                else:
                    return False
            
            return self.save_config()
            
        except Exception as e:
            print(f"重置配置失败: {e}")
            return False
    
    def validate_config(self) -> Dict[str, Any]:
        """
        验证配置完整性和合法性
        
        Returns:
            dict: 验证结果，包含验证状态和错误信息
        """
        result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        try:
            # 验证基础配置
            base_config = self.get_config('base', {})
            if not base_config.get('root_dir'):
                result['errors'].append('base.root_dir 不能为空')
                result['valid'] = False
            
            # 验证日志配置
            log_level = self.get_config('log.level', 'INFO')
            if log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
                result['errors'].append(f'log.level 值无效: {log_level}')
                result['valid'] = False
            
            # 验证数值配置
            numeric_configs = [
                ('download.timeout', 1, 3600),
                ('download.max_retries', 1, 10),
                ('process.dedup_threshold', 0.0, 1.0),
                ('distill.batch_size', 1, 1000),
                ('log.max_file_size', 1024, 1073741824),  # 1KB - 1GB
            ]
            
            for config_key, min_val, max_val in numeric_configs:
                value = self.get_config(config_key)
                if value is not None:
                    try:
                        num_value = float(value)
                        if not (min_val <= num_value <= max_val):
                            result['warnings'].append(
                                f'{config_key} 值 {value} 超出推荐范围 [{min_val}, {max_val}]'
                            )
                    except (ValueError, TypeError):
                        result['errors'].append(f'{config_key} 必须是数值类型')
                        result['valid'] = False
            
            return result
            
        except Exception as e:
            result['valid'] = False
            result['errors'].append(f'配置验证过程发生错误: {str(e)}')
            return result


# 全局配置管理器实例
config_manager = ConfigManager()


if __name__ == "__main__":
    """
    命令行入口，用于配置管理操作
    """
    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser(description='配置管理工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # get命令
    get_parser = subparsers.add_parser('get', help='获取配置值')
    get_parser.add_argument('key', help='配置项路径（如 download.timeout）')
    get_parser.add_argument('--default', help='默认值', default=None)
    
    # set命令
    set_parser = subparsers.add_parser('set', help='设置配置值')
    set_parser.add_argument('key', help='配置项路径')
    set_parser.add_argument('value', help='配置值')
    set_parser.add_argument('--type', choices=['str', 'int', 'float', 'bool'], 
                           default='str', help='值类型')
    
    # encrypt命令
    encrypt_parser = subparsers.add_parser('encrypt', help='加密敏感配置')
    encrypt_parser.add_argument('key', help='要加密的配置项路径')
    
    # decrypt命令
    decrypt_parser = subparsers.add_parser('decrypt', help='解密敏感配置')
    decrypt_parser.add_argument('key', help='要解密的配置项路径')
    
    # validate命令
    validate_parser = subparsers.add_parser('validate', help='验证配置')
    
    # reset命令
    reset_parser = subparsers.add_parser('reset', help='重置配置')
    reset_parser.add_argument('--section', help='要重置的配置节，为空时重置全部')
    
    # show命令
    show_parser = subparsers.add_parser('show', help='显示配置')
    show_parser.add_argument('--section', help='只显示指定配置节')
    
    args = parser.parse_args()
    
    if args.command == 'get':
        value = config_manager.get_config(args.key, args.default)
        print(f"{args.key} = {value}")
        
    elif args.command == 'set':
        # 类型转换
        value = args.value
        if args.type == 'int':
            value = int(value)
        elif args.type == 'float':
            value = float(value)
        elif args.type == 'bool':
            value = value.lower() in ('true', '1', 'yes', 'on')
        
        success = config_manager.update_config(args.key, value)
        if success:
            print(f"配置更新成功: {args.key} = {value}")
        else:
            print(f"配置更新失败: {args.key}")
            
    elif args.command == 'encrypt':
        result = config_manager.encrypt_config(args.key)
        print(result['message'])
        
    elif args.command == 'decrypt':
        value = config_manager.decrypt_config(args.key)
        if value is not None:
            print(f"{args.key} = {value}")
        else:
            print(f"解密失败或配置项不存在: {args.key}")
            
    elif args.command == 'validate':
        result = config_manager.validate_config()
        if result['valid']:
            print("配置验证通过")
        else:
            print("配置验证失败")
            
        if result['errors']:
            print("\n错误:")
            for error in result['errors']:
                print(f"  - {error}")
                
        if result['warnings']:
            print("\n警告:")
            for warning in result['warnings']:
                print(f"  - {warning}")
                
    elif args.command == 'reset':
        success = config_manager.reset_config(args.section)
        if success:
            print(f"配置重置成功: {args.section if args.section else '全部配置'}")
        else:
            print("配置重置失败")
            
    elif args.command == 'show':
        if args.section:
            config = config_manager.get_config(args.section, {})
            print(f"[{args.section}]")
            print(yaml.dump(config, default_flow_style=False, allow_unicode=True, indent=2))
        else:
            config = config_manager.get_all_config()
            print(yaml.dump(config, default_flow_style=False, allow_unicode=True, indent=2))
            
    else:
        parser.print_help()