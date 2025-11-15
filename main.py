#!/usr/bin/env python3
"""
自动数据蒸馏软件 - 主启动脚本

提供统一的启动入口，支持UI界面和命令行模式。
"""

import os
import sys
import argparse
from pathlib import Path

def setup_environment():
    """设置运行环境"""
    project_root = Path(__file__).parent
    src_dir = project_root / "src"
    
    # 添加src目录到Python路径
    sys.path.insert(0, str(src_dir))
    
    # 切换到项目根目录
    os.chdir(project_root)
    
    return project_root, src_dir

def check_dependencies():
    """检查依赖库"""
    try:
        import gradio
        import pandas
        import requests
        import yaml
        return True
    except ImportError as e:
        print(f"❌ 缺少依赖库: {e}")
        print("请运行: pip install -r requirements.txt")
        return False

def start_ui_mode(share=False, port=7860):
    """启动UI界面模式"""
    try:
        print("🚀 启动Web界面...")
        from src.ui_launcher import UILauncher
        
        launcher = UILauncher()
        launcher.launch(share=share, server_port=port)
        
    except Exception as e:
        print(f"❌ UI启动失败: {e}")
        return 1
    
    return 0

def start_cli_mode():
    """启动命令行模式"""
    print("📱 命令行模式")
    print("使用 'python start_cli.py --help' 查看命令行选项")
    return 0

def show_status():
    """显示系统状态"""
    print("📊 系统状态检查:")
    
    try:
        from src.config_manager import config_manager
        from src.log_manager import log_manager
        from src.state_manager import state_manager
        
        # 检查配置
        config = config_manager.get_config('base.root_dir', './data')
        print(f"  数据目录: {config}")
        
        # 检查任务状态
        tasks = state_manager.list_tasks()
        print(f"  活跃任务: {len(tasks)}个")
        
        # 检查模型
        from src.model_manager import model_manager
        models = model_manager.get_active_models()
        print(f"  可用模型: {len(models)}个")
        
        print("✅ 系统状态正常")
        
    except Exception as e:
        print(f"❌ 状态检查失败: {e}")
        return 1
    
    return 0

def main():
    """主入口函数"""
    parser = argparse.ArgumentParser(
        description="自动数据蒸馏软件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py                    # 启动Web界面
  python main.py --ui --share       # 启动Web界面并创建公共链接
  python main.py --cli              # 启动命令行模式
  python main.py --status           # 查看系统状态
        """
    )
    
    parser.add_argument('--ui', action='store_true', help='启动Web界面（默认）')
    parser.add_argument('--cli', action='store_true', help='启动命令行模式')
    parser.add_argument('--status', action='store_true', help='显示系统状态')
    parser.add_argument('--share', action='store_true', help='创建公共链接（仅UI模式）')
    parser.add_argument('--port', type=int, default=7860, help='Web服务端口（默认7860）')
    
    args = parser.parse_args()
    
    # 设置环境
    setup_environment()
    
    # 检查依赖
    if not check_dependencies():
        return 1
    
    # 根据参数启动相应模式
    if args.status:
        return show_status()
    elif args.cli:
        return start_cli_mode()
    else:
        # 默认启动UI模式
        return start_ui_mode(share=args.share, port=args.port)

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n👋 用户中断，程序退出")
        sys.exit(0)
    except Exception as e:
        print(f"❌ 程序异常退出: {e}")
        sys.exit(1)
