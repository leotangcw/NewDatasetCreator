#!/usr/bin/env python3
"""
状态管理模块

本模块负责记录和管理所有任务的状态和进度信息。
功能特点：
- 任务状态持久化（写入state.json）
- 实时状态更新和进度跟踪
- 支持断点续传的状态恢复
- 任务生命周期管理（创建/运行/暂停/完成/失败）
- 状态查询和过滤功能

设计原则：
- 线程安全的状态操作
- 可靠的状态持久化
- 详细的任务进度记录
- 支持任务状态的批量操作

作者：自动数据蒸馏软件团队
版本：v1.0
许可：商业软件
"""

import json
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from enum import Enum
import uuid

# 模块导出列表
__all__ = ['TaskStatus', 'TaskType', 'StateManager', 'state_manager']
import copy


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"      # 等待中
    RUNNING = "running"      # 运行中
    PAUSED = "paused"       # 已暂停
    COMPLETED = "completed"  # 已完成
    FAILED = "failed"       # 失败
    CANCELLED = "cancelled"  # 已取消


class TaskType(Enum):
    """任务类型枚举"""
    DOWNLOAD = "download"           # 数据下载
    CONVERT = "convert"            # 格式转换
    EXTRACT = "extract"            # 字段提取
    MERGE = "merge"               # 数据合并
    CLEAN = "clean"               # 数据清洗
    DISTILL = "distill"           # 蒸馏生成
    BACKUP = "backup"             # 数据备份
    RESTORE = "restore"           # 数据恢复


class StateManager:
    """
    状态管理器类
    
    提供任务状态的创建、更新、查询和持久化功能。
    支持多线程安全的状态操作和自动状态恢复。
    
    使用方法：
        state_manager = StateManager()
        task_id = state_manager.add_task("download", "huggingface", {"dataset_name": "squad"})
        state_manager.update_state(task_id, "progress", 50.0)
        state_manager.update_state(task_id, "status", TaskStatus.COMPLETED)
    """
    
    def __init__(self, state_file: str = None, auto_save: bool = True, save_interval: int = 30):
        """
        初始化状态管理器
        
        Args:
            state_file (str, optional): 状态文件路径，默认为./data/states/default.json
            auto_save (bool): 是否自动保存状态，默认True
            save_interval (int): 自动保存间隔（秒），默认30秒
        """
        if state_file is None:
            # 默认状态文件路径
            project_root = Path(__file__).parent.parent
            state_dir = project_root / "data" / "states"
            state_dir.mkdir(parents=True, exist_ok=True)
            state_file = state_dir / "default.json"
        
        self.state_file = Path(state_file)
        self.auto_save = auto_save
        self.save_interval = save_interval
        
        # 状态数据结构
        self.state_data = {
            "tasks": {},           # 任务状态字典
            "last_updated": None,  # 最后更新时间
            "version": "1.0"       # 状态文件版本
        }
        
        # 线程安全锁
        self._lock = threading.RLock()
        self._auto_save_thread = None
        self._stop_auto_save = threading.Event()
        
        # 加载现有状态
        self.load_state()
        
        # 启动自动保存线程
        if self.auto_save:
            self.start_auto_save()
    
    def init_state(self):
        """
        初始化状态文件（首次运行时创建state.json）
        
        如果状态文件不存在，则创建默认的状态文件。
        """
        if not self.state_file.exists():
            self.state_data = {
                "tasks": {},
                "last_updated": datetime.now().isoformat(),
                "version": "1.0"
            }
            self.save_state()
            print(f"状态文件已初始化: {self.state_file}")
        else:
            print(f"状态文件已存在: {self.state_file}")
    
    def add_task(self, task_type: Union[str, TaskType], task_subtype: str, 
                params: Dict[str, Any], task_id: str = None) -> str:
        """
        新增任务状态记录
        
        Args:
            task_type (str or TaskType): 任务类型（如"download"/"distill"）
            task_subtype (str): 任务子类型（如"huggingface"/"openai"）
            params (dict): 任务参数
            task_id (str, optional): 自定义任务ID，为None时自动生成
            
        Returns:
            str: 任务ID
            
        Examples:
            >>> task_id = state_manager.add_task("download", "huggingface", 
            ...     {"dataset_name": "squad", "save_dir": "./data/raw"})
            >>> print(task_id)
            "download-20241001120000-abc123"
        """
        with self._lock:
            # 生成任务ID
            if task_id is None:
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                random_suffix = str(uuid.uuid4())[:6]
                task_type_str = task_type.value if isinstance(task_type, TaskType) else task_type
                task_id = f"{task_type_str}-{timestamp}-{random_suffix}"
            
            # 创建任务状态记录
            task_state = {
                "task_id": task_id,
                "task_type": task_type.value if isinstance(task_type, TaskType) else task_type,
                "task_subtype": task_subtype,
                "status": TaskStatus.PENDING.value,
                "progress": 0.0,
                "params": copy.deepcopy(params),
                "created_time": datetime.now().isoformat(),
                "start_time": None,
                "end_time": None,
                "last_updated": datetime.now().isoformat(),
                "error_message": None,
                "result": None,
                "statistics": {
                    "total_items": 0,
                    "processed_items": 0,
                    "failed_items": 0,
                    "retry_count": 0
                },
                "metadata": {}
            }
            
            # 保存任务状态
            self.state_data["tasks"][task_id] = task_state
            self.state_data["last_updated"] = datetime.now().isoformat()
            
            return task_id
    
    def update_state(self, task_id: str, key: str, value: Any) -> bool:
        """
        更新任务指定状态字段（如进度）
        
        Args:
            task_id (str): 任务ID
            key (str): 状态字段名（如"progress"/"status"/"error_message"）
            value (Any): 状态值
            
        Returns:
            bool: 更新成功与否
            
        Examples:
            >>> state_manager.update_state("task_001", "progress", 75.5)
            True
            >>> state_manager.update_state("task_001", "status", TaskStatus.RUNNING)
            True
        """
        with self._lock:
            if task_id not in self.state_data["tasks"]:
                return False
            
            task_state = self.state_data["tasks"][task_id]
            
            # 更新状态字段
            if key == "status":
                # 状态更新时处理特殊逻辑
                if isinstance(value, TaskStatus):
                    value = value.value
                
                old_status = task_state.get("status")
                task_state["status"] = value
                
                # 状态变化时的时间记录
                if value == TaskStatus.RUNNING.value and old_status != TaskStatus.RUNNING.value:
                    task_state["start_time"] = datetime.now().isoformat()
                elif value in [TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, TaskStatus.CANCELLED.value]:
                    task_state["end_time"] = datetime.now().isoformat()
                    
            elif key == "progress":
                # 进度更新
                task_state["progress"] = float(value)
                
            elif key.startswith("statistics."):
                # 统计信息更新
                stat_key = key.replace("statistics.", "")
                task_state["statistics"][stat_key] = value
                
            elif key.startswith("metadata."):
                # 元数据更新
                meta_key = key.replace("metadata.", "")
                task_state["metadata"][meta_key] = value
                
            else:
                # 其他字段直接更新
                task_state[key] = value
            
            # 更新最后修改时间
            task_state["last_updated"] = datetime.now().isoformat()
            self.state_data["last_updated"] = datetime.now().isoformat()
            
            return True
    
    def get_task_state(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取指定任务的完整状态
        
        Args:
            task_id (str): 任务ID
            
        Returns:
            dict or None: 任务状态字典，任务不存在时返回None
            
        Examples:
            >>> state = state_manager.get_task_state("task_001")
            >>> print(state["progress"])
            75.5
        """
        with self._lock:
            if task_id in self.state_data["tasks"]:
                return copy.deepcopy(self.state_data["tasks"][task_id])
            return None
    
    def list_tasks(self, status: Union[str, TaskStatus] = None, 
                  task_type: Union[str, TaskType] = None,
                  limit: int = None) -> List[Dict[str, Any]]:
        """
        列出符合条件的所有任务
        
        Args:
            status (str or TaskStatus, optional): 状态过滤（如"running"）
            task_type (str or TaskType, optional): 任务类型过滤（如"download"）
            limit (int, optional): 返回结果数量限制
            
        Returns:
            list: 任务列表，按创建时间倒序排列
            
        Examples:
            >>> running_tasks = state_manager.list_tasks(status=TaskStatus.RUNNING)
            >>> download_tasks = state_manager.list_tasks(task_type="download")
        """
        with self._lock:
            tasks = []
            
            status_filter = status.value if isinstance(status, TaskStatus) else status
            type_filter = task_type.value if isinstance(task_type, TaskType) else task_type
            
            for task_id, task_state in self.state_data["tasks"].items():
                # 状态过滤
                if status_filter and task_state.get("status") != status_filter:
                    continue
                
                # 任务类型过滤
                if type_filter and task_state.get("task_type") != type_filter:
                    continue
                
                tasks.append(copy.deepcopy(task_state))
            
            # 按创建时间倒序排序
            tasks.sort(key=lambda x: x.get("created_time", ""), reverse=True)
            
            # 应用数量限制
            if limit and limit > 0:
                tasks = tasks[:limit]
            
            return tasks
    
    def delete_task(self, task_id: str) -> bool:
        """
        删除已完成/失败的任务状态记录
        
        Args:
            task_id (str): 任务ID
            
        Returns:
            bool: 删除成功与否
            
        Examples:
            >>> success = state_manager.delete_task("task_001")
        """
        with self._lock:
            if task_id not in self.state_data["tasks"]:
                return False
            
            task_state = self.state_data["tasks"][task_id]
            current_status = task_state.get("status")
            
            # 只允许删除已完成、失败或取消的任务
            if current_status in [TaskStatus.COMPLETED.value, TaskStatus.FAILED.value, 
                                TaskStatus.CANCELLED.value]:
                del self.state_data["tasks"][task_id]
                self.state_data["last_updated"] = datetime.now().isoformat()
                return True
            else:
                # 运行中或暂停的任务不允许删除
                return False
    
    def pause_task(self, task_id: str) -> bool:
        """
        暂停任务
        
        Args:
            task_id (str): 任务ID
            
        Returns:
            bool: 操作成功与否
        """
        task_state = self.get_task_state(task_id)
        if not task_state:
            return False
        
        current_status = task_state.get("status")
        if current_status == TaskStatus.RUNNING.value:
            return self.update_state(task_id, "status", TaskStatus.PAUSED)
        
        return False
    
    def resume_task(self, task_id: str) -> bool:
        """
        恢复任务
        
        Args:
            task_id (str): 任务ID
            
        Returns:
            bool: 操作成功与否
        """
        task_state = self.get_task_state(task_id)
        if not task_state:
            return False
        
        current_status = task_state.get("status")
        if current_status == TaskStatus.PAUSED.value:
            return self.update_state(task_id, "status", TaskStatus.RUNNING)
        
        return False
    
    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id (str): 任务ID
            
        Returns:
            bool: 操作成功与否
        """
        task_state = self.get_task_state(task_id)
        if not task_state:
            return False
        
        current_status = task_state.get("status")
        if current_status in [TaskStatus.PENDING.value, TaskStatus.RUNNING.value, TaskStatus.PAUSED.value]:
            return self.update_state(task_id, "status", TaskStatus.CANCELLED)
        
        return False
    
    def save_state(self) -> bool:
        """
        保存状态到文件
        
        Returns:
            bool: 保存成功与否
        """
        try:
            with self._lock:
                # 确保状态文件目录存在
                self.state_file.parent.mkdir(parents=True, exist_ok=True)
                tmp_file = self.state_file.with_suffix('.json.tmp')
                payload = json.dumps(self.state_data, ensure_ascii=False, indent=2)
                # 尝试多次原子替换，缓解跨系统挂载偶发 Invalid argument
                for attempt in range(3):
                    try:
                        with open(tmp_file, 'w', encoding='utf-8') as f:
                            f.write(payload)
                            f.flush()
                            os.fsync(f.fileno())
                        # 使用 replace 保持原子性（Python 3.3+）
                        tmp_file.replace(self.state_file)
                        return True
                    except OSError as oe:
                        time.sleep(0.1 * (attempt + 1))
                        if attempt == 2:
                            raise oe
                
        except Exception as e:
            print(f"保存状态文件失败: {e}")
            return False
    
    def load_state(self) -> bool:
        """
        从文件加载状态
        
        Returns:
            bool: 加载成功与否
        """
        try:
            if not self.state_file.exists():
                # 文件不存在，使用默认状态
                self.init_state()
                return True
            
            with open(self.state_file, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
            
            with self._lock:
                # 验证数据格式
                if "tasks" in loaded_data:
                    self.state_data = loaded_data
                    
                    # 修复可能的状态不一致
                    self._fix_inconsistent_states()
                    
                    print(f"状态文件加载成功: {len(self.state_data['tasks'])} 个任务")
                    return True
                else:
                    print("状态文件格式错误，重新初始化")
                    self.init_state()
                    return False
                    
        except json.JSONDecodeError as e:
            print(f"状态文件JSON格式错误: {e}")
            # 备份损坏的文件
            backup_file = self.state_file.with_suffix('.json.backup')
            if self.state_file.exists():
                self.state_file.rename(backup_file)
                print(f"损坏的状态文件已备份到: {backup_file}")
            
            self.init_state()
            return False

    # ------------------------------------------------------------------
    # 兼容外部模块的通用状态读写接口
    # 用于存取自定义键（例如 download_tasks）
    # ------------------------------------------------------------------
    def get_state(self, key: str, default=None):
        """获取状态字典中的自定义键值（浅拷贝）。"""
        with self._lock:
            return copy.deepcopy(self.state_data.get(key, default))

    def set_state(self, key: str, value):
        """设置状态字典中的自定义键值，并立即保存。"""
        with self._lock:
            self.state_data[key] = copy.deepcopy(value)
            self.state_data["last_updated"] = datetime.now().isoformat()
        # 立即持久化
        self.save_state()
    
    def _fix_inconsistent_states(self):
        """修复不一致的任务状态"""
        current_time = datetime.now()
        
        for task_id, task_state in self.state_data["tasks"].items():
            status = task_state.get("status")
            
            # 修复长时间运行但无更新的任务
            if status == TaskStatus.RUNNING.value:
                last_updated = task_state.get("last_updated")
                if last_updated:
                    try:
                        last_update_time = datetime.fromisoformat(last_updated)
                        # 如果超过1小时没有更新，标记为失败
                        if current_time - last_update_time > timedelta(hours=1):
                            task_state["status"] = TaskStatus.FAILED.value
                            task_state["error_message"] = "任务超时，自动标记为失败"
                            task_state["end_time"] = current_time.isoformat()
                            print(f"任务 {task_id} 超时，已标记为失败")
                    except ValueError:
                        pass
    
    def start_auto_save(self):
        """启动自动保存线程"""
        if self._auto_save_thread is not None:
            return
        
        def auto_save_worker():
            while not self._stop_auto_save.wait(self.save_interval):
                try:
                    self.save_state()
                except Exception as e:
                    print(f"自动保存状态失败: {e}")
        
        self._auto_save_thread = threading.Thread(target=auto_save_worker, daemon=True)
        self._auto_save_thread.start()
    
    def stop_auto_save(self):
        """停止自动保存线程"""
        if self._auto_save_thread is not None:
            self._stop_auto_save.set()
            self._auto_save_thread.join(timeout=5)
            self._auto_save_thread = None
            self._stop_auto_save.clear()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取任务统计信息
        
        Returns:
            dict: 统计信息字典
        """
        with self._lock:
            stats = {
                "total_tasks": len(self.state_data["tasks"]),
                "by_status": {},
                "by_type": {},
                "active_tasks": 0,
                "completed_today": 0
            }
            
            today = datetime.now().date()
            
            for task_state in self.state_data["tasks"].values():
                status = task_state.get("status", "unknown")
                task_type = task_state.get("task_type", "unknown")
                
                # 按状态统计
                stats["by_status"][status] = stats["by_status"].get(status, 0) + 1
                
                # 按类型统计
                stats["by_type"][task_type] = stats["by_type"].get(task_type, 0) + 1
                
                # 活跃任务统计
                if status in [TaskStatus.PENDING.value, TaskStatus.RUNNING.value, TaskStatus.PAUSED.value]:
                    stats["active_tasks"] += 1
                
                # 今日完成任务统计
                if status == TaskStatus.COMPLETED.value:
                    end_time = task_state.get("end_time")
                    if end_time:
                        try:
                            end_date = datetime.fromisoformat(end_time).date()
                            if end_date == today:
                                stats["completed_today"] += 1
                        except ValueError:
                            pass
            
            return stats
    
    def cleanup_old_tasks(self, days_to_keep: int = 30, 
                         keep_failed: bool = True) -> Dict[str, int]:
        """
        清理过期的任务记录
        
        Args:
            days_to_keep (int): 保留的天数
            keep_failed (bool): 是否保留失败的任务
            
        Returns:
            dict: 清理结果统计
        """
        with self._lock:
            cutoff_time = datetime.now() - timedelta(days=days_to_keep)
            tasks_to_remove = []
            
            for task_id, task_state in self.state_data["tasks"].items():
                # 检查任务结束时间
                end_time_str = task_state.get("end_time")
                if not end_time_str:
                    continue
                
                try:
                    end_time = datetime.fromisoformat(end_time_str)
                    if end_time < cutoff_time:
                        # 检查是否要保留失败任务
                        if keep_failed and task_state.get("status") == TaskStatus.FAILED.value:
                            continue
                        
                        tasks_to_remove.append(task_id)
                        
                except ValueError:
                    continue
            
            # 删除过期任务
            removed_count = 0
            for task_id in tasks_to_remove:
                del self.state_data["tasks"][task_id]
                removed_count += 1
            
            if removed_count > 0:
                self.state_data["last_updated"] = datetime.now().isoformat()
                self.save_state()
            
            return {
                "removed_count": removed_count,
                "remaining_count": len(self.state_data["tasks"])
            }
    
    def __del__(self):
        """析构函数，确保保存状态"""
        try:
            self.stop_auto_save()
            self.save_state()
        except Exception:
            pass


# 全局状态管理器实例
state_manager = StateManager()


if __name__ == "__main__":
    """
    命令行入口，用于状态管理操作
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='状态管理工具')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # list命令
    list_parser = subparsers.add_parser('list', help='列出任务')
    list_parser.add_argument('--status', help='按状态过滤')
    list_parser.add_argument('--type', help='按类型过滤')
    list_parser.add_argument('--limit', type=int, help='结果数量限制')
    
    # show命令
    show_parser = subparsers.add_parser('show', help='显示任务详情')
    show_parser.add_argument('task_id', help='任务ID')
    
    # pause命令
    pause_parser = subparsers.add_parser('pause', help='暂停任务')
    pause_parser.add_argument('task_id', help='任务ID')
    
    # resume命令
    resume_parser = subparsers.add_parser('resume', help='恢复任务')
    resume_parser.add_argument('task_id', help='任务ID')
    
    # cancel命令
    cancel_parser = subparsers.add_parser('cancel', help='取消任务')
    cancel_parser.add_argument('task_id', help='任务ID')
    
    # delete命令
    delete_parser = subparsers.add_parser('delete', help='删除任务')
    delete_parser.add_argument('task_id', help='任务ID')
    
    # stats命令
    stats_parser = subparsers.add_parser('stats', help='显示统计信息')
    
    # cleanup命令
    cleanup_parser = subparsers.add_parser('cleanup', help='清理过期任务')
    cleanup_parser.add_argument('--days', type=int, default=30, help='保留天数')
    cleanup_parser.add_argument('--keep-failed', action='store_true', help='保留失败任务')
    
    # test命令
    test_parser = subparsers.add_parser('test', help='测试状态管理功能')
    
    args = parser.parse_args()
    
    if args.command == 'list':
        tasks = state_manager.list_tasks(args.status, args.type, args.limit)
        if tasks:
            print(f"任务列表 (共 {len(tasks)} 个):")
            for task in tasks:
                print(f"  {task['task_id']} - {task['task_type']} - {task['status']} - {task['progress']:.1f}%")
        else:
            print("未找到符合条件的任务")
            
    elif args.command == 'show':
        task_state = state_manager.get_task_state(args.task_id)
        if task_state:
            print(f"任务详情: {args.task_id}")
            for key, value in task_state.items():
                print(f"  {key}: {value}")
        else:
            print(f"任务不存在: {args.task_id}")
            
    elif args.command == 'pause':
        success = state_manager.pause_task(args.task_id)
        print(f"暂停任务: {'成功' if success else '失败'}")
        
    elif args.command == 'resume':
        success = state_manager.resume_task(args.task_id)
        print(f"恢复任务: {'成功' if success else '失败'}")
        
    elif args.command == 'cancel':
        success = state_manager.cancel_task(args.task_id)
        print(f"取消任务: {'成功' if success else '失败'}")
        
    elif args.command == 'delete':
        success = state_manager.delete_task(args.task_id)
        print(f"删除任务: {'成功' if success else '失败'}")
        
    elif args.command == 'stats':
        stats = state_manager.get_statistics()
        print("任务统计信息:")
        print(f"  总任务数: {stats['total_tasks']}")
        print(f"  活跃任务: {stats['active_tasks']}")
        print(f"  今日完成: {stats['completed_today']}")
        print("  按状态分布:")
        for status, count in stats['by_status'].items():
            print(f"    {status}: {count}")
        print("  按类型分布:")
        for task_type, count in stats['by_type'].items():
            print(f"    {task_type}: {count}")
            
    elif args.command == 'cleanup':
        result = state_manager.cleanup_old_tasks(args.days, args.keep_failed)
        print(f"清理完成: 删除 {result['removed_count']} 个任务，剩余 {result['remaining_count']} 个任务")
        
    elif args.command == 'test':
        print("测试状态管理功能...")
        
        # 创建测试任务
        task_id = state_manager.add_task(TaskType.DOWNLOAD, "test", {"test_param": "test_value"})
        print(f"创建测试任务: {task_id}")
        
        # 更新任务状态
        state_manager.update_state(task_id, "status", TaskStatus.RUNNING)
        state_manager.update_state(task_id, "progress", 50.0)
        state_manager.update_state(task_id, "statistics.total_items", 100)
        state_manager.update_state(task_id, "statistics.processed_items", 50)
        
        # 查询任务状态
        task_state = state_manager.get_task_state(task_id)
        print(f"任务状态: {task_state['status']}, 进度: {task_state['progress']}%")
        
        # 完成任务
        state_manager.update_state(task_id, "status", TaskStatus.COMPLETED)
        state_manager.update_state(task_id, "progress", 100.0)
        
        print("状态管理测试完成")
        
    else:
        parser.print_help()
