#!/usr/bin/env python3
"""
通用字段提取器 - 支持嵌套JSON字段提取

这个模块提供了对复杂嵌套JSON结构的字段提取功能，
支持从大文件中递归提取所有字段，包括深层嵌套的字段。
"""

import json
from .dependencies import pd, ijson, HAS_IJSON
from typing import List, Dict, Any, Set, Union, Optional
import os
from collections import defaultdict


class UniversalFieldExtractor:
    """通用字段提取器，支持嵌套JSON结构"""
    
    def __init__(self):
        self.extracted_fields = set()
        self.field_examples = defaultdict(list)
    
    def _extract_nested_fields(self, data: Any, prefix: str = "") -> Set[str]:
        """递归提取嵌套字段
        
        Args:
            data: 要分析的数据
            prefix: 字段前缀
            
        Returns:
            Set[str]: 所有字段名称的集合
        """
        fields = set()
        
        if isinstance(data, dict):
            for key, value in data.items():
                current_field = f"{prefix}.{key}" if prefix else key
                fields.add(current_field)
                
                # 保存字段示例值
                if len(self.field_examples[current_field]) < 3:
                    self.field_examples[current_field].append(value)
                
                # 递归处理嵌套对象
                if isinstance(value, (dict, list)):
                    nested_fields = self._extract_nested_fields(value, current_field)
                    fields.update(nested_fields)
                    
        elif isinstance(data, list) and data:
            # 处理列表：为第一个元素生成带索引的字段路径
            if isinstance(data[0], dict):
                # 如果是字典列表，分析第一个字典，并生成带[0]索引的路径
                array_prefix = f"{prefix}[0]" if prefix else "[0]"
                nested_fields = self._extract_nested_fields(data[0], array_prefix)
                fields.update(nested_fields)
            elif isinstance(data[0], list):
                # 如果是嵌套列表，继续递归
                array_prefix = f"{prefix}[0]" if prefix else "[0]"
                nested_fields = self._extract_nested_fields(data[0], array_prefix)
                fields.update(nested_fields)
        
        return fields
    
    def _get_nested_value(self, data: Dict, field_path: str) -> Any:
        """根据字段路径获取嵌套值，支持数组索引
        
        Args:
            data: 源数据字典
            field_path: 字段路径，如 "reasoning.teacher" 或 "reasoning[0].full_response"
            
        Returns:
            Any: 字段值，如果不存在返回None
        """
        import re
        
        try:
            current = data
            
            # 分割路径，处理数组索引
            path_parts = []
            remaining_path = field_path
            
            while remaining_path:
                # 匹配字段名和可选的数组索引: key[index] 或 key
                match = re.match(r'^([^.\[]+)(\[(\d+)\])?(\.(.*))?$', remaining_path)
                if match:
                    key = match.group(1)  # 字段名
                    index = match.group(3)  # 数组索引（如果有的话）
                    rest = match.group(5)  # 剩余路径
                    
                    path_parts.append((key, int(index) if index is not None else None))
                    remaining_path = rest if rest else ''
                else:
                    # 处理简单情况：没有数组索引
                    if '.' in remaining_path:
                        key, remaining_path = remaining_path.split('.', 1)
                    else:
                        key = remaining_path
                        remaining_path = ''
                    path_parts.append((key, None))
            
            # 遍历路径获取值
            for key, index in path_parts:
                if current is None:
                    return None
                
                # 获取字段值
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
                
                # 如果有数组索引，访问数组元素
                if index is not None:
                    if isinstance(current, list) and 0 <= index < len(current):
                        current = current[index]
                    else:
                        return None
            
            return current
        except (KeyError, TypeError, AttributeError, IndexError, ValueError):
            return None
    
    def extract_fields_from_file(self, file_path: str, sample_size: int = 100) -> List[Dict[str, Any]]:
        """从文件中提取字段信息
        
        Args:
            file_path: 文件路径
            sample_size: 采样大小
            
        Returns:
            List[Dict]: 字段信息列表
        """
        self.extracted_fields.clear()
        self.field_examples.clear()
        
        try:
            # 检查是否是HuggingFace数据集目录
            if os.path.isdir(file_path):
                hf_result = self._extract_from_huggingface_dataset(file_path, sample_size)
                if hf_result:
                    return hf_result
            
            # 根据文件扩展名确定格式
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.jsonl':
                return self._extract_from_jsonl(file_path, sample_size)
            elif file_ext == '.json':
                return self._extract_from_json(file_path, sample_size)
            elif file_ext == '.csv':
                return self._extract_from_csv(file_path, sample_size)
            elif file_ext in ['.xlsx', '.xls']:
                return self._extract_from_excel(file_path, sample_size)
            elif file_ext == '.arrow':
                return self._extract_from_arrow(file_path, sample_size)
            else:
                raise ValueError(f"不支持的文件格式: {file_ext}")
                
        except Exception as e:
            print(f"字段提取错误: {str(e)}")
            return []
    
    def _extract_from_jsonl(self, file_path: str, sample_size: int) -> List[Dict[str, Any]]:
        """从JSONL文件提取字段"""
        all_fields = set()
        sample_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if sample_count >= sample_size:
                    break
                    
                line = line.strip()
                if line:
                    try:
                        data = json.loads(line)
                        fields = self._extract_nested_fields(data)
                        all_fields.update(fields)
                        sample_count += 1
                    except json.JSONDecodeError:
                        continue
        
        return self._format_field_info(all_fields)
    
    def _extract_from_json(self, file_path: str, sample_size: int) -> List[Dict[str, Any]]:
        """从JSON文件提取字段"""
        all_fields = set()
        
        if HAS_IJSON:
            try:
                with open(file_path, 'rb') as f:
                    # 尝试流式解析数组
                    objects = ijson.items(f, 'item')
                    count = 0
                    for item in objects:
                        if count >= sample_size:
                            break
                        if isinstance(item, dict):
                            fields = self._extract_nested_fields(item)
                            all_fields.update(fields)
                        count += 1
                    
                    if count > 0:
                        return self._format_field_info(all_fields)
            except Exception:
                # 解析失败回退
                pass

        # 回退到普通加载
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            # 如果是数组，分析前N个元素
            for i, item in enumerate(data[:sample_size]):
                if isinstance(item, dict):
                    fields = self._extract_nested_fields(item)
                    all_fields.update(fields)
        elif isinstance(data, dict):
            # 如果是单个对象
            fields = self._extract_nested_fields(data)
            all_fields.update(fields)
        
        return self._format_field_info(all_fields)
    
    def _extract_from_csv(self, file_path: str, sample_size: int) -> List[Dict[str, Any]]:
        """从CSV文件提取字段"""
        df = pd.read_csv(file_path, nrows=sample_size)
        fields = list(df.columns)
        
        field_info = []
        for field in fields:
            field_info.append({
                'name': field,
                'type': str(df[field].dtype),
                'examples': df[field].dropna().head(3).tolist(),
                'nested': False
            })
        
        return field_info
    
    def _extract_from_huggingface_dataset(self, dataset_path: str, sample_size: int) -> Optional[List[Dict[str, Any]]]:
        """从HuggingFace数据集目录提取字段
        
        Args:
            dataset_path: 数据集目录路径
            sample_size: 采样大小
            
        Returns:
            Optional[List[Dict]]: 字段信息列表，如果不是HF数据集返回None
        """
        try:
            # 检查是否是HuggingFace数据集目录结构
            dataset_dir = os.path.join(dataset_path, 'dataset')
            if not os.path.exists(dataset_dir):
                return None
            
            # 查找train目录下的dataset_info.json
            train_dir = os.path.join(dataset_dir, 'train')
            if not os.path.exists(train_dir):
                return None
                
            dataset_info_path = os.path.join(train_dir, 'dataset_info.json')
            if not os.path.exists(dataset_info_path):
                return None
            
            # 读取dataset_info.json获取字段信息
            with open(dataset_info_path, 'r', encoding='utf-8') as f:
                dataset_info = json.load(f)
            
            features = dataset_info.get('features', {})
            if not features:
                return None
            
            # 从features提取字段信息
            all_fields = set()
            self._extract_features_fields(features, "", all_fields)
            
            # 尝试从实际的Arrow文件中获取示例数据
            arrow_files = [f for f in os.listdir(train_dir) if f.endswith('.arrow')]
            if arrow_files:
                try:
                    from .dependencies import pq, pa
                    
                    # 读取第一个Arrow文件的少量数据作为示例
                    arrow_path = os.path.join(train_dir, arrow_files[0])
                    table = pa.ipc.open_file(arrow_path).read_all()
                    
                    # 转换为pandas DataFrame并获取前几行作为示例
                    df = table.to_pandas()
                    if not df.empty:
                        sample_data = df.head(min(sample_size, len(df)))
                        for field in all_fields:
                            try:
                                value = self._get_nested_value(sample_data.iloc[0].to_dict(), field)
                                if value is not None and len(self.field_examples[field]) < 3:
                                    self.field_examples[field].append(value)
                            except:
                                pass
                                
                except ImportError:
                    print("PyArrow未安装，无法读取Arrow文件示例")
                except Exception as e:
                    print(f"读取Arrow文件示例失败: {e}")
            
            return self._format_field_info(all_fields)
            
        except Exception as e:
            print(f"HuggingFace数据集字段提取失败: {e}")
            return None
    
    def _extract_features_fields(self, features: Dict[str, Any], prefix: str, all_fields: Set[str]):
        """递归提取features中的字段
        
        Args:
            features: features字典
            prefix: 字段前缀
            all_fields: 字段集合
        """
        for key, value in features.items():
            current_field = f"{prefix}.{key}" if prefix else key
            all_fields.add(current_field)
            
            if isinstance(value, dict):
                if '_type' in value:
                    # 这是一个类型定义，不需要进一步递归
                    continue
                else:
                    # 递归处理嵌套字段
                    self._extract_features_fields(value, current_field, all_fields)
            elif isinstance(value, list) and value:
                # 处理列表字段，通常表示数组类型
                if isinstance(value[0], dict):
                    array_prefix = f"{current_field}[0]"
                    self._extract_features_fields(value[0], array_prefix, all_fields)
    
    def _extract_from_arrow(self, file_path: str, sample_size: int) -> List[Dict[str, Any]]:
        """从Arrow文件提取字段"""
        try:
            from .dependencies import pa
            
            # 读取Arrow文件
            table = pa.ipc.open_file(file_path).read_all()
            
            # 获取字段名
            field_names = table.schema.names
            
            # 转换为pandas进行示例数据提取
            df = table.to_pandas()
            
            field_info = []
            for field in field_names:
                if field in df.columns:
                    examples = df[field].dropna().head(3).tolist()
                    field_info.append({
                        'name': field,
                        'type': str(df[field].dtype),
                        'examples': examples,
                        'nested': False
                    })
            
            return field_info
            
        except ImportError:
            print("PyArrow未安装，无法读取Arrow文件")
            return []
        except Exception as e:
            print(f"Arrow文件字段提取失败: {e}")
            return []
    
    def _extract_from_excel(self, file_path: str, sample_size: int) -> List[Dict[str, Any]]:
        """从Excel文件提取字段"""
        df = pd.read_excel(file_path, nrows=sample_size)
        fields = list(df.columns)
        
        field_info = []
        for field in fields:
            field_info.append({
                'name': field,
                'type': str(df[field].dtype),
                'examples': df[field].dropna().head(3).tolist(),
                'nested': False
            })
        
        return field_info
    
    def _format_field_info(self, fields: Set[str]) -> List[Dict[str, Any]]:
        """格式化字段信息"""
        field_info = []
        
        for field in sorted(fields):
            examples = self.field_examples.get(field, [])
            field_info.append({
                'name': field,
                'type': self._infer_type(examples),
                'examples': examples[:3],
                'nested': '.' in field
            })
        
        return field_info
    
    def _infer_type(self, examples: List[Any]) -> str:
        """推断字段类型"""
        if not examples:
            return 'unknown'
        
        first_example = examples[0]
        if isinstance(first_example, str):
            return 'string'
        elif isinstance(first_example, int):
            return 'integer'
        elif isinstance(first_example, float):
            return 'float'
        elif isinstance(first_example, bool):
            return 'boolean'
        elif isinstance(first_example, list):
            return 'array'
        elif isinstance(first_example, dict):
            return 'object'
        else:
            return 'mixed'


# 全局提取器实例
_extractor = UniversalFieldExtractor()


def extract_fields_universal(source_path: str, fields: List[str], output_dir: str = None, 
                           field_mapping: Dict[str, str] = None, progress_callback=None) -> str:
    """通用字段提取函数（完整版）
    
    Args:
        source_path: 源文件路径
        fields: 要提取的字段列表
        output_dir: 输出目录
        field_mapping: 字段重命名映射
        progress_callback: 进度回调函数
        
    Returns:
        str: 输出文件路径
    """
    import tempfile
    from datetime import datetime
    
    try:
        if progress_callback:
            progress_callback("🔄 开始字段提取...", 15)
        
        # 创建输出目录
        if not output_dir:
            # 避免硬编码绝对路径，保持相对 root_dir 的默认结构，由调用方传入更佳
            # 这里使用相对路径，交由上层以 config 中的 root_dir 传入覆盖
            output_dir = os.path.join('.', 'data', 'processed')
        
        # 创建提取特定的子目录
        timestamp = int(datetime.now().timestamp())
        # 规范化分隔符，保证跨平台
        extract_dir = os.path.join(output_dir, f"extract-{timestamp}-{os.urandom(3).hex()}")
        os.makedirs(extract_dir, exist_ok=True)
        
        if progress_callback:
            progress_callback("📁 创建输出目录...", 25)
        
        # 生成输出文件名
        base_name = os.path.splitext(os.path.basename(source_path))[0]
        output_filename = f"{base_name}_extracted.jsonl"
        output_path = os.path.join(extract_dir, output_filename)
        
        if progress_callback:
            progress_callback("📖 开始读取源文件...", 35)
        
        # 根据文件格式进行提取
        file_ext = os.path.splitext(source_path)[1].lower()
        
        if file_ext == '.jsonl':
            success = _extract_jsonl_fields_with_mapping(
                source_path, fields, output_path, field_mapping, progress_callback
            )
        elif file_ext == '.json':
            success = _extract_json_fields_with_mapping(
                source_path, fields, output_path, field_mapping, progress_callback
            )
        else:
            raise ValueError(f"不支持的文件格式: {file_ext}")
        
        if success:
            # 清理文件末尾的多余空行
            _clean_extract_file_ending(output_path, file_ext)
            
            if progress_callback:
                progress_callback("✅ 字段提取完成！", 100)
            return output_path
        else:
            raise Exception("字段提取失败")
            
    except Exception as e:
        if progress_callback:
            progress_callback(f"❌ 提取失败: {str(e)}", 100)
        raise e


def get_field_names_universal(file_path: str, sample_size: int = 100) -> List[str]:
    """获取字段名称列表
    
    Args:
        file_path: 文件路径
        sample_size: 采样大小
        
    Returns:
        List[str]: 字段名称列表
    """
    field_info = get_fields_universal(file_path, sample_size)
    return [field['name'] for field in field_info]


def get_fields_universal(file_path: str, sample_size: int = 100) -> List[Dict[str, Any]]:
    """获取字段信息列表（原extract_fields_universal的功能）
    
    Args:
        file_path: 文件路径
        sample_size: 采样大小
        
    Returns:
        List[Dict]: 字段信息列表
    """
    return _extractor.extract_fields_from_file(file_path, sample_size)


def _extract_jsonl_fields_with_mapping(source_path: str, fields: List[str], output_path: str, 
                                     field_mapping: Dict[str, str] = None, progress_callback=None) -> bool:
    """从JSONL文件提取指定字段（带映射和进度）"""
    try:
        total_lines = _count_lines(source_path)
        processed_lines = 0
        
        with open(source_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8', newline='\n') as outfile:
            
            for line in infile:
                line = line.strip()
                if line:
                    try:
                        data = json.loads(line)
                        extracted = {}
                        
                        for field in fields:
                            value = _extractor._get_nested_value(data, field)
                            if value is not None:
                                # 应用字段映射
                                output_field = field_mapping.get(field, field) if field_mapping else field
                                extracted[output_field] = value
                        
                        if extracted:
                            # 使用原生JSON写入，确保统一的行终止符
                            json_line = json.dumps(extracted, ensure_ascii=False)
                            outfile.write(json_line + '\n')
                            
                    except json.JSONDecodeError:
                        continue
                
                processed_lines += 1
                if progress_callback and processed_lines % 1000 == 0:
                    progress = 35 + int((processed_lines / total_lines) * 60)
                    progress_callback(f"📝 处理中... {processed_lines:,}/{total_lines:,}", progress)
        
        return True
        
    except Exception as e:
        print(f"JSONL字段提取失败: {str(e)}")
        return False


def _extract_json_fields_with_mapping(source_path: str, fields: List[str], output_path: str, 
                                    field_mapping: Dict[str, str] = None, progress_callback=None) -> bool:
    """从JSON文件提取指定字段（带映射和进度）- 使用流式处理避免OOM"""
    try:
        if progress_callback:
            progress_callback("📖 准备读取JSON文件...", 45)
        
        # 使用 ijson 进行流式解析，避免一次性加载大文件
        try:
            import ijson
        except ImportError:
            print("缺少 ijson 库，尝试使用普通加载方式")
            # 回退到普通加载，但仍需注意内存
            return _extract_json_fields_fallback(source_path, fields, output_path, field_mapping, progress_callback)

        file_size = os.path.getsize(source_path)
        processed_count = 0
        
        with open(source_path, 'rb') as infile, \
             open(output_path, 'w', encoding='utf-8', newline='\n') as outfile:
            
            # 尝试检测JSON结构
            # 如果是列表，使用 items='item'
            # 如果是单个对象，可能需要不同的处理，但通常数据集是列表
            
            # 简单的启发式检查：读取第一个非空字符
            pos = infile.tell()
            first_char = infile.read(1)
            while first_char and first_char.isspace():
                first_char = infile.read(1)
            infile.seek(pos)
            
            is_list = first_char == b'['
            
            if is_list:
                parser = ijson.items(infile, 'item')
            else:
                # 如果是单个大对象，假设我们想提取顶层字段，或者它不是标准数据集格式
                # 这里假设是单个对象，我们只处理一次
                parser = ijson.items(infile, '')
            
            for item in parser:
                if isinstance(item, dict):
                    extracted = {}
                    for field in fields:
                        value = _extractor._get_nested_value(item, field)
                        if value is not None:
                            # 应用字段映射
                            output_field = field_mapping.get(field, field) if field_mapping else field
                            extracted[output_field] = value
                    
                    if extracted:
                        # 立即写入，不积压在内存中
                        json_line = json.dumps(extracted, ensure_ascii=False)
                        outfile.write(json_line + '\n')
                
                processed_count += 1
                if progress_callback and processed_count % 1000 == 0:
                    # 估算进度（基于文件位置）
                    try:
                        current_pos = infile.tell()
                        progress = 45 + int((current_pos / file_size) * 50)
                        progress_callback(f"📝 处理中... {processed_count:,} 条", progress)
                    except:
                        pass
        
        return True
        
    except Exception as e:
        print(f"JSON字段提取失败: {str(e)}")
        return False

def _extract_json_fields_fallback(source_path: str, fields: List[str], output_path: str, 
                                field_mapping: Dict[str, str] = None, progress_callback=None) -> bool:
    """从JSON文件提取指定字段（回退模式：一次性加载）"""
    try:
        if progress_callback:
            progress_callback("📖 读取JSON文件(内存模式)...", 45)
        
        with open(source_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # ...existing code...
        extracted_data = []
        
        if isinstance(data, list):
            total_items = len(data)
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    extracted = {}
                    for field in fields:
                        value = _extractor._get_nested_value(item, field)
                        if value is not None:
                            # 应用字段映射
                            output_field = field_mapping.get(field, field) if field_mapping else field
                            extracted[output_field] = value
                    
                    if extracted:
                        extracted_data.append(extracted)
                
                if progress_callback and i % 1000 == 0:
                    progress = 45 + int((i / total_items) * 50)
                    progress_callback(f"📝 处理中... {i:,}/{total_items:,}", progress)
        
        elif isinstance(data, dict):
            extracted = {}
            for field in fields:
                value = _extractor._get_nested_value(data, field)
                if value is not None:
                    # 应用字段映射
                    output_field = field_mapping.get(field, field) if field_mapping else field
                    extracted[output_field] = value
            
            if extracted:
                extracted_data.append(extracted)
        
        # 保存为JSONL格式
        if progress_callback:
            progress_callback("💾 保存提取结果...", 95)
        
        with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
            for item in extracted_data:
                # 使用原生JSON写入，确保统一的行终止符
                json_line = json.dumps(item, ensure_ascii=False)
                f.write(json_line + '\n')
        
        return True
        
    except Exception as e:
        print(f"JSON字段提取(回退模式)失败: {str(e)}")
        return False


def _count_lines(file_path: str) -> int:
    """快速计算文件行数"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def _clean_extract_file_ending(file_path: str, file_format: str, encoding: str = 'utf-8'):
    """清理提取文件末尾的多余空行
    
    Args:
        file_path: 文件路径
        file_format: 文件格式
        encoding: 文件编码
    """
    try:
        if file_format == '.jsonl':
            # 读取文件内容
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            
            # 移除末尾的多余换行符，但保留最后一行的换行符
            content = content.rstrip('\n') + '\n'
            
            # 重写文件，确保使用统一的行终止符
            with open(file_path, 'w', encoding=encoding, newline='\n') as f:
                f.write(content)
                
    except Exception as e:
        print(f"清理提取文件末尾失败: {str(e)}")


if __name__ == "__main__":
    # 测试代码
    import tempfile
    
    # 创建测试数据
    test_data = [
        {
            "id": 1,
            "name": "test1",
            "reasoning": {
                "teacher": "AI Assistant",
                "Cognitive_Difficulty": {
                    "level": "medium",
                    "score": 0.6
                }
            }
        },
        {
            "id": 2,
            "name": "test2", 
            "reasoning": {
                "teacher": "Human Expert",
                "Cognitive_Difficulty": {
                    "level": "hard",
                    "score": 0.9
                }
            }
        }
    ]
    
    # 创建临时JSONL文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for item in test_data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
        temp_file = f.name
    
    # 测试字段提取
    print("测试字段提取...")
    fields = get_fields_universal(temp_file)
    print(f"提取到 {len(fields)} 个字段:")
    for field in fields:
        print(f"  - {field['name']} ({field['type']}): {field['examples']}")
    
    # 清理临时文件
    os.unlink(temp_file)
    print("测试完成!")
