import sys
import importlib
from typing import Any, Tuple, Optional

def safe_import(module_name: str) -> Tuple[Optional[Any], bool]:
    """
    Safely import a module and return it along with a success flag.
    
    Args:
        module_name: The name of the module to import.
        
    Returns:
        Tuple[Optional[Any], bool]: (module, True) if successful, (None, False) otherwise.
    """
    try:
        module = importlib.import_module(module_name)
        return module, True
    except ImportError:
        return None, False

# Core dependencies
pd, HAS_PANDAS = safe_import('pandas')
jsonlines, HAS_JSONLINES = safe_import('jsonlines')
requests, HAS_REQUESTS = safe_import('requests')
yaml, HAS_YAML = safe_import('yaml')

# Optional dependencies
ijson, HAS_IJSON = safe_import('ijson')
openpyxl, HAS_OPENPYXL = safe_import('openpyxl')
chardet, HAS_CHARDET = safe_import('chardet')
psutil, HAS_PSUTIL = safe_import('psutil')
datasets, HAS_DATASETS = safe_import('datasets')
modelscope, HAS_MODELSCOPE = safe_import('modelscope')
pq, HAS_PARQUET = safe_import('pyarrow.parquet')
pa, HAS_PYARROW = safe_import('pyarrow')
ET, HAS_XML = safe_import('xml.etree.ElementTree')

# Addict is used by modelscope
addict, HAS_ADDICT = safe_import('addict')

# Cryptography
cryptography, HAS_CRYPTOGRAPHY = safe_import('cryptography')

# Extended HuggingFace support
load_dataset = None
hf_hub_download = None
list_datasets = None
HAS_HF = False

if HAS_DATASETS:
    try:
        from datasets import load_dataset
        HAS_HF = True
    except ImportError:
        pass

try:
    from huggingface_hub import hf_hub_download, list_datasets
except ImportError:
    try:
        from huggingface_hub import hf_hub_download
    except ImportError:
        pass

# Extended ModelScope support
MsDataset = None
model_file_download = None
HubApi = None

if HAS_MODELSCOPE:
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            from modelscope.msdatasets import MsDataset
        except Exception:
            try:
                from modelscope import MsDataset
            except Exception:
                pass
        
        try:
            from modelscope.hub.file_download import model_file_download
        except Exception:
            pass
            
        try:
            from modelscope.hub.api import HubApi
        except Exception:
            pass

# Requests adapters
HTTPAdapter = None
Retry = None
if HAS_REQUESTS:
    try:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
    except ImportError:
        pass

# TQDM
tqdm = None
try:
    from tqdm import tqdm
except ImportError:
    pass
