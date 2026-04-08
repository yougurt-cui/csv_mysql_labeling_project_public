from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.yaml"
DEFAULT_COLMAP_PATH = Path(__file__).resolve().parents[1] / "config" / "column_map.yaml"
DEFAULT_DICT_SEED_PATH = Path(__file__).resolve().parents[1] / "config" / "dictionaries_seed.yaml"

@dataclass
class Settings:
    mysql: Dict[str, Any]
    openai: Dict[str, Any]
    ocr: Dict[str, Any]
    pipeline: Dict[str, Any]
    column_map: Dict[str, str]
    dict_seed: Dict[str, Any]

def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def load_settings(
    config_path: Path = DEFAULT_CONFIG_PATH,
    colmap_path: Path = DEFAULT_COLMAP_PATH,
    dict_seed_path: Path = DEFAULT_DICT_SEED_PATH,
) -> Settings:
    cfg = _load_yaml(config_path)
    colmap = _load_yaml(colmap_path)
    dseed = _load_yaml(dict_seed_path)

    # basic guards
    if not cfg.get("mysql"):
        raise ValueError("config.yaml missing `mysql` section")
    if not cfg.get("openai"):
        raise ValueError("config.yaml missing `openai` section")
    if not cfg.get("ocr"):
        cfg["ocr"] = {}
    if not cfg.get("pipeline"):
        cfg["pipeline"] = {}

    return Settings(
        mysql=cfg["mysql"],
        openai=cfg["openai"],
        ocr=cfg.get("ocr", {}),
        pipeline=cfg.get("pipeline", {}),
        column_map=colmap,
        dict_seed=dseed,
    )
