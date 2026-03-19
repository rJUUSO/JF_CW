Import("env")

import os
import shutil
from pathlib import Path

from SCons.Script import COMMAND_LINE_TARGETS


def get_project_option(name, default=""):
    try:
        return env.GetProjectOption(name)
    except Exception:
        return default


def should_stage_fs_config():
    return any(target in ("buildfs", "uploadfs") for target in COMMAND_LINE_TARGETS)


if should_stage_fs_config():
    project_dir = Path(env.subst("$PROJECT_DIR"))
    data_dir = Path(env.subst("$PROJECT_DATA_DIR"))
    selected_name = os.environ.get("COWORKER_FS_CONFIG", get_project_option("custom_coworker_fs_config", "coworker.toml")).strip() or "coworker.toml"
    selected_path = data_dir / selected_name

    if not selected_path.exists():
        raise FileNotFoundError(f"Coworker filesystem config not found: {selected_path}")

    staged_dir = project_dir / ".pio" / "coworker_fs_data"
    if staged_dir.exists():
        shutil.rmtree(staged_dir)
    shutil.copytree(data_dir, staged_dir)

    if selected_name != "coworker.toml":
        shutil.copyfile(selected_path, staged_dir / "coworker.toml")

    env.Replace(PROJECT_DATA_DIR=str(staged_dir))
    print(f"Using {selected_name} as LittleFS /coworker.toml")