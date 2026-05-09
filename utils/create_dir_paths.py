import logging
import os
import hydra
import yaml
from omegaconf import DictConfig

LOGGER = logging.getLogger(__name__)
VERSIONS_CONFIG = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "conf", "versions.yaml"))


def create_subfolders_and_links(datapath="data", folder_dict=None):
    """
    Recursively create subfolders and symbolic links.
    """
    if not os.path.exists(datapath):
        LOGGER.info(f"Error: {datapath} does not exists.")
        return
    if isinstance(folder_dict, DictConfig):
        for path, subfolder_dict in folder_dict.items():
            sub_datapath = os.path.join(datapath, path)
            if isinstance(subfolder_dict, str):
                # Check if the folder is a symbolic link
                if os.path.islink(sub_datapath):
                    # Get the target of the symlink
                    link_target = os.readlink(sub_datapath)
                    # Check if the link points to the specified target path
                    if os.path.abspath(link_target) == os.path.abspath(subfolder_dict):
                        LOGGER.info(f"There is a symbolic link to {subfolder_dict} at {sub_datapath} already")
                    else:
                        LOGGER.info(f"Error: {sub_datapath} is a symbolic link to {link_target}, not {subfolder_dict}")
                        return 
                # Create symbolic link
                else:
                    if os.path.exists(sub_datapath):
                        LOGGER.info(f"Error: Path {sub_datapath} already exists, cannot create symlink")
                        return
                    else:
                        os.makedirs(os.path.abspath(subfolder_dict), exist_ok=True)
                        os.symlink(os.path.abspath(subfolder_dict), sub_datapath)
                        LOGGER.info(f"Created symlink {sub_datapath} -> {subfolder_dict}")
            else:
                # Create subfolder
                if os.path.exists(sub_datapath):
                    LOGGER.info(f"Path {sub_datapath} already exists")
                else:
                    os.mkdir(sub_datapath)
                    LOGGER.info(f"Created data path {sub_datapath}")
                if subfolder_dict is not None:
                    # Recursive call for nested subfolders
                    create_subfolders_and_links(sub_datapath, subfolder_dict)


def load_versions_config():
    with open(VERSIONS_CONFIG, "r") as f:
        return yaml.safe_load(f)


def versioned_output_name(filename, output_label, output_labels):
    prefix = "pm25_components__randall__"

    if not filename.startswith(prefix) or not filename.endswith(".parquet"):
        return None

    file_stem = filename.removeprefix(prefix)
    if any(file_stem.startswith(f"{label}_") for label in output_labels):
        return None

    return f"{prefix}{output_label}_{file_stem}"


def version_from_datapath(datapath, configured_version, versions_cfg):
    path_parts = {part.upper() for part in os.path.abspath(datapath).split(os.sep)}
    for version in versions_cfg:
        if version.upper() in path_parts:
            return version

    configured_version = str(configured_version).strip().upper()
    if configured_version not in versions_cfg:
        raise ValueError(f"Unknown version '{configured_version}'. Expected one of {list(versions_cfg.keys())}.")
    return configured_version


def create_versioned_output_links(datapath, output_label, output_labels):
    output_root = os.path.join(datapath, "output")
    if not os.path.exists(output_root):
        LOGGER.info(f"Output path {output_root} does not exist, skipping versioned output symlinks")
        return

    for output_dir in os.listdir(output_root):
        output_path = os.path.join(output_root, output_dir)
        if not os.path.isdir(output_path):
            continue

        for filename in os.listdir(output_path):
            src = os.path.join(output_path, filename)
            if not os.path.isfile(src):
                continue

            versioned_name = versioned_output_name(filename, output_label, output_labels)
            if versioned_name is None:
                continue

            dst = os.path.join(output_path, versioned_name)
            if os.path.exists(dst) or os.path.islink(dst):
                LOGGER.info(f"Versioned output already exists at {dst}")
                continue

            os.symlink(filename, dst)
            LOGGER.info(f"Created versioned output symlink {dst} -> {filename}")


@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    """Create data subfolders and symbolic links as indicated in config file."""
    datapath = cfg.datapaths.base_path
    if datapath is None:
        datapath = "data"
    if not os.path.exists(datapath):
        LOGGER.info(f"Creating base path {datapath}")
        os.makedirs(datapath, exist_ok=True)
    else:
        LOGGER.info(f"Base path {datapath} already exists")
    create_subfolders_and_links(datapath=datapath, folder_dict=cfg.datapaths.dirs)

    versions_cfg = load_versions_config()
    version = version_from_datapath(datapath, cfg.get("version", "V6NA"), versions_cfg)
    output_label = versions_cfg[version].get("output_label", version)
    output_labels = {
        version_cfg.get("output_label", version_name)
        for version_name, version_cfg in versions_cfg.items()
    }
    create_versioned_output_links(datapath=datapath, output_label=output_label, output_labels=output_labels)

if __name__ == "__main__":
    main()
