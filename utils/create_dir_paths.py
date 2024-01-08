import logging
import os
import hydra

LOGGER = logging.getLogger(__name__)

@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    """Create data subfolders and symbolic links if indicated in config file."""
    
    for datapath, subfolder_dict in cfg.datapaths.items():
        for subfolder_name, subfolder_link in subfolder_dict.items():
            
            subfolder_path = f"data/{datapath}/{subfolder_name}"

            if os.path.exists(subfolder_path):
                LOGGER.info(f"Subfolder {subfolder_path} already exists.")
                continue
            if subfolder_link is not None:
                # Expand the tilde to the user's home directory
                expanded_path = os.path.expanduser(subfolder_link)
                os.symlink(expanded_path, subfolder_path)
                LOGGER.info(f"Created symlink {subfolder_path} -> {subfolder_link}")
            else:
                os.makedirs(subfolder_path, exist_ok=True)
                LOGGER.info(f"Created subfolder {subfolder_path}")

if __name__ == "__main__":
    main()
