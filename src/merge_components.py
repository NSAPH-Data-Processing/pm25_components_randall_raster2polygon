import pandas as pd
import hydra
import logging  
import pathlib
import os

from hydra.core.hydra_config import HydraConfig


# configure logger to print at info level
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="../conf", config_name="config", version_base=None)
def main(cfg):
    # get aggregation defaults
    LOGGER.info(f"Running merge for: {cfg.temporal_freq} {cfg.polygon_name} {cfg.year}")
    logging_dir = HydraConfig.get().runtime.output_dir

    components = cfg.satellite_component.component.keys()
    LOGGER.info(f"Components to merge: {list(components)}")

    # Load all component files and merge them
    component_dfs = []
    for component in components:
        component_file = f"data/intermediate/pm25_components__randall/{cfg.temporal_freq}/{component}/{component}__{cfg.polygon_name}_{cfg.temporal_freq}_{cfg.year}.parquet"
        
        if not os.path.exists(component_file):
            LOGGER.error(f"Component file not found: {component_file}")
            return
            
        LOGGER.info(f"Loading component file: {component_file}")
        df = pd.read_parquet(component_file)
        component_dfs.append(df)

    # Merge all components into wide format
    base_df = component_dfs[0]
    
    # merge on geo id, year, month
    merge_columns = [col for col in base_df.columns if col not in components]
    
    for i, df in enumerate(component_dfs[1:], 1):
        component_name = list(components)[i]
        base_df = base_df.merge(df, on=merge_columns, how='outer')

    # Reorder columns: spatial resolution, year, (month if monthly), then components
    column_order = [cfg.polygon_name, "year"]
    
    if cfg.temporal_freq == "monthly":
        column_order.append("month")
        
    # Add components in alphabetical order
    component_order = sorted(list(components))
    column_order.extend(component_order)
    
    final_df = base_df[column_order]

    LOGGER.info(f"Final dataset shape: {final_df.shape}")
    LOGGER.info(f"Columns: {list(final_df.columns)}")

    # == save output file
    output_dir = f"data/output/pm25_components__randall/{cfg.polygon_name}_{cfg.temporal_freq}/"
    output_filename = f"{output_dir}pm25_components__randall__{cfg.polygon_name}_{cfg.temporal_freq}_{cfg.year}.parquet"

    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.abspath(output_filename)
    LOGGER.info(f"Saving final output to {output_path}")
    
    # save to parquet
    final_df.to_parquet(output_path, index=False)

    LOGGER.info(f"Successfully created merged file: {output_path}")


if __name__ == "__main__":
    main()
