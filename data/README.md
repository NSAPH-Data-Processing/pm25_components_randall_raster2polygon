
`utils/create_dir_paths.py` fixes the paths or directories to run the pipeline. 


There are several options, either you can run directly

```
python utils/create_dir_paths.py
```

Or determine the configuration file inside `conf/datapaths`to be used and run

```
python utils/create_dir_paths.py datapaths=<configuration yaml>
```

The `<configuration yaml>` is read by `utils/create_dir_paths.py` as `cfg.datapaths`.

