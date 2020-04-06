import os
import pathlib

import databroker
import yaml

from ._utils import CatalogNameExists

__all__ = ("unpack",)


def unpack(path, catalog_name):
    """
    Place a catalog configuration file in the user configuration area.

    Parameters
    ----------
    path: Path
        Path to output from pack
    catalog_name: Str
        A unique name for the catalog

    Returns
    -------
    config_path: Path
        Location of new catalog configuration file
    """
    # Validate user input.
    if not os.path.isdir(path):
        raise ValueError(f"{path} is not a directory")
    source_catalog_file_path = pathlib.Path(path, "catalog.yml")
    if not os.path.isfile(source_catalog_file_path):
        raise ValueError(f"Could not find 'catalog.yml' in {path}")
    if catalog_name in databroker.catalog:
        raise CatalogNameExists(catalog_name)

    config_dir = databroker.catalog_search_path()[0]
    with open(source_catalog_file_path) as file:
        catalog = yaml.safe_load(file)
    source = catalog["sources"].pop("packed_catalog")

    # Handle temporary condition where 'pack' puts absolute paths in "args"
    # and puts relative paths off to the side.
    if any(pathlib.Path(p).is_absolute() for p in source["args"]["paths"]):
        relative_paths = source["metadata"]["relative_paths"]
        new_paths = [str(pathlib.Path(path, rel_path)) for rel_path in relative_paths]
        source["args"]["paths"] = new_paths

    # The root_map values may be relative inside a pack, given relative to the
    # catalog file. Now that we are going to use a catalog file in a config
    # directory, we need to make these paths absolute.
    for k, v in source["args"].get("root_map", {}).items():
        if not pathlib.Path(v).is_absolute():
            source["args"]["root_map"][k] = str(pathlib.Path(path, v))

    catalog["sources"][catalog_name] = source
    config_filename = f"databroker_unpack_{catalog_name}.yml"
    config_path = pathlib.Path(config_dir, config_filename)
    os.makedirs(config_dir, exist_ok=True)
    with open(config_path, "xt") as file:
        yaml.dump(catalog, file)
    return config_path
