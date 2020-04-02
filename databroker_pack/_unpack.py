import os
import pathlib

import databroker.utils
import yaml

from ._utils import CatalogNameExists

__all__ = ("unpack",)


def unpack(catalog_name, path):
    """
    Place a catalog configuration file in the user configuration area.

    Parameters
    ----------
    catalog_name: Str
        A unique name for the catalog
    path: Path
        Path to output from pack

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
        raise ValueError(f"Cold not find 'catalog.yml' in {path}")
    if catalog_name in databroker.utils.list_configs():
        raise CatalogNameExists(catalog_name)

    config_dir = databroker.catalog_search_path()[0]
    with open(source_catalog_file_path) as file:
        catalog = yaml.safe_load(file)
    source = catalog["sources"].pop("packed_catalog")
    relative_paths = source["metadata"]["relative_paths"]
    new_paths = [str(pathlib.Path(path, rel_path)) for rel_path in relative_paths]
    source["args"]["paths"] = new_paths
    catalog["sources"][catalog_name] = source
    config_filename = f"databroker_unpack_{catalog_name}.yml"
    config_path = pathlib.Path(config_dir, config_filename)
    with open(config_path, "xt") as file:
        yaml.dump(catalog, file)
    return config_path
