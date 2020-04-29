import os
import pathlib

import databroker
import yaml

from ._utils import CatalogNameExists

__all__ = ("unpack_inplace",)


def unpack_inplace(path, catalog_name, merge=False):
    """
    Place a catalog configuration file in the user configuration area.

    Parameters
    ----------
    path: Path
        Path to output from pack
    catalog_name: Str
        A unique name for the catalog
    merge: Boolean
        Unpack into an existing catalog

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
    dest_catalog_file_name = f"databroker_unpack_{catalog_name}.yml"
    config_dir = databroker.catalog_search_path()[0]
    dest_catalog_file_path = pathlib.Path(config_dir, dest_catalog_file_name)
    exists = catalog_name in databroker.catalog
    if exists:
        if not merge:
            raise CatalogNameExists(catalog_name)
        if not os.path.isfile(dest_catalog_file_name):
            raise ValueError(
                "The catalog exists but not in the user-writable location. "
                "Pick a different catalog name."
            )
        with open(dest_catalog_file_path) as file:
            existing_catalog = yaml.safe_load(file)
        existing_source = existing_catalog["sources"][catalog_name]
    else:
        existing_source = None

    with open(source_catalog_file_path) as file:
        catalog = yaml.safe_load(file)
    source = catalog["sources"].pop("packed_catalog")

    # Check that the drivers match. For example, we cannot merge a
    # msgpack-backed catalog into an existing JSONL-backed one.
    if existing_source is not None:
        source_driver = source.get("driver")
        existing_source_driver = existing_source.get("driver")
        if existing_source_driver != source_driver:
            raise ValueError(
                f"Cannot merge source with driver {source_driver} into source "
                f"with driver {existing_source_driver}"
            )

    # Handle temporary condition where 'pack' puts absolute paths in "args"
    # and puts relative paths off to the side.
    relative_paths = source.get("metadata", {}).get("relative_paths")
    if relative_paths:
        new_paths = [
            str(pathlib.Path(path, rel_path).absolute()) for rel_path in relative_paths
        ]
        source["args"]["paths"] = sorted(new_paths)

    # Merge paths and relative_paths from existing source, if applicable.
    if existing_source is not None:
        source["args"]["paths"] = sorted(
            set(source["args"]["paths"]) | set(existing_source["args"]["paths"])
        )
        if "metadata" not in source:
            source["metadata"] = {}
        source["metadata"]["relative_paths"] = sorted(
            set(source["metadata"].get("relative_paths", []))
            | set(existing_source.get("metadata", {}).get("relative_paths", []))
        )

    # The root_map values may be relative inside a pack, given relative to the
    # catalog file. Now that we are going to use a catalog file in a config
    # directory, we need to make these paths absolute.
    for k, v in source["args"].get("root_map", {}).items():
        if not pathlib.Path(v).is_absolute():
            source["args"]["root_map"][k] = str(pathlib.Path(path, v))

    # Merge root_map from existing source, if applicable.
    if existing_source is not None:
        collisions = set(source["args"].get("root_map", {})).intersection(
            set(existing_source["args"].get("root_map", {}))
        )
        if collisions:
            raise ValueError(
                "root_map between existing source and new source have "
                f"colliding keys {collisions}"
            )
        source["args"]["root_map"].update(existing_source["args"].get("root_map", {}))

    catalog["sources"][catalog_name] = source
    os.makedirs(config_dir, exist_ok=True)
    with open(dest_catalog_file_path, "xt") as file:
        yaml.dump(catalog, file)
    return dest_catalog_file_path
