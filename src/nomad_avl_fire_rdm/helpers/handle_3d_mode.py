from src.utils import sftp_get_dir
import os
from src.ensight_to_xdmf import (
    convert_ensight_case,
    EnsightConversionConfig,
)
from pathlib import Path
import shutil
import h5py
import xml.etree.ElementTree as ET


def _get_3d_output_paths():
    output_dir = Path("data/3D_EnSight_converted")
    filename_h5 = "fields.h5"
    filename_xdmf = "fields.xdmf"
    return filename_h5, output_dir, output_dir / filename_h5, output_dir / filename_xdmf


def _download_ensight_data(sftp_client, input_data_path, data_directory):
    destination = os.path.join("data", data_directory.split(".")[-1])
    sftp_get_dir(sftp_client, input_data_path, destination)
    return destination


def _create_ensight_conversion_config():
    return EnsightConversionConfig(
        case_file=Path(r"data/results/3D_EnSight/PEMStar_BekaertPTL_DOM_8_0.case"),
        output_dir=Path(r"data/3D_EnSight_converted"),
        case_id="PEMStar_BekaertPTL_DOM_8_0",
    )


def _convert_ensight_data():
    config = _create_ensight_conversion_config()
    return convert_ensight_case(config, only_last_time=True)


def _archive_h5_file(archive, saved_path_h5, filename):
    with archive.m_context.raw_file(filename, "w") as newfile:
        shutil.move(str(saved_path_h5), newfile.name)


def rename_h5_keys(saved_path_h5, rename_dict):

    with h5py.File(saved_path_h5, "r+") as h5_file:

        for old_key, new_name in rename_dict.items():
            if old_key in h5_file:
                print(f"Renaming '{old_key}' to '{new_name}'")
                # change keys
                h5_file.move(old_key, new_name)
            else:
                print(
                    f"Warning: Key '{old_key}' requested by XML was not found in {saved_path_h5}"
                )


def handle_3d_mode(sftp_client, input_data_paths, data_directory, archive):
    print("3D mode enabled, processing EnSight data...")

    _download_ensight_data(sftp_client, input_data_paths[3], data_directory)
    metadata = _convert_ensight_data()

    filename, output_dir, saved_path_h5, saved_path_xdmf = _get_3d_output_paths()

    tree = ET.parse(saved_path_xdmf)
    root = tree.getroot()
    rename_map = {}
    print(root)
    for attribute in root.iter("Attribute"):
        attr_name = attribute.get("Name")

        # Find the nested <DataItem> element
        data_item = attribute.find("DataItem")
        if data_item is not None and data_item.text:
            # The text looks like "fields.h5:/data2". I have split by ':' to isolate the key name
            text_content = data_item.text.strip()
            if ":" in text_content:
                h5_path = text_content.split(":")[-1]  # yields "/data2"
                old_key = h5_path.lstrip("/")  # yields "data2"

                rename_map[old_key] = attr_name

    rename_h5_keys(saved_path_h5, rename_map)

    _archive_h5_file(archive, saved_path_h5, filename)
