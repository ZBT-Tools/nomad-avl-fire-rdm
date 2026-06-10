from src.utils import sftp_get_dir
import os
from src.ensight_to_xdmf import (
    convert_ensight_case,
    EnsightConversionConfig,
)
from pathlib import Path
import shutil


def handle_3d_mode(sftp_client, input_data_paths, data_directory, archive):
    print("3D mode enabled, processing EnSight data...")
    sftp_get_dir(
        sftp_client,
        input_data_paths[3],
        os.path.join("data", data_directory.split(".")[-1]),
    )
    metadata = convert_ensight_case(
        EnsightConversionConfig(
            case_file=Path(r"data/results/3D_EnSight/PEMStar_BekaertPTL_DOM_8_0.case"),
            output_dir=Path(r"data/3D_EnSight_converted"),
            case_id="PEMStar_BekaertPTL_DOM_8_0",
        ),
        only_last_time=True,
    )
    saved_path = "data/3D_EnSight_converted/fields.h5"
    filename = "fields.h5"

    with archive.m_context.raw_file(filename, "w") as newfile:
        shutil.move(saved_path, newfile.name)
        shutil.rmtree("data/3D_EnSight_converted/")
