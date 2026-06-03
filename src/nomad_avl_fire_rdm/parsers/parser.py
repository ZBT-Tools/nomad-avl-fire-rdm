from fileinput import filename
import os
import shutil
import shutil
from typing import (
    TYPE_CHECKING,
)

import h5py
from nomad import archive
from paramiko.agent import key

from nomad_avl_fire_rdm.helpers.nomad_helpers import (
    convert_to_hdf,
    convert_to_hdf_multiple,
)
from nomad_avl_fire_rdm.schema_packages.schema_package import (
    AsixResults,
    NewSchemaPackage,
)

if TYPE_CHECKING:
    from nomad.datamodel.datamodel import (
        EntryArchive,
    )
    from structlog.stdlib import (
        BoundLogger,
    )

import importlib
from stat import S_ISDIR

import dotenv
import pandas as pd
import paramiko
from nomad.config import config
from nomad.datamodel.metainfo.workflow import Workflow
from nomad.parsing.parser import MatchingParser
from nomad.files import StagingUploadFiles
from nomad.datamodel.context import ServerContext
from src.asix_parser import parse_asix
import yaml
from src.ensight_to_xdmf import (
    convert_ensight_case,
    EnsightConversionConfig,
)
import pyvista as pv
import numpy as np
import json
import pandas as pd

# importing from the AVL-FIRE repo, not the "src" folder of this repo,
import src.firem_name_parser_integration as firem_parser
from src.firem_name_parser_integration import (
    load_yaml_from_github,
    normalize_2d_results_columns,
    rename_2d_results_columns,
)
from src.utils import retrieve_avl_fire_data_paths, sftp_get_dir
from pathlib import Path

# from src.ensight_to_xdmf import EnsightConversionConfig

from nomad.datamodel.metainfo.plot import PlotlyFigure, PlotSection
import uuid
from nomad.datamodel.hdf5 import HDF5Reference
from nomad_avl_fire_rdm.schema_packages.schema_package import EnsightCaseResults

importlib.reload(firem_parser)

configuration = config.get_plugin_entry_point(
    "nomad_avl_fire_rdm.parsers:parser_entry_point"
)


class NewParser(MatchingParser):
    def parse(
        self,
        mainfile: str,
        archive: "EntryArchive",
        logger: "BoundLogger",
        child_archives: dict[str, "EntryArchive"] = None,
    ) -> None:
        logger.info("NewParser.parse", parameter=configuration.parameter)
        print(mainfile)

        archive.metadata.upload_id = (
            archive.m_context.upload_id if archive.m_context.upload_id else "unknown"
        )

        upload_id = archive.metadata.upload_id

        archive.metadata.entry_id = f"avl-{uuid.uuid4()}"

        archive.data = NewSchemaPackage()

        dotenv.load_dotenv()
        with open(mainfile, "r") as f:
            config = yaml.safe_load(f)

        hostname = config["hostname"]
        user = config["USER"]
        password = config["PASSWORD"]
        PROJECT_DIRECTORY = config["PROJECT_DIRECTORY"]
        MODEL_NAME = config["MODEL_NAME"]
        CASE_SET_NAME = config["CASE_SET_NAME"]
        CASE_NAME = None  # Set to None to search
        data_directory = config["data_directory"]
        mode_3d = config["mode_3d"] if "mode_3d" in config else False
        ssh_client = paramiko.SSHClient()
        # Automatically add the server's host key. For production, it's better to manage known_hosts explicitly.
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Connect to the SSH server
            ssh_client.connect(hostname=hostname, username=user, password=password)
            print(f"Connected to {hostname} using password.")

            # Open an SFTP client
            sftp_client = ssh_client.open_sftp()
            print("Opened SFTP session.")
        except paramiko.AuthenticationException:
            print(
                "Authentication failed. Check your username, password, or private key."
            )
        except paramiko.SSHException as e:
            print(f"Could not establish SSH connection: {e}")

        input_data_paths = retrieve_avl_fire_data_paths(
            sftp_client=sftp_client,
            project_directory=PROJECT_DIRECTORY,
            model_name=MODEL_NAME,
            case_set_name=CASE_SET_NAME,
            data_directory=data_directory,
            file_extension=".asix" if not mode_3d else None,
        )
        if mode_3d:
            print("3D mode enabled, processing EnSight data...")
            sftp_get_dir(
                sftp_client,
                input_data_paths[3],
                os.path.join("data", data_directory.split(".")[-1]),
            )
            metadata = convert_ensight_case(
                EnsightConversionConfig(
                    case_file=Path(
                        r"data/results/3D_EnSight/PEMStar_BekaertPTL_DOM_8_0.case"
                    ),
                    output_dir=Path(r"data/3D_EnSight_converted"),
                    case_id="PEMStar_BekaertPTL_DOM_8_0",
                ),
                only_last_time=True,
            )
            saved_path = "data/3D_EnSight_converted/fields.h5"
            filename = "fields.h5"

            with archive.m_context.raw_file(filename, "w") as newfile:
                shutil.copyfile(saved_path, newfile.name)

            return

        input_data_dicts_list = []
        for data_path in input_data_paths:

            with sftp_client.open(data_path, "r") as data_file:
                # data = remote_file.read()
                data = parse_asix(
                    data_file,
                    always_list=False,
                    keep_all_attributes=True,
                    cast_values=True,
                )
                input_data_dicts_list.append(data)

        for i, data_dict in enumerate(input_data_dicts_list):
            asix_result = AsixResults()
            asix_result.asix_item = data_dict

            archive.data.asix_results.append(asix_result)
        # with open("data.json", "w") as f:
        #     dict_to_dump = input_data_dicts_list[0]["AST_input_data"]
        #     dict_to_dump.pop("AST_information")
        #     json.dump(dict_to_dump, f, indent=4, default=str)

        results_2d_data_paths = retrieve_avl_fire_data_paths(
            sftp_client=sftp_client,
            project_directory=PROJECT_DIRECTORY,
            model_name=MODEL_NAME,
            case_set_name=CASE_SET_NAME,
            data_directory="results",
            file_extension=".csv",
        )

        result_2d_result_list = []
        result_2d = None
        for data_path in results_2d_data_paths:
            try:

                with sftp_client.open(data_path, "r") as data_file:

                    df = pd.read_csv(
                        data_file, header=[1, 2], sep=";"
                    )  # Adjust separator if needed
                    result_2d_result_list.append(df)

            except Exception as e:
                print(f"Error reading 2D results from {data_path}: {e}")

        # Save as one HDF/ multiple HDF files.
        results_monitoring_data_paths = retrieve_avl_fire_data_paths(
            sftp_client=sftp_client,
            project_directory=PROJECT_DIRECTORY,
            model_name=MODEL_NAME,
            case_set_name=CASE_SET_NAME,
            data_directory="results",
            file_extension="_flc.csv",
        )

        result_monitoring_result_list = []
        for data_path in results_monitoring_data_paths:
            try:
                with sftp_client.open(data_path, "r") as data_file:
                    df = pd.read_csv(
                        data_file, header=[1, 2], sep=";"
                    )  # Adjust separator if needed
            except Exception as e:
                print(f"Error reading monitoring results from {data_path}: {e}")

        result_monitoring_result_list.append(df)

        if "sftp_client" in locals() and sftp_client:
            sftp_client.close()
            print("SFTP session closed.")
        if "ssh_client" in locals() and ssh_client:
            ssh_client.close()
            print("SSH connection closed.")

        rules_path = load_yaml_from_github()

        flat_df = pd.DataFrame()
        original_dfs = []
        for i in range(len(result_2d_result_list)):
            renamed_data, _ = rename_2d_results_columns(
                result_2d_result_list[i], input_data_dicts_list[i], rules_path
            )
            original_dfs.append(renamed_data)
            # [[], []]
            flat_df = pd.concat([flat_df, renamed_data], ignore_index=True)

        convert_to_hdf(archive, "data.hdf", flat_df)
        convert_to_hdf_multiple(archive, "monitoring_data.hdf", original_dfs)

        archive.workflow2 = Workflow(name="test")
