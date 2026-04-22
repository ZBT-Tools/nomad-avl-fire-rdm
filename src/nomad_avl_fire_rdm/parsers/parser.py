from typing import (
    TYPE_CHECKING,
)

from nomad_avl_fire_rdm.helpers.asix_parser import parse_asix

if TYPE_CHECKING:
    from nomad.datamodel.datamodel import (
        EntryArchive,
    )
    from structlog.stdlib import (
        BoundLogger,
    )

import importlib
import os
from stat import S_ISDIR

import dotenv
import pandas as pd
import paramiko
from nomad.config import config
from nomad.datamodel.metainfo.workflow import Workflow
from nomad.parsing.parser import MatchingParser
import json

import nomad_avl_fire_rdm.helpers.firem_name_parser_integration as firem_parser


importlib.reload(firem_parser)
from ..helpers.firem_name_parser_integration import (
    load_yaml_from_github,
    normalize_2d_results_columns,
    rename_2d_results_columns,
)

configuration = config.get_plugin_entry_point(
    'nomad_avl_fire_rdm.parsers:parser_entry_point'
)


def retrieve_avl_fire_data_paths(
    sftp_client,
    project_directory,
    model_name,
    case_set_name,
    data_directory,
    file_extension,
    case_name=None,
):
    simulation_project_path = f'{project_directory}/simulation/project/'
    if case_name is None:
        print(
            'case_name is None, searching for all cases in the specified model and case set...'
        )
        data_paths = []
        for entry in sftp_client.listdir_attr(simulation_project_path):
            if (
                S_ISDIR(entry.st_mode)
                and f'{model_name}.{case_set_name}.' in entry.filename
            ):
                case_path = f'{simulation_project_path}{entry.filename}'
                data_path = f'{case_path}/{data_directory}/{model_name}{file_extension}'
                data_paths.append(data_path)
                print(f'Found case: {entry.filename}, data path: {data_path}')
    else:
        case_set_path = f'{simulation_project_path}{model_name}.{case_set_name}'
        data_path = (
            f'{case_set_path}.{case_name}/{data_directory}/{model_name}{file_extension}'
        )
        print(f'Using specified case_name: {case_name}, data path: {data_path}')
        data_paths = [f'{data_path}']
    return data_paths


class NewParser(MatchingParser):
    def parse(
        self,
        mainfile: str,
        archive: 'EntryArchive',
        logger: 'BoundLogger',
        child_archives: dict[str, 'EntryArchive'] = None,
    ) -> None:
        logger.info('NewParser.parse', parameter=configuration.parameter)
        print(mainfile)

        dotenv.load_dotenv()
        with open(mainfile, 'r') as f:
            config = json.load(f)

        hostname = config['hostname']
        user = config['USER']
        password = config['PASSWORD']

        PROJECT_DIRECTORY = '/mnt/data_raid/feierabend/AVL_FIRE/PEMWE/PEMStar_2'  # Project directory on the remote server
        MODEL_NAME = 'PEMStar_BekaertPTL'  # AVL FIRE model name
        CASE_SET_NAME = 'PolCurve_Bek~rtPTL_Update'  # Case set name within the model
        CASE_NAME = None  # Set to None to search

        ssh_client = paramiko.SSHClient()
        # Automatically add the server's host key. For production, it's better to manage known_hosts explicitly.
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Connect to the SSH server
            ssh_client.connect(hostname=hostname, username=user, password=password)
            print(f'Connected to {hostname} using password.')

            # Open an SFTP client
            sftp_client = ssh_client.open_sftp()
            print('Opened SFTP session.')
        except paramiko.AuthenticationException:
            print(
                'Authentication failed. Check your username, password, or private key.'
            )
        except paramiko.SSHException as e:
            print(f'Could not establish SSH connection: {e}')

        input_data_paths = retrieve_avl_fire_data_paths(
            sftp_client=sftp_client,
            project_directory=PROJECT_DIRECTORY,
            model_name=MODEL_NAME,
            case_set_name=CASE_SET_NAME,
            data_directory='input',
            file_extension='.asix',
        )
        input_data_dicts_list = []
        for data_path in input_data_paths:
            with sftp_client.open(data_path, 'r') as data_file:
                # data = remote_file.read()
                data = parse_asix(
                    data_file,
                    always_list=False,
                    keep_all_attributes=True,
                    cast_values=True,
                )
                input_data_dicts_list.append(data)

        input_data = input_data_dicts_list[0]

        results_2d_data_paths = retrieve_avl_fire_data_paths(
            sftp_client=sftp_client,
            project_directory=PROJECT_DIRECTORY,
            model_name=MODEL_NAME,
            case_set_name=CASE_SET_NAME,
            data_directory='results',
            file_extension='.csv',
        )
        data_path = results_2d_data_paths[0]
        result_2d_result_list = []
        for data_path in results_2d_data_paths:
            with sftp_client.open(data_path, 'r') as data_file:
                df = pd.read_csv(
                    data_file, header=[1, 2], sep=';'
                )  # Adjust separator if needed
                result_2d_result_list.append(df)
        result_2d = result_2d_result_list[0]

        results_monitoring_data_paths = retrieve_avl_fire_data_paths(
            sftp_client=sftp_client,
            project_directory=PROJECT_DIRECTORY,
            model_name=MODEL_NAME,
            case_set_name=CASE_SET_NAME,
            data_directory='results',
            file_extension='_flc.csv',
        )

        result_monitoring_result_list = []
        for data_path in results_monitoring_data_paths:
            with sftp_client.open(data_path, 'r') as data_file:
                df = pd.read_csv(
                    data_file, header=[1, 2], sep=';'
                )  # Adjust separator if needed
                result_monitoring_result_list.append(df)

        if 'sftp_client' in locals() and sftp_client:
            sftp_client.close()
            print('SFTP session closed.')
        if 'ssh_client' in locals() and ssh_client:
            ssh_client.close()
            print('SSH connection closed.')

        rules_path = load_yaml_from_github()

        # Single case
        mapping_df = normalize_2d_results_columns(result_2d, input_data, rules_path)
        df_2d_renamed, rename_map = rename_2d_results_columns(
            result_2d, input_data, rules_path
        )

        archive.workflow2 = Workflow(name='test')
