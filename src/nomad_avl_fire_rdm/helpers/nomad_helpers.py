import h5py
import os

from paramiko import SFTPClient, SFTPFile


def convert_to_hdf(archive, filename, dataframe):
    with archive.m_context.raw_file(filename, "w") as newfile:
        with h5py.File(newfile.name, "w") as hdf:
            for key in dataframe.columns:

                values = dataframe[key].tolist()

                group = hdf.create_group(key)
                group.create_dataset("value", data=values)


def convert_to_hdf_multiple(archive, filename, dataframe_list):

    with archive.m_context.raw_file(filename, "w") as newfile:
        with h5py.File(newfile.name, "w") as hdf:
            for i, dataframe in enumerate(dataframe_list):
                for key in dataframe.columns:
                    group = hdf.create_group(f"{i}/{key}")
                    values = dataframe[key].tolist()
                    group.create_dataset(key, data=values)


def save_asix_files_to_storage(
    archive, sftp_client: SFTPClient, sftp_filenames: list[SFTPFile]
):

    for index, data_path in enumerate(sftp_filenames):
        with sftp_client.open(data_path) as file:
            print(file)
            if hasattr(file, "read"):
                try:
                    content = file.read()

                    # get filename from SFTPFile object or fallback to provided path
                    file_path = getattr(file, "name", None) or data_path
                    file_name = os.path.basename(file_path)
                    file_name = f"{index}_{file_name}"
                    # create a blank file first. Without it the code will fail.
                    with open(file_name, "wb") as file_to_write:
                        file_to_write.write(b"")

                    with archive.m_context.raw_file(file_name) as newfile:
                        print(newfile.name)
                    with open(newfile.name, "wb") as file_to_write:
                        try:
                            file_to_write.write(content)
                        except Exception as e:
                            print("Error at index", index, " error ", e)

                except Exception as e:
                    print("Error at index: ", index, "with error ", e)
