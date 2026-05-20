import h5py


def convert_to_hdf(archive, filename, dataframe):
    with archive.m_context.raw_file(filename, "w") as newfile:
        with h5py.File(newfile.name, "w") as hdf:
            for key in dataframe.columns:

                values = dataframe[key].tolist()

                group = hdf.create_group(key)
                group.create_dataset("value", data=values)
                # group.create_dataset("time", data=dataframe["Datum"].tolist())
                # group.attrs["axes"] = "time"
                # group.attrs["signal"] = "value"
                # group.attrs["NX_class"] = "NXdata"


def convert_to_hdf_multiple(archive, filename, dataframe_list):

    with archive.m_context.raw_file(filename, "w") as newfile:
        with h5py.File(newfile.name, "w") as hdf:
            for i, dataframe in enumerate(dataframe_list):
                for key in dataframe.columns:
                    group = hdf.create_group(f"{i}/{key}")
                    values = dataframe[key].tolist()
                    group.create_dataset(key, data=values)
