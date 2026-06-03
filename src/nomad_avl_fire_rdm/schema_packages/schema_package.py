from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from nomad.datamodel.datamodel import (
        EntryArchive,
    )
    from structlog.stdlib import (
        BoundLogger,
    )

from nomad.config import config
from nomad.datamodel.data import Schema
from nomad.datamodel.metainfo.annotations import ELNAnnotation, ELNComponentEnum
from nomad.metainfo import JSON, Quantity, SchemaPackage, SubSection
from nomad.datamodel.metainfo.plot import PlotlyFigure, PlotSection
from nomad.datamodel.hdf5 import HDF5Reference

configuration = config.get_plugin_entry_point(
    "nomad_avl_fire_rdm.schema_packages:schema_package_entry_point"
)

m_package = SchemaPackage()


class EnsightCaseResults(Schema):
    h5_value = Quantity(type=HDF5Reference)


class AsixResults(Schema):
    asix_item = Quantity(type=JSON)


class NewSchemaPackage(Schema):
    asix_results = SubSection(section=AsixResults, repeats=True)
    ensight_case_results = SubSection(section=EnsightCaseResults, repeats=True)
    name = Quantity(
        type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity)
    )
    message = Quantity(type=str)

    def normalize(self, archive: "EntryArchive", logger: "BoundLogger") -> None:
        super().normalize(archive, logger)

        logger.info("NewSchema.normalize", parameter=configuration.parameter)

        self.message = f"Hello {self.name}!"


m_package.__init_metainfo__()
