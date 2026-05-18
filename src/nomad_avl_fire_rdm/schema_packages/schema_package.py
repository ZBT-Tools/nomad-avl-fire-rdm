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
from nomad.metainfo import JSON, Quantity, SchemaPackage
from nomad.datamodel.metainfo.plot import PlotlyFigure, PlotSection

configuration = config.get_plugin_entry_point(
    "nomad_avl_fire_rdm.schema_packages:schema_package_entry_point"
)

m_package = SchemaPackage()


class AxisResults(Schema):
    axis_item = Quantity(type=JSON)


class NewSchemaPackage(Schema):
    cell_count__mea_flow_channels = Quantity(type=float)
    wall_time_since_start__mea_flow_channels = Quantity(type=float)
    current__electrical_conductor__cathode__terminal = Quantity(type=float)
    current_density_key = Quantity(type=int)
    axis_results = Quantity(type=JSON, repeats=True)
    name = Quantity(
        type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity)
    )
    message = Quantity(type=str)

    def normalize(self, archive: "EntryArchive", logger: "BoundLogger") -> None:
        super().normalize(archive, logger)

        logger.info("NewSchema.normalize", parameter=configuration.parameter)

        self.message = f"Hello {self.name}!"


m_package.__init_metainfo__()
