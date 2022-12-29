import dataclasses
import functools
import pathlib
import typing

import jsonref
import singer
import singer.metadata
from pendulum.datetime import DateTime

ParametersFn = typing.Callable[[DateTime | None, DateTime | None], list[dict]]

no_parameters: ParametersFn = lambda *_: [{}]


@dataclasses.dataclass
class Stream:
    endpoint: str
    key_properties: list[str]
    schema_path: pathlib.Path
    paginate: bool
    replication: bool = False
    parameters_fn: ParametersFn = no_parameters

    @functools.cached_property
    def schema(self) -> singer.Schema:
        with self.schema_path.open("r") as file:
            return singer.Schema.from_dict(jsonref.load(file))

    @property
    def name(self) -> str:
        return self.endpoint

    @property
    def stream_id(self) -> str:
        return self.endpoint

    @property
    def metadata(self):
        return {
            (): {
                "selected": True,
                "selected-by-default": True,
                "inclusion": "available",
                "table-key-properties": self.key_properties,
            }
        }

    @property
    def catalog_entry(self) -> singer.CatalogEntry:
        return singer.CatalogEntry(
            tap_stream_id=self.stream_id,
            stream=self.name,
            schema=self.schema,
            key_properties=self.key_properties,
            metadata=singer.metadata.to_list(self.metadata),
        )
