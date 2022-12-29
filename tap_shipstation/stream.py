import dataclasses
import functools
import pathlib
import typing

import jsonref
import singer
import singer.metadata
from pendulum.datetime import DateTime

ParametersFn = typing.Callable[
    [typing.Optional[DateTime], typing.Optional[DateTime]], typing.List[dict]
]

no_parameters: ParametersFn = lambda *_: [{}]


@dataclasses.dataclass
class Stream:
    endpoint: str
    key_properties: typing.List[str]
    schema_path: pathlib.Path
    paginate: bool
    replication: bool = False
    parameters_fn: ParametersFn = no_parameters
    _schema: typing.Optional[singer.Schema] = None

    @property
    def schema(self) -> singer.Schema:
        if self._schema is None:
            with self.schema_path.open("r") as file:
                self._schema = singer.Schema.from_dict(jsonref.load(file))
        return self._schema

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
