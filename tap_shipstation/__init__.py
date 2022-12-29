import os
import pathlib
import typing
from datetime import datetime, timedelta

import pendulum
import singer
from pendulum.datetime import DateTime
from singer import Catalog, metadata, utils

from tap_shipstation.client import TIMEZONE, ShipStationClient, prepare_datetime
from tap_shipstation.stream import Stream

REQUIRED_CONFIG_KEYS = ["api_key", "api_secret", "default_start_datetime"]
LOGGER = singer.get_logger()

Record = typing.Dict[str, object]
Config = typing.Dict[str, object]
State = typing.Dict[str, object]


def get_abs_path(path: str) -> pathlib.Path:
    return pathlib.Path(os.path.dirname(os.path.realpath(__file__)), path)


def orders_parameters(start, end):
    return [
        {
            "modifyDateStart": prepare_datetime(start),
            "modifyDateEnd": prepare_datetime(end),
        }
    ]


def shipments_parameters(start, end):
    return [
        {
            "createDateStart": prepare_datetime(start),
            "createDateEnd": prepare_datetime(end),
            "includeShipmentItems": True,
            "void": False,
        },
        {
            "voidDateStart": prepare_datetime(start),
            "voidDateEnd": prepare_datetime(end),
            "includeShipmentItems": True,
        },
    ]


STREAMS = [
    Stream(
        endpoint="shipments",
        key_properties=["shipmentId"],
        schema_path=pathlib.Path(get_abs_path("schemas/shipments.json")),
        paginate=True,
        replication=True,
        parameters_fn=shipments_parameters,
    ),
    Stream(
        endpoint="orders",
        key_properties=["orderId"],
        schema_path=pathlib.Path(get_abs_path("schemas/orders.json")),
        paginate=True,
        replication=True,
        parameters_fn=orders_parameters,
    ),
    Stream(
        endpoint="stores",
        key_properties=["storeId"],
        schema_path=pathlib.Path(get_abs_path("schemas/stores.json")),
        paginate=False,
        replication=False,
    ),
]


def discover() -> Catalog:
    return Catalog([stream.catalog_entry for stream in STREAMS])


def get_selected_streams(catalog: Catalog) -> "list[Stream]":
    """
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    """
    selected_stream_ids = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)
        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_stream_ids.append(stream.tap_stream_id)

    return [stream for stream in STREAMS if stream.stream_id in selected_stream_ids]


def get_sync_start_date(config: Config, state: State, stream: Stream) -> DateTime:
    bookmark = singer.get_bookmark(
        state=state, tap_stream_id=stream.stream_id, key="modifyDate"
    )

    if bookmark:
        return pendulum.parse(bookmark, tz=TIMEZONE)  # type: ignore
    else:
        return pendulum.parse(str(config["default_start_datetime"]), tz=TIMEZONE)  # type: ignore


def sync(config: Config, state: State, catalog: Catalog):
    selected_streams = get_selected_streams(catalog)
    client = ShipStationClient(config)

    for stream in selected_streams:
        LOGGER.info("Beginning sync of stream '%s'.", stream.stream_id)

        singer.write_schema(
            stream.stream_id, stream.schema.to_dict(), stream.key_properties
        )
        sync_stream(config, state, client, stream)

        LOGGER.info("Finished syncing stream '%s'.", stream.stream_id)


def sync_stream(
    config: Config, state: State, client: ShipStationClient, stream: Stream
):
    if not stream.replication:
        for params in stream.parameters_fn(None, None):
            response = client.fetch_endpoint(stream.endpoint, params)
            sync_records(stream, response)
    else:
        start_at = get_sync_start_date(config, state, stream)
        stream_end_at = pendulum.now(TIMEZONE)
        end_at = start_at
        while end_at < stream_end_at:
            # Increment queries by 1 day, limit to stream end datetime
            end_at += timedelta(days=1)
            if end_at > stream_end_at:
                end_at = stream_end_at

            # For endpoints requiring multiple queries, cycle through timestamps
            for params in stream.parameters_fn(start_at, end_at):
                for page in client.paginate(stream.endpoint, params):
                    sync_records(stream, page)

            # Write state at end of daily loop for stream
            state = singer.write_bookmark(
                state=state,
                tap_stream_id=stream.stream_id,
                key="modifyDate",
                val=end_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
            singer.write_state(state)
            start_at = end_at


def sync_records(stream: Stream, records: dict):
    for record in records:
        transformed = singer.transform(record, stream.schema.to_dict())
        singer.write_record(stream.stream_id, transformed)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
