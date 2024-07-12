# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from elastic_connectors.connectors.source import DataSourceConfiguration
from elastic_connectors.connectors.sources.directory import DirectoryDataSource
from elastic_connectors.connector_base import ConnectorBase


class DirectoryConnector(ConnectorBase):
    """
    DirectoryConnector class generated for connecting to the data source.

    Args:

        directory (str): Directory path

        pattern (str): File glob-like pattern

    """

    def __init__(
        self,
        directory="/Users/jedr/connectors/lib/python3.10/site-packages/connectors/sources",
        pattern="**/*.*",
        **kwargs
    ):

        configuration = DirectoryDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=DirectoryDataSource(connector_configuration), **kwargs
        )

        self.directory = directory
        self.pattern = pattern