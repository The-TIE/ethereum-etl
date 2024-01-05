import collections
import json
import time

from sqlalchemy import (
    create_engine,
    exc,
    text,
)

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter


class MySQLItemExporter:

    def __init__(self, connection_url, item_type_to_insert_stmt_mapping, converters=(), print_sql=True):
        self.connection_url = connection_url
        self.item_type_to_insert_stmt_mapping = item_type_to_insert_stmt_mapping
        self.converter = CompositeItemConverter(converters)
        self.print_sql = print_sql

        self.engine = self.create_engine()

    def open(self):
        pass

    def export_items(self, items):
        items_grouped_by_type = group_by_item_type(items)

        for item_type, insert_stmt in self.item_type_to_insert_stmt_mapping.items():
            item_group = items_grouped_by_type.get(item_type)
            if item_group:
                connection = self.engine.connect()
                converted_items = list(self.convert_items(item_group))
                try:
                    # Reformat.
                    new_converted_items = []
                    for cv in converted_items:
                        for k, v in cv.items():
                            if type(v) == list:
                                cv[k] = json.dumps(cv[k])


                        # Remove null txhash on traces.
                        if "transaction_hash" in cv and not cv["transaction_hash"]:
                            # Block reward trace has no transaction hash.
                            cv["transaction_hash"] = ""

                        new_converted_items.append(cv)

                    # Do insert.
                    connection.execute(insert_stmt, converted_items)
                except exc.IntegrityError as e:
                    continue

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def create_engine(self):
        engine = create_engine(self.connection_url, echo=self.print_sql, pool_recycle=3600)
        return engine

    def close(self):
        pass


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result