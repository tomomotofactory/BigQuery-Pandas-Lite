from google.cloud.bigquery.table import Table, _convert_timestamp, _TABLE_HAS_NO_SCHEMA


class BQLiteTable(Table):

    def insert_data(self,
                    rows,
                    row_ids=None,
                    skip_invalid_rows=None,
                    ignore_unknown_values=None,
                    template_suffix=None,
                    client=None):

        if len(self._schema) == 0:
            raise ValueError(_TABLE_HAS_NO_SCHEMA)

        client = self._require_client(client)
        rows_info = []
        data = {'rows': rows_info}

        for index, row in enumerate(rows):
            row_info = {}

            for field, value in zip(self._schema, row):
                if field.field_type == 'TIMESTAMP':
                    # BigQuery stores TIMESTAMP data internally as a
                    # UNIX timestamp with microsecond precision.
                    # Specifies the number of seconds since the epoch.
                    value = _convert_timestamp(value)

                # if column`s value is NONE or NAN: Null value in bigQuery
                if value is not None and value == value:
                    row_info[field.name] = value

            info = {'json': row_info}
            if row_ids is not None:
                info['insertId'] = row_ids[index]

            rows_info.append(info)

        if skip_invalid_rows is not None:
            data['skipInvalidRows'] = skip_invalid_rows

        if ignore_unknown_values is not None:
            data['ignoreUnknownValues'] = ignore_unknown_values

        if template_suffix is not None:
            data['templateSuffix'] = template_suffix

        response = client._connection.api_request(
            method='POST',
            path='%s/insertAll' % self.path,
            data=data)
        errors = []

        for error in response.get('insertErrors', ()):
            errors.append({'index': int(error['index']),
                           'errors': error['errors']})

        return errors
