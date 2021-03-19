"""Tool to convert big query schema in dict format to SQL schema (with CREATE TABLE and COMMENT on
statements."""

import re
import crowdtangle_bigquery_schema

big_query_type_to_sql_type = {
    'STRING': 'character varying',
    'INT64': 'bigint',
    'DATETIME': 'timestamp with time zone',
    'TIMESTAMP': 'timestamp with time zone',
    'BOOLEAN': 'boolean',
    'FLOAT': 'double precision',
    'ACCOUNT': 'FIX_ME_ACCOUNT',
}

POST_ID_FK_STATEMENT = (
    'CONSTRAINT post_id_fk FOREIGN KEY (post_id) REFERENCES {schema_name}.posts (id) MATCH SIMPLE ON UPDATE '
    'NO ACTION ON DELETE NO ACTION')
ACCOUNT_ID_FK_STATEMENT_FORMAT = (
    'CONSTRAINT {column_name}_fk FOREIGN KEY ({column_name}) REFERENCES {schema_name}.accounts (id) MATCH SIMPLE ON '
    'UPDATE NO ACTION ON DELETE NO ACTION')
LAST_MODIFIED_TIME_COLUMN = (
    'last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL')

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def get_sql_datatype_from_big_query_type(field):
    return big_query_type_to_sql_type[field['type'].upper()]

def get_column_comment_statement(schema_name, table_name, column_name, comment):
    return 'COMMENT ON COLUMN {schema_name}.{table_name}.{column_name} IS \'{comment}\';\n'.format(
        schema_name=schema_name, table_name=table_name, column_name=column_name, comment=comment)

def convert_fields_list_to_create_table_statement(schema_name, table_name, table_comment, fields):
    column_statements = []
    column_comments = {}
    fk_statements = []
    for field in fields:
        name = camel_to_snake(field['name'])
        if name == 'post_id':
            fk_statements.append(POST_ID_FK_STATEMENT.format(schema_name=schema_name))
        elif name.endswith('account_id'):
            fk_statements.append(ACCOUNT_ID_FK_STATEMENT_FORMAT.format(schema_name=schema_name, column_name=name))

        datatype = get_sql_datatype_from_big_query_type(field)
        if field['mode'] == 'NULLABLE':
            column_statements.append('{name} {datatype}'.format(name=name, datatype=datatype))
        elif field['mode'] == 'REQUIRED':
            column_statements.append('{name} {datatype} NOT NULL'.format(name=name, datatype=datatype))
        elif field['mode'] == 'PRIMARY_KEY':
            column_statements.append('{name} {datatype} PRIMARY KEY'.format(name=name, datatype=datatype))
        else:
            raise ValueError('Unrecognized mode: %s', field['mode'])
        if 'description' in field:
            column_comments[name] = field['description'].replace('\'', '\'\'')

    column_statements.append(LAST_MODIFIED_TIME_COLUMN)

    column_comments_str = ''.join([get_column_comment_statement(schema_name, table_name, column_name, comment)
                                   for column_name, comment in column_comments.items()])
    create_statement = 'CREATE TABLE {schema_name}.{table_name} (\n  {column_statements_str}\n);'.format(
        schema_name=schema_name, table_name=table_name,
        column_statements_str=',\n  '.join(column_statements + fk_statements))
    return create_statement, column_comments_str


def convert_big_query_dict_schema_to_sql(big_query_schema, schema_name='public'):
    create_statements = []
    comment_statements = []
    for name, sub_schema in big_query_schema.items():
        table_name = sub_schema['name']
        table_comment = sub_schema['description']
        create_statement, comment_statement = convert_fields_list_to_create_table_statement(
            schema_name, table_name, table_comment, sub_schema['fields'])
        create_statements.append(create_statement)
        comment_statements.append(comment_statement)

    print('CREATE SCHEMA', schema_name, ';')
    print('SELECT pg_catalog.set_config(\'search_path\',', '\'{}\''.format(schema_name), ', false);')
    print('\n'.join(create_statements + comment_statements))


if __name__ == '__main__':
    convert_big_query_dict_schema_to_sql(crowdtangle_bigquery_schema.CROWDTANGLE_BIGQUERY_SCHEMAS)
