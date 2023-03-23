import functions
import json

## source
config_source = functions.get_json_data('config/prd.json')
schema_source = 'ecommerce.order'

## destination
config_destination = functions.get_json_data('config/local.json')
schema_destination = 'ecommerce.order'

## query
query_source = "select *, id+30000 as id from ecommerce.order where order_channel_type = 'EXTERNAL' limit 32, 10"

## copy
functions.copy_schema( config_source, config_destination, schema_source, schema_destination, query_source)
print("Done")