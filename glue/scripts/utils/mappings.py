postgres_to_pandas_datatypes = {
    "smallint": "int16",
    "integer": "int32",
    # "integer": "int64",  TODO
    "bigint": "int64",
    "decimal": "float64",  # Adjust if precision is needed
    "numeric": "float64",  # Adjust if precision is needed
    "real": "float32",
    "double precision": "float64",
    "serial": "int32",
    "bigserial": "int64",
    "character varying": "object",
    "character": "object",
    "varchar": "object",
    "text": "object",
    "boolean": "bool",
    "date": "datetime64[ns]",
    "time without time zone": "object",  # Adjust if specific dtype is required
    "time with time zone": "object",  # Adjust if specific dtype is required
    "timestamp without time zone": "datetime64[ns]",
    "timestamp with time zone": "datetime64[ns, UTC]",  # Adjust if specific dtype is required
    "interval": "timedelta64[ns]",
    "date": "object",
    "uuid": "object",
    "json": "object",
    "jsonb": "object",
    "bytea": "object",
    "inet": "object",
    "cidr": "object",
    "macaddr": "object",
    "bit": "object",
    "bit varying": "object",
    "geometry": "object",
}

postgres_compatible_datatypes = {
    "smallint": ["smallint"],
    "integer": ["smallint", "integer"],
    "bigint": ["smallint", "integer", "bigint"],
    "decimal": ["decimal"],
    "numeric": ["decimal"],
    "real": ["real"],
    "double precision": ["real", "double precision"],
    "serial": ["serial"],
    "bigserial": ["serial", "bigserial"],
    "character": ["character"],
    "character varying": ["character varying"],
    "varchar": ["character varying"],
    "text": ["character varying", "text", "int", ""],
    "boolean": ["boolean"],
    "date": ["date"],
    "time without time zone": ["time without time zone"],
    "time with time zone": ["time with time zone"],
    "timestamp without time zone": ["timestamp without time zone"],
    "timestamp with time zone": ["timestamp with time zone"],
    "interval": ["interval"],
    "uuid": ["uuid"],
    "json": ["json"],
    "jsonb": ["jsonb", "json"],
    "bytea": ["bytea"],
    "inet": ["inet"],
    "cidr": ["inet", "cidr"],
    "macaddr": ["macaddr"],
    "bit": ["bit"],
    "bit varying": ["bit varying", "bit"],
    "geometry": ["geometry"],
}

pandas_to_postgres_datatypes = {
    "int8": "smallint",
    "int16": "smallint",
    "int32": "integer",
    "int64": "bigint",
    "uint8": "smallint",
    "uint16": "integer",
    "uint32": "bigint",
    "uint64": "bigint",
    "float16": "real",
    "float32": "real",
    "float64": "double precision",
    "object": "text",
    "bool": "boolean",
    "datetime64[ns]": "timestamp without time zone",
    "timedelta64[ns]": "interval",
    "category": "text",
    "datetime64[ns, UTC]": "timestamp with time zone",  # Adjust if needed
}

pandas_compatible_datatypes = {
    "int8": ["int8"],
    "int16": ["int8", "int16"],
    "int32": ["int8", "int16", "int32"],
    "int64": ["int8", "int16", "int32", "int64"],
    "uint8": ["uint8"],
    "uint16": ["uint8", "uint16"],
    "uint32": ["uint8", "uint16", "uint32"],
    "uint64": ["uint8", "uint16", "uint32", "uint64"],
    "float16": ["float16", "int8", "int16"],
    "float32": ["float16", "float32", "int8", "int16", "int32"],
    "float64": ["float16", "float32", "float64", "int8", "int16", "int32", "int64"],
    "object": ["object", "int8", "int16", "int32", "int64", "float32", "float16"],
    "bool": ["bool"],
    "datetime64[ns]": ["datetime64[ns]"],
    "timedelta64[ns]": ["timedelta64[ns]"],
    "category": ["category"],
}
