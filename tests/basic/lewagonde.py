import json
import os
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import pandas as pd
import psycopg2 as pg
import yaml
from psycopg2 import extensions as pgext


# Constants
###########

ENV_DB_READONLY_USER = "DB_READONLY_USER"
ENV_DB_READONLY_PASSWORD = "DB_READONLY_PASSWORD"
ENV_DB_HOST = "DB_HOST"
ENV_DB_NAME = "DB_NAME"


# Comparison functions
######################


def is_comment_line(line: str, comment_chars: str):
    """
    Returns true if the line starts with `comment_chars`.
    Strips first spaces from the left of the `line`
    """
    return line.lstrip().startswith(comment_chars)


def soft_equal_transform(txt: str, comment_chars: Union[str, None]) -> str:
    """
    Remove comments and transform newlines into spaces
    """
    txt_no_comment_no_newline = (
        txt.replace("\n", " ") if comment_chars is None else " ".join([line for line in txt.split("\n") if not is_comment_line(line, comment_chars)])
    )
    # Removes duplicate spaces and tabs
    return txt_no_comment_no_newline.replace("\t", "").replace(" ", "")


def soft_equal(a: str, b: str, comment_chars: Union[str, None] = None):
    """
    Two strings are soft-equal if
    - after removing all lines starting with the `comment_chars` (# in Python, -- in SQL)
    - transforming new lines in spaces
    - removing all spaces

    they're equal
    """
    tr_a = soft_equal_transform(a, comment_chars=comment_chars)
    tr_b = soft_equal_transform(b, comment_chars=comment_chars)
    return tr_a == tr_b


# Docker Compose
################


def is_key_value_like(txt: str) -> bool:
    """
    Checks if `txt` is of the form <something>=<some other thing>
    """

    lr = txt.split("=")
    return len(lr) == 2 and lr[0] != "" and lr[1] != ""


def is_kvlist(l: List[str]) -> bool:
    """
    Returns true iff the list is only made of strings of the sort `<some key>=<some value>`
    """
    for row in l:
        if not isinstance(row, str) or not is_key_value_like(row):
            return False
    return True


def kvlist_to_dict(kvlist: List[str]) -> Dict[str, str]:
    """
    Turns `["A=10", "B=20"]` to `{"A": 10, "B": 20}`
    """
    return dict([kv.split("=") for kv in kvlist])


def dict_or_kvlist_to_dict(d):
    """
    Applies kvlist_to_dict to key-value lists, or returns the input dict
    """
    if is_kvlist(d):
        return kvlist_to_dict(d)
    elif isinstance(d, dict):
        return d
    else:
        raise ValueError("Not a kvlist or a dict")


def docker_compose_transform_dict_block(dc_dict_block: Dict[str, Any]):
    """
    - Recursively transforms dictionaries into sorted strings
    - Removes single and double quotes
    """
    sorted_keys = sorted(dc_dict_block.keys())
    lines = []
    for key in sorted_keys:
        value_obj = dc_dict_block[key]
        value_str = ""
        if isinstance(value_obj, dict):
            # Dict -> docker_compose_transform_dict_block -> json.dumps
            value_str = docker_compose_transform_dict_block(value_obj)
        elif isinstance(value_obj, list) and is_kvlist(value_obj):
            # KVList -> Dict -> docker_compose_transform_dict_block -> json.dumps
            value_str = docker_compose_transform_dict_block(kvlist_to_dict(value_obj))
        else:
            # Anything else -> json.dumps
            value_str = json.dumps(value_obj)

        lines.append(f"{key}:{value_str}")
    return "".join(lines).replace('"', "").replace("'", "")


def docker_compose_equal(dc1_fp: str, dc2_fp: str):
    """
    Checks if two Docker-Compose files are equal.
    - Recursively transforms dictionaries into sorted strings
    - Checks soft equality between the strings
    """
    with open(dc1_fp) as f:
        dc1 = yaml.safe_load(f)
    with open(dc2_fp) as f:
        dc2 = yaml.safe_load(f)
    return docker_compose_equal_content(dc1, dc2)


def docker_compose_equal_content(dc1: Dict[str, Any], dc2: Dict[str, Any]):
    """
    Checks parsed Docker-Compose YAML soft equality
    """
    tr_dc1 = docker_compose_transform_dict_block(dc1)
    tr_dc2 = docker_compose_transform_dict_block(dc2)

    return soft_equal(tr_dc1, tr_dc2)


# Environment variables
#######################


def load_dot_env(dot_env_fp: str = "./.env"):
    """
    Given a `dot_env_fp`, parse it and export all of the env vars
    """
    envs = []
    with open(dot_env_fp) as f:
        for line in f:
            if line.startswith("#"):
                continue
            split = line.split("=")
            if len(split) != 2:
                continue
            envs.append((split[0], split[1]))

    for k, v in envs:
        os.environ[k] = v


# Pandas
########


def get_db_connection(
    password: str = os.environ.get(ENV_DB_READONLY_PASSWORD, ""),
    user: str = os.environ.get(ENV_DB_READONLY_USER, ""),
    host: str = os.environ.get(ENV_DB_HOST, ""),
    dbname: str = os.environ.get(ENV_DB_NAME, ""),
) -> pgext.connection:
    """
    _get_db_connection will get you a Postgres connection
    Try not to use directly, prefer the pd_read_sql_query helper which will open and close the DB connection for you

    Helper method to get a Postgres DB connection, with sane read-only overridable defaults
    """
    return pg.connect(f"host={host} dbname={dbname} user={user} password={password}")


def read_sql_query(
    sql: str,
    index_col=None,
    coerce_float=True,
    params=None,
    parse_dates=None,
    chunksize=None,
    dtype=None,
    password: str = os.environ.get(ENV_DB_READONLY_PASSWORD, ""),
    user: str = os.environ.get(ENV_DB_READONLY_USER, ""),
    host: str = os.environ.get(ENV_DB_HOST, ""),
    dbname: str = os.environ.get(ENV_DB_NAME, ""),
) -> pd.DataFrame:
    """
    read_sql_query wraps pandas.read_sql_query,
    handles the PDB read only connection creation and closing once the query is executed
    """
    con = get_db_connection(password=password, user=user, host=host, dbname=dbname)
    try:
        return pd.read_sql_query(
            sql=sql,
            con=con,
            index_col=index_col,
            coerce_float=coerce_float,
            params=params,
            parse_dates=parse_dates,
            chunksize=chunksize,
            dtype=dtype,
        )
    except BaseException as e:
        raise e
    finally:
        con.close()
