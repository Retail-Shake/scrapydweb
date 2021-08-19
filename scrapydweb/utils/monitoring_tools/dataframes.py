# -*- coding: utf-8 -*-
import logging
import os

from . import maths as mtm


def mysql_to_df(
    url=None,  # TODO #1 @h4r1c0t: find how to setup the connection params automatically from settings.
    database="scrapydweb_jobs",
    table="128_0_0_1_6800",
    select="*",
    where="project = 'retail_shake'",
):
    """
    This function is used to automatically get data from scrapyd SQLite DB.
    By default get spyder data from 127.0.0.1:6800 table of the jobs.db

    :param url:      (str) MySQL server connection url
    :param database: (str) the database to select
    :param table:    (str) table name (default: 127.0.0.1:6800, the default server)
    :param select:   (str) column to select (default: * all the columns)
    :param where:    (str) where condition for the select  (default: spider from 'retail_shake' project)
    :return:         (df)  query output as a pandas DataFrame
    """
    import re
    import mysql.connector
    from mysql.connector import connect, errorcode

    if not url:
        try:
            url = os.environ['DATABASE_URL']
            user, password = re.findall(r'(?<=//)(.*?)(?=@)', url)[0].split(':')
            hostname, port = re.findall(r'(?<=@)(.*?)$', url)[0].split(':')

            try:
                connect(
                    hostname=hostname,
                    port=port,
                    database=database,
                    user=user,
                    password=password,
                )
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                else:
                    print(err)

        except KeyError:
            logging.Logger('DB url not found in environment variable!')

# TODO #2 @h4r1c0t: multinode request -> get the current node and the corresponding server.


def sqlite_to_df(
    path=None,
    database="jobs.db",
    table="127_0_0_1_6800",  # 'all' argument to JOIN request to all the scrapyd server
    select="*",
    where="project = 'retail_shake'",
):
    """
    This function is used to automatically get data from scrapyd SQLite DB.
    By default get spyder data from 127.0.0.1:6800 table of the jobs.db

    :param path:    (str) path to the *.db file (default: local pathway to jobs.db)
    :param database: (str) the database to select
    :param table:   (str) table name (default: 127.0.0.1:6800, the default server)
    :param select:  (str) column to select (default: * all the columns)
    :param where:   (str) where condition for the select  (default: spider from 'retail_shake' project)
    :return:        (df)  query output as a pandas DataFrame
    """
    # | import section |
    import sqlite3
    import pandas as pd

    # | code section |
    # db connect
    if not path:
        from scrapydweb_settings_v10 import DATABASE_URL

        path = DATABASE_URL + "/" + database
        path = path.replace("sqlite:///", "")  # !!! > mysql / postgre options
    con = sqlite3.connect(path)

    # import data
    df = pd.read_sql(
        f"""
        SELECT {select}
        FROM '{table}'
        WHERE {where}
        """,
        con,
    )

    # create date format and sort
    df.start = pd.to_datetime(df.start)
    df["start_date"] = df.start.dt.date
    df = df.sort_values(by="start_date")

    df["items"] = df["items"].fillna(0)
    df["pages"] = df["pages"].fillna(0)

    return df


def select_spider(dataframe, spider_name):
    """
    This function is used to extract a specific spider from the DataFrame obtains with sqlite_to_df() function.

    :param dataframe:       (df)  the DataFrame obtains with sqlite_to_df() function
    :param spider_name:     (str) the spider name to extract
    :return:                (df)  a DataFrame with the data corresponding to the spider selected
                            and means items and pages
    """
    # | code section |
    # get specific column of the dataframe for the specific spider name
    data = dataframe[["spider", "start_date", "items", "pages"]][
        dataframe["spider"] == spider_name
    ]
    # TODO : See witch *'fill method'* is the best for dates
    data = data.fillna(method="ffill").sort_values(by="start_date")

    # automatically compute the floating means
    data = mtm.compute_floating_means(data, "items", 7)
    data = mtm.compute_floating_means(data, "pages", 7)

    return data
