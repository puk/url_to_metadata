import pickle
import re
from typing import Optional, Dict, List

import pandas as pd
import requests
from shucks.snowflake import SnowflakeConfig
from tqdm.notebook import tqdm

SSQL = SnowflakeConfig.load(section_name="edp")


names = dict(
    links="link_lookup.pkl",
    regex_str=r"(978[0-9]{10})|(B[A-Z0-9]{9})|([0-9]{9}[A-Z0-9]{1})",
    q="""
    SELECT DISTINCT
    
            ISBN_13,
            ASIN,
            TITLE as "title",
            AUTHOR as "author",
            ORIGINAL_PUBLICATION_DATE as "pub_date",
            NIELSEN_CATEGORY as "genre",
            NIELSEN_SUB_CATEGORY as "subgenre",
            DIVISION as "division"
            
    FROM    "EDP_PROD"."INFO_CORE"."TITLE_INFO_DIM"
    
    WHERE   DIVISION not in ('PRH Other','PRH Corporate')
    AND     {} in {}
    """,
)


def get_long_links(link_list: List, names: Dict[str, str]) -> List:
    """
    Iterate over the links in the list. For each link,
    check if it is in the link dictionary already. If not,
    look it up online. Then update the lookup and re-save.
    """
    try:
        with open(names["links"], "rb") as handle:
            link_dict = pickle.load(handle)
    except FileNotFoundError:
        link_dict = {}

    longlink_list = []
    for link in tqdm(link_list):
        try:
            longlink = link_dict[link]
            longlink_list.append((link, longlink))
        except KeyError:
            try:
                longlink = requests.head(link, allow_redirects=True, timeout=5).url
                longlink_list.append((link, longlink))
            except:
                continue

    new_link_dict = {a: b for (a, b) in longlink_list}
    link_dict.update(new_link_dict)

    with open(names["links"], "wb") as handle:
        pickle.dump(link_dict, handle)

    print(f"""Number of input links: {len(link_list)}""")
    print(f"""Valid long links:      {len(longlink_list)}""")

    return longlink_list
    

def get_asinisbns(longlink_list: List, names: Dict[str, str]) -> List:
    """
    Takes in a list of tuples (link, long_link)
    Returns a list of tuples (link, isbn/asin)
    """

    exp = names["regex_str"]
    asinisbn_list = []

    for (link, longlink) in longlink_list:
        match = re.search(exp, longlink)
        try:
            asinisbn_list.append((link, match[0]))
        except IndexError:
            continue

    print(f"""Number of ISBNs/ASINs: {len(asinisbn_list)}""")
    return asinisbn_list


def get_inputs(input_list: List, names: Dict[str, str]) -> Dict[str, List]:
    """
    Takes in a list of tuples (link, asin/isbn)
    Returns a dict with a list of asins and a list of isbns
    """

    asin_input = []
    isbn_input = []

    for (link, asinisbn) in input_list:
        if len(asinisbn) == 10:
            asin_input.append((link, asinisbn))
        elif len(asinisbn) == 13:
            isbn_input.append((link, asinisbn))
        else:
            continue

    return dict(asin=asin_input, isbn=isbn_input)


def get_metadata(
    names: Dict[str, str],
    conn,
    id_tuple: List[tuple],
    id_type: str,
) -> pd.DataFrame():

    id_types = ["ASIN", "ISBN_13"]
    if id_type not in id_types:
        raise ValueError("expected list_type to be %s" % id_types)

    id_input = tuple([b for a, b in id_tuple])
    query = names["q"].format(id_type, id_input)

    _avalon = pd.read_sql(query, conn)
    metadata_df = pd.DataFrame(data=id_tuple, columns=["link", id_type])

    return metadata_df.merge(_avalon, on=id_type)


def get(link_list: List, names: Dict[str, str] = names) -> pd.DataFrame():

    l1 = get_long_links(link_list, names)
    l2 = get_asinisbns(l1, names)
    m = get_inputs(l2, names)

    df_asin = pd.DataFrame()
    df_isbn = pd.DataFrame()

    SSQL = SnowflakeConfig.load(section_name="edp")

    with SSQL.connect() as conn:
        if len(m["asin"]) > 0:
            df_asin = get_metadata(names, conn, m["asin"], "ASIN")
        if len(m["isbn"]) > 0:
            df_isbn = get_metadata(names, conn, m["isbn"], "ISBN_13")

    final_df = pd.concat([df_asin, df_isbn])

    print(f"""Number of records:     {len(final_df)}""")

    return final_df