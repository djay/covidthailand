# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 16:42:08 2020

@author: FelixKling
"""
import datetime as dt
import html
import re
import zipfile

import numpy as np
import pandas as pd
from lxml import etree


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def read(path, sheet_name=None, header=True, index_col=False, skiprows=[], skipcolumns=[]):
    """
    Reads an .xlsx or .xlsm file and returns a Pandas DataFrame. Is much faster than pandas.read_excel().

    Parameters
    ----------
    path : str
        The path to the .xlsx or .xlsm file.
    sheet_name : str, optional
        Name of the sheet to read. If none, the first (not the active!) sheet is read. The default is None.
    header : bool, optional
        Whether to use the first line as column headers. The default is True.
    index_col : bool, optional
        Whether to use the first column as index. The default is False.
    skiprows : list of int, optional.
        The row numbers to skip ([0, 1] skips the first two rows). The default is [].
    skipcolumns : list of int, optional.
        The column numbers to skip ([0, 1] skips the first two columns). The default is [].

    Raises
    ------
    TypeError
        If the file is no .xlsx or .xlsm file.
    FileNotFoundError
        If the sheet name is not found.

    Returns
    -------
    Pandas DataFrame
        The input file as DataFrame.

    """
    # check extension
    if "." not in path:
        raise TypeError("This is no .xlsx or .xlsm file!")
    if path.rsplit(".", 1)[1] not in ["xlsx", "xlsm"]:
        raise TypeError("This is no .xlsx or .xlsm file!")

    path = path.replace("\\", "/")

    tempfiles = dict()
    with zipfile.ZipFile(path, 'r') as zipObj:
        for name in zipObj.namelist():
            if name.startswith("xl/worksheets/") or name in [
                    "xl/_rels/workbook.xml.rels",
                    "xl/styles.xml",
                    "xl/workbook.xml",
                    "xl/sharedStrings.xml",
            ]:
                try:
                    tempfiles[name] = zipObj.read(name).decode("utf-8")
                except UnicodeDecodeError:
                    tempfiles[name] = zipObj.read(name).decode("utf-16")

    # read rels (paths to sheets)

    text = tempfiles["xl/_rels/workbook.xml.rels"]
    rels = {}

    relids = re.findall(r'<Relationship Id="([^"]+)"', text)
    relpaths = re.findall(r'<Relationship .*?Target="([^"]+)"', text)
    rels = dict(zip(relids, relpaths))

    # read sheet names and relation ids

    if sheet_name:
        text = tempfiles["xl/workbook.xml"]
        workbooks = {}

        workbookids = re.findall(r'<sheet.*? r:id="([^"]+)"', text)
        workbooknames = re.findall(r'<sheet.*? name="([^"]+)"', text)
        workbooks = dict(zip(workbooknames, workbookids))
        if sheet_name in workbooks:
            sheet = rels[workbooks[sheet_name]].rsplit("/", 1)[1]
        else:
            raise FileNotFoundError("Sheet " + str(sheet_name) +
                                    " not found in Excel file! Available sheets: " + "; ".join(workbooks.keys()))

    else:
        sheet = "sheet1.xml"

    # read strings, they are numbered
    string_items = []
    if "xl/sharedStrings.xml" in tempfiles:
        text = tempfiles["xl/sharedStrings.xml"]

        string_items = re.split(r"<si.*?><t.*?>", text.replace("<t/>",
                                "<t></t>").replace("</t></si>", "").replace("</sst>", ""))[1:]
        string_items = [html.unescape(str(i).split("</t>")[0]) if i != "" else np.nan for i in string_items]

    # read styles, they are numbered

    text = tempfiles["xl/styles.xml"]
    styles = re.split(r"<[/]?cellXfs.*?>", text)[1]
    styles = styles.split('numFmtId="')[1:]
    styles = [int(s.split('"', 1)[0]) for s in styles]

    # numfmts = text.split("<numFmt ")[1:]
    # numfmts = [n.split("/>", 1)[0] for n in numfmts]
    # for i, n in enumerate(numfmts):
    #     n = re.sub(r"\[[^\]]*\]", "", n)
    #     n = re.sub(r'"[^"]*"', "", n)
    numfmts = {}
    q = etree.fromstring(bytes(text, "utf8"))
    for e in q.findall(".//{http://schemas.openxmlformats.org/spreadsheetml/2006/main}numFmt"):
        i = int(e.attrib['numFmtId'])
        n = e.attrib['formatCode']
        if any([x in n for x in ["y", "d", "w", "q"]]):
            numfmts[i] = "date"
        elif any([x in n for x in ["h", "s", "A", "P"]]):
            numfmts[i] = "time"
        else:
            numfmts[i] = "number"

    def style_type(x):
        if 14 <= x <= 22:
            return "date"
        if 45 <= x <= 47:
            return "time"
        if x >= 165:
            return numfmts[x]
        else:
            return "number"

    styles = list(map(style_type, styles))

    text = tempfiles["xl/worksheets/" + sheet]

    def code2nr(x):
        nr = 0
        d = 1
        for c in x[::-1]:
            nr += (ord(c) - 64) * d
            d *= 26
        return nr - 1

    table = []
    max_row_len = 0

    rows = [r.replace("</row>", "") for r in re.split(r"<row .*?>", text)[1:]]
    for r in rows:
        # c><c r="AT2" s="1" t="n"><v></v></c><c r="AU2" s="115" t="inlineStr"><is><t>bla (Namensk&#252;rzel)</t></is></c>

        r = re.sub(r"</?r.*?>", "", r)
        r = re.sub(r"<(is|si).*?><t.*?>", "<v>", r)
        r = re.sub(r"</t></(is|si)>", "</v>", r)
        r = re.sub(r"</t><t.*?>", "", r)

        values = r.split("</v>")[:-1]
        add = []
        colnr = 0
        for v in values:
            value = re.split("<v.*?>", v)[1]

            v = v.rsplit("<c", 1)[1]
            # get column number of the field
            nr = v.split(' r="')[1].split('"')[0]
            nr = code2nr("".join([n for n in nr if n.isalpha()]))
            if nr > colnr:
                for i in range(nr - colnr):
                    add.append(np.nan)
            colnr = nr + 1

            sty = "number"
            if ' s="' in v:
                sty = int(v.split(' s="', 1)[1].split('"', 1)[0])
                sty = styles[sty]

            # inline strings
            if 't="inlineStr"' in v:
                add.append(html.unescape(value) if value != "" else np.nan)
            # string from list
            elif 't="s"' in v:
                add.append(string_items[int(value)])
            # boolean
            elif 't="b"' in v:
                add.append(bool(int(value)))
            # date
            elif sty == "date":
                if len(value) == 0:
                    add.append(pd.NaT)
                # Texts like errors
                elif not is_number(value):
                    add.append(html.unescape(value))
                else:
                    add.append(dt.datetime(1900, 1, 1) + dt.timedelta(days=float(value) - 2))
            # time
            elif sty == "time":
                if len(value) == 0:
                    add.append(pd.NaT)
                # Texts like errors
                elif not is_number(value):
                    add.append(html.unescape(value))
                else:
                    add.append((dt.datetime(1900, 1, 1) + dt.timedelta(days=float(value) - 2)).time())
            # Null
            elif len(value) == 0:
                add.append(np.nan)
            # Texts like errors
            elif not is_number(value):
                add.append(html.unescape(value))
            # numbers
            else:
                add.append(round(float(value), 16))
        table.append(add)
        if len(add) > max_row_len:
            max_row_len = len(add)

    df = pd.DataFrame(table)

    # skip rows or columns
    df = df.iloc[[i for i in range(len(df)) if i not in skiprows], [i for i in range(len(df.columns)) if i not in skipcolumns]]

    if index_col:
        df = df.set_index(df.columns[0])
    if header:
        df.columns = df.iloc[0].values
        df = df.iloc[1:]

    return df
