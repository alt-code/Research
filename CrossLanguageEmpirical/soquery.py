"""
This script contains functions to query the sotorrent-org.2018_12_09.Posts dump.

NOTE: Please refer to the following instructions installing the Python package
for BigQuery for Python and for setting up BigQuery api token as an environment
variable before running script: 

https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python

"""

from google.cloud import bigquery
import pandas as pd

##################################

# Languages of interest
# source: https://erikbern.com/2017/03/15/the-eigenvector-of-why-we-moved-from-language-x-to-language-y.html

langs = ["java", "c", "c++", "c#", "python", "visual basic", "node", 
    "perl", "php", "ruby", "go", "swift", "objective c", "cobol", "fortran", 
    "lua", "scala", "lisp", "haskell", "rust", "erlang", "clojure", "matlab", 
    "pascal", "r", "kotlin"]

# some languages require the right tag text, this dict maps to the proper text
tag_langs = {
    'visual basic': 'vb.net',
    'node': 'node.js',
    'objective c': 'objective-c',
}
langs.sort()

DIR = '/Volumes/TarDisk/soposts2'

##################################

# Utility functions for language text or language filename

def fix_lang_text(lang):
    if lang == 'c':
        return r"/\bin c\b/"
    elif lang == 'r':
        return r"/\bin r\b/"
    elif lang == 'go':
        return r"/\bin go\b/"
    else:
        return r"/\b"+lang+r"\b/"

def lang_file_name(lang):
    if lang == 'c#':
        return 'cs'
    elif lang == 'c++':
        return 'cpp'
    elif lang == 'objective c':
        return 'objectivec'
    elif lang == 'visual basic':
        return 'visualbasic'
    return lang

def revert_lang_file_name(lang):
    if lang == 'cs':
        return 'c#'
    elif lang == 'cpp':
        return 'c++'
    elif lang == 'objectivec':
        return 'objective c'
    elif lang == 'visualbasic':
        return 'visual basic'
    return lang

##################################

# Functions for querying from SO

def query_language_pair(a, b, path='./'):
    """
    Queries so torrent posts that are:
    - taggged with a and b OR tagged with a and has b in the title/body

    Saves the csv file locally on current path (default) or given path.

    Arguments:
    a -- the "source" language
    b -- the "target" language
    path -- the pathname to save the csv file to
    """
    # https://cloud.google.com/bigquery/docs/parameterized-queries#bigquery-query-params-python
    btext = fix_lang_text(b)
    atag = "%<" + tag_langs[a] + ">%" if a in tag_langs else "%<" + a + ">%"
    btag = "%<" + tag_langs[b] + ">%" if b in tag_langs else "%<" + b + ">%"
    lang_no_space = lang_file_name(b).replace(' ','')
    query = \
        f'''
        SELECT CONCAT('http://stackoverflow.com/q/', Cast(P.Id as string)) as URL, P.Title, P.ViewCount, P.Score, P.AcceptedAnswerId, P.Tags, P.Body
        FROM `sotorrent-org.2018_12_09.Posts` P
        WHERE P.PostTypeId = 1 AND (
        (P.Tags LIKE '{atag}' AND P.Tags LIKE '{btag}')
        OR
        (P.Tags LIKE '{atag}' AND (P.Title LIKE '{btext}' OR P.Body LIKE '{btext}'))
        )
        AND
        P.Score >= 0
        AND
        P.AcceptedAnswerId IS NOT NULL
        '''
    client = bigquery.Client()
    print(f'Querying posts for pair <{a}, {b}>...')
    df = client.query(
        query,
        location='US').to_dataframe()  # API request - starts the query
    a_filename = lang_file_name(a)
    b_filename = lang_file_name(b)
    print('Saving...')
    df.to_csv(f'{path}/{a_filename}_{b_filename}.csv', index=False)

def build_sql_expr(a):
    """
    Builds and chains the SELECT queries

    Arguments:
    a -- the source/previous language
    """
    sql_expr = """SELECT * FROM\n\n"""
    # in this instance we only set atag once as we're writing a query for
    # <a, b> where b is all the other target languages
    atag = "%<" + a + ">%"
    if a in tag_langs:
        atag = "%<" + tag_langs[a] + ">%"
    for i, l in enumerate(langs):
        btext = fix_lang_text(l)
        btag = "%<" + l + ">%"
        if l in tag_langs:
            btag = "%<" + tag_langs[l] + ">%"
        lang_no_space = lang_file_name(l).replace(' ','')
        if l != a:
            sql_expr += \
            f'''
            (SELECT COUNT(distinct P.Id) AS {lang_no_space}
            FROM `sotorrent-org.2018_12_09.Posts` P
            WHERE P.PostTypeId = 1 AND (
            (P.Tags LIKE '{atag}' AND P.Tags LIKE '{btag}')
            OR
            (P.Tags LIKE '{atag}' AND (P.Title LIKE '{btext}' OR P.Body LIKE '{btext}'))
            )
            AND
            P.Score >= 0) AS {lang_no_space}'''
            # since visual basic is the last in the list, this makes sure that 
            # we don't add a comma and \n after the swift SELECT
            if a == 'visual basic':
                if l == 'swift':
                    continue
            if i != len(langs) - 1:
                sql_expr += ',\n'
    return sql_expr

def query_all():
    """
    Queries all pairs and downloads a csv containing counts for each pair
    in a folder called langcounts in current directory
    """
    for l in langs:
        print(f'Querying for {l}...')
        sql_expr = build_sql_expr(l)
        client = bigquery.Client()
        # API request - starts the query and converts to df
        df = client.query(sql_expr).to_dataframe()
        csv_name = lang_file_name(l)
        # TODO don't hardcode the root path 
        df.to_csv(f'langcounts/{csv_name}.csv', index=False)

def query_pairs():
    '''
    Query sepecific language pairs 
    
    
    These are based on pairs resulting from our stop rule criteria in paper:

    c	cpp
    cs	visualbasic
    clojure	java
    java	cs
    kotlin	java
    lua	cpp
    matlab	python
    node	php
    objectivec	swift
    perl	python
    php	java
    python	cpp
    r	python
    ruby	python
    scala	java
    '''
    # TODO have a way to pass in a list of previous to target languages instead
    # of hardcoding.
    prev = ['c', 'cs', 'clojure', 'java', 'kotlin', 'lua', 'matlab', 'node',
       'objectivec', 'perl', 'php', 'python', 'r', 'ruby', 'scala']
    target = ['cpp', 'visualbasic', 'java', 'cs', 'java', 'cpp', 'python', 'php',
       'swift', 'python', 'java', 'cpp', 'python', 'python', 'java']
    for p, t in zip(prev, target):
        p = revert_lang_file_name(p)
        t = revert_lang_file_name(t)
        print(f'Saving posts for <{p}, {t}>...')
        query_language_pair(p, t, path=DIR)

if __name__ == '__main__':
    """
    TODO accept arguments for either querying all combinations or specific
    language pairs
    """
    # or query for posts
    # Uncomment this to query SO posts for languages in lang
    # query_all()
    # Uncomment this to query SO posts for specific language pairs in lang
    # query_pairs()
    