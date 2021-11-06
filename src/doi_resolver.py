"""Helper Class providing multiple static functions to extract a doi from a url """

import json
import logging
import re
import requests
import time

import urllib3
from lxml import html
from functools import lru_cache

from requests import Session
from urllib3.exceptions import ReadTimeoutError, SSLError, NewConnectionError


def check_doi_list_valid(potential_dois):
    """check if a list of potential dois are valid and if so return the valid doi

        Arguments:
            potential_dois: a list of dois that should be checked
    """
    pdoi = ''
    for doi in potential_dois:
        if doi is not None and doi:
            pdoi = pdoi + doi + ','
    pre = "http://doi.org/doiRA/"  # todo
    rw = False
    if pdoi != '':
        r = requests.get(pre + pdoi)
        json_response = r.json()
        for j in json_response:
            if 'RA' in j:
                rw = j['DOI']
    return rw


# /2.4 Case sensitivity
def crossref_url_search(url):
    """search the url in crossref eventdata to get a doi

        Arguments:
            url: the url to get a doi for
    """
    r = requests.get("https://api.eventdata.crossref.org/v1/events?rows=1&obj.url=" + url)
    if r.status_code == 200:
        json_response = r.json()
        if 'status' in json_response:
            if json_response['status'] == 'ok':
                # logging.debug(json_response)
                if 'message' in json_response:
                    if 'events' in json_response['message']:
                        for event in json_response['message']['events']:
                            return event['obj_id'][16:]  # https://doi.org/ -> 16
    return False


def get_potential_dois_from_text(text):
    """use multiple different regexes to get a list of potential dois,
        it uses a very generic regex first which only checks for the bare minimum start to the end of line,
        this result will than be searched for possible endings generating possible dois
        the result is a set and will likely contain unvalid dois

        Arguments:
            text: the text which should be checked
    """
    doi_re = re.compile("(10\\.\\d{4,9}(?:/|%2F|%2f)[^\\s]+)")
    last_slash = re.compile("^(10\\.\\d+/.*)/.*$")
    first_slash = re.compile("^(10\\.\\d+/.*?)/.*$")
    semicolon = re.compile("^(10\\.\\d+/.*);.*$")
    hashchar = re.compile("^(10\\.\\d+/.*?)#.*$")
    question_mark = re.compile("^(10\\.\\d+/.*?)\\?.*$")
    amp_mark = re.compile("^(10\\.\\d+/.*?)&.*$")
    v1 = re.compile("^(10\\.\\d+/.*)v1*$")  # biorxiv, make if v\\d+ (v and digit v1,v2,v3

    result = set([])

    if doi_re.search(text) is not None:
        temp_doi = doi_re.search(text).group()
        # logging.debug(doi_re.findall(ur))
        # logging.debug(temp_doi)
        result.add(temp_doi)
        result.add(get_dois_regex(last_slash, temp_doi))
        result.add(get_dois_regex(first_slash, temp_doi))
        result.add(get_dois_regex(semicolon, temp_doi))
        result.add(get_dois_regex(hashchar, temp_doi))
        result.add(get_dois_regex(question_mark, temp_doi))
        result.add(get_dois_regex(amp_mark, temp_doi))
        result.add(get_dois_regex(v1, temp_doi))

    return result


def get_dois_regex(regex, temp_doi):
    """find all occurrences of a given regex in a doi-string which ending is searched

        Arguments:
            regex: the regex to look for
            temp_doi: the doi to use
    """
    if regex.search(temp_doi) is not None:
        # logging.debug(regex.search(temp_doi).group())
        # logging.debug(regex.findall(temp_doi))
        return regex.findall(temp_doi)[0]


# def find_all(a_str, sub):
#     start = 0
#     while True:
#         start = a_str.find(sub, start)
#         if start == -1: return
#         yield start
#         start += len(sub)
# cache in case of duplicates
@lru_cache(maxsize=100)
def get_response(url, s):
    """get a response from a given url using a given session s, a session can be used for headers,
    this function is cached up to 100 elements

        Arguments:
            url: the url to get
            s: the session to use
    """
    try:
        result = s.get(url, timeout=5)
    except (ConnectionRefusedError, SSLError, ReadTimeoutError, requests.exceptions.TooManyRedirects, requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout, NewConnectionError, requests.exceptions.SSLError, ConnectionError):
        logging.warning('Perculator error, reset session')
        s = Session()
        # get the response for the provided url
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
            'Pragma': 'no-cache'
        }
        s.headers.update(headers)
    else:
        return result
    return None


def search_fulltext(r):
    """search the fulltext of an response

        Arguments:
            r: the response we want to search
    """
    return get_potential_dois_from_text(r.text)


def get_lxml(page):
    """use lxml to search for meta tags that could contain the doi

        Arguments:
            page: the page to search
    """
    content = html.fromstring(page.content)
    result = set([])
    for meta in content.xpath('//meta'):
        for name, value in sorted(meta.items()):
            if value.strip().lower() in ['citation_doi', 'dc.identifier', 'evt-doipage', 'news_doi', 'citation_doi']:
                result.add(meta.get('content'))
    return result


def get_filtered_dois_from_meta(potential_dois):
    """check potential dois from meta tags for dois to extract them in case they have a full url

        Arguments:
            potential_dois: the dois to filter
    """
    result = set([])
    for t in potential_dois:
        result.add(t.replace('doi:', ''))

    doi_re = re.compile("(10\\.\\d{4,9}(?:/|%2F|%2f)[^\\s]+)")

    r = set([])
    for t in result:
        if doi_re.search(t) is not None:
            r.add(t)
    return r


def url_doi_check(data):
    """check data for urls,
       get first url in urls,
       prefer expanded_url

        Arguments:
            data: data to get url from
    """
    doi_data = False

    if 'entities' in data:
        if 'urls' in data['entities']:
            for url in data['entities']['urls']:
                if doi_data is False and 'expanded_url' in url:
                    doi_data = link_url(url['expanded_url'])
                if doi_data is False and 'unwound_url' in url:
                    doi_data = link_url(url['unwound_url'])
            if doi_data is not False:
                return doi_data
    return doi_data


@lru_cache(maxsize=50000)
def link_url(url):
    """link a url to a valid doi,
    it will try to get potential dois using multiple regex and than check if their are valid and than return the doi
    it uses multiple methods to search for the doi,
    this function is cached up to 500 elements

        Arguments:
            url: the url to get
            s: the session to use
    """
    logging.debug(url)

    # check if the url contains the doi
    doi = check_doi_list_valid(get_potential_dois_from_text(url))
    if doi:
        logging.debug('url')
        return doi

    s = Session()
    # get the response for the provided url
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    s.headers.update(headers)
    r = get_response(url, s)

    # check if the doi is in any meta tag
    if r:
        pot_doi = get_lxml(r)
        doi = check_doi_list_valid(get_filtered_dois_from_meta(pot_doi))
        if doi and doi != set([]):
            logging.debug('meta')
            return doi

    # check if crossref knows this url and returns the doi
    doi = crossref_url_search(url)
    if doi:
        logging.debug('crossref')
        return doi

    if r:
        # do a fulltext search of the url
        doi = check_doi_list_valid(search_fulltext(r))
        if doi:
            logging.debug('fulltext')
            return doi

    return False


if __name__ == '__main__':
    # todo make it an actual test
    urls = [
        "https://jamanetwork.com/journals/jamainternalmedicine/article-abstract/623118",
        "https://jamanetwork.com/journals/jamainternalmedicine/article-abstract/623118",
        "https://jamanetwork.com/journals/jamainternalmedicine/article-abstract/623118",
        "https://doi.org/10.1242/jeb.224485",
        "https://doi.org/10.1242/jeb.224485",
        "https://doi.org/10.1242/jeb.224485",
        "http://dx.doi.org/10.1016/j.redox.2021.101988",
        "https://www.emerald.com/insight/content/doi/10.1108/INTR-01-2020-0038/full/html",
        "https://www.sciencedirect.com/science/article/pii/S1934590921001594",
        "https://www.degruyter.com/document/doi/10.7208/9780226733050/html",
        "https://link.springer.com/article/10.1007/s00467-021-05115-7",
        "https://onlinelibrary.wiley.com/doi/10.1111/andr.13003",
        "https://www.nature.com/articles/s41398-021-01387-7",
        "https://science.sciencemag.org/content/372/6543/694.1.full",
        "https://journals.sagepub.com/doi/10.1177/00469580211005191",
        "https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1008922",
        "https://www.frontiersin.org/articles/10.3389/fnume.2021.671914/full",
        "https://www.tandfonline.com/doi/full/10.1080/09638237.2021.1898552",
        "https://www.mdpi.com/2072-4292/13/10/1955",
        "https://iopscience.iop.org/article/10.1088/1361-6528/abfee9/meta",
        "https://www.cochranelibrary.com/cdsr/doi/10.1002/14651858.CD013263.pub2/full",
        "https://www.nejm.org/doi/full/10.1056/NEJMcibr2034927",
        "https://www.thelancet.com/journals/eclinm/article/PIIS2589-5370(20)30464-8/fulltext",
        "https://www.bmj.com/content/373/bmj.n922",
        "https://www.pnas.org/content/117/48/30071",
        "https://jamanetwork.com/journals/jamaneurology/article-abstract/2780249",
        "https://www.acpjournals.org/doi/10.7326/G20-0087",
        "https://n.neurology.org/content/96/19/e2414.abstract",
        "https://doi.apa.org/record/1988-31508-001",
        "https://ieeexplore.ieee.org/document/9430520",
        "https://dl.acm.org/doi/abs/10.1145/3411764.3445371",
        "https://jmir.org/2021/5/e26618",
        "https://journals.aps.org/pra/abstract/10.1103/PhysRevA.103.053314",
        "https://www.biorxiv.org/content/10.1101/2021.05.14.444134v1",
        "https://arxiv.org/abs/2103.11251",
        "https://academic.oup.com/glycob/advance-article-abstract/doi/10.1093/glycob/cwab035/6274761#.YKKxIEAvSvs.twitter",
        "https://www.jmcc-online.com/article/S0022-2828(21)00101-2/fulltext"
    ]
    logging.debug("start")
    start = time.time()
    r = set([])
    for url in urls:
        r.add(link_url(url))
        logging.debug(link_url(url))
    # logging.debug(sorted(r))
    logging.debug("total link: " + str(time.time() - start))
