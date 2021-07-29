import json
import re
import requests
import time
from lxml import html
from functools import lru_cache

# todo real cache
from requests import Session


def check_doi_list_valid(potential_dois):
    pdoi = ''
    for doi in potential_dois:
        if doi is not None and doi:
            pdoi = pdoi + doi + ','
    pre = "http://doi.org/doiRA/" # todo
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
    r = requests.get("https://api.eventdata.crossref.org/v1/events?rows=1&obj.url=" + url)
    if r.status_code == 200:
        json_response = r.json()
        if 'status' in json_response:
            if json_response['status'] == 'ok':
                # print(json_response)
                if 'message' in json_response:
                    if 'events' in json_response['message']:
                        for event in json_response['message']['events']:
                            return event['obj_id']
    return False


def get_potential_dois_from_text(text):
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
        # print(doi_re.findall(ur))
        # print(temp_doi)
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
    if regex.search(temp_doi) is not None:
        # print(regex.search(temp_doi).group())
        # print(regex.findall(temp_doi))
        return regex.findall(temp_doi)[0]


def find_all(a_str, sub):
    start = 0
    while True:
        start = a_str.find(sub, start)
        if start == -1: return
        yield start
        start += len(sub)


def get_response(url, s):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    s.headers.update(headers)
    return s.get(url)


def search_fulltext(r):
    return get_potential_dois_from_text(r.text)


def get_lxml(page):
    content = html.fromstring(page.content)
    result = set([])
    for meta in content.xpath('//html//head//meta'):
        for name, value in sorted(meta.items()):
            # print(name)
            if value.strip().lower() in ['citation_doi', 'dc.identifier', 'evt-doipage']:
                # print(meta.get('content'))
                result.add(meta.get('content'))
    return result


def get_filtered_dois_from_meta(potential_dois):
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


@lru_cache(maxsize=100)
def link_url(url):
    print(url)

    # url
    doi = check_doi_list_valid(get_potential_dois_from_text(url))
    if doi:
        print('url')
        return doi

    s = Session()
    r = get_response(url, s)

    # meta
    pot_doi = get_lxml(r)
    doi = check_doi_list_valid(get_filtered_dois_from_meta(pot_doi))
    if doi and doi != set([]):
        print('meta')
        return doi

    # crossref
    doi = crossref_url_search(url)
    if doi:
        print('crossref')
        return doi

    # fulltext
    doi = check_doi_list_valid(search_fulltext(r))
    if doi:
        print('fulltext')
        return doi

    return False


if __name__ == '__main__':
    urls = [
        "https://jamanetwork.com/journals/jamainternalmedicine/article-abstract/623118",
        "https://jamanetwork.com/journals/jamainternalmedicine/article-abstract/623118",
        "https://jamanetwork.com/journals/jamainternalmedicine/article-abstract/623118"]

    #     "https://doi.org/10.1242/jeb.224485",
    #     "https://doi.org/10.1242/jeb.224485",
    #     "https://doi.org/10.1242/jeb.224485",
    #     "http://dx.doi.org/10.1016/j.redox.2021.101988",
    #     "https://www.emerald.com/insight/content/doi/10.1108/INTR-01-2020-0038/full/html",
    #     "https://www.sciencedirect.com/science/article/pii/S1934590921001594",
    #     "https://www.degruyter.com/document/doi/10.7208/9780226733050/html",
    #     "https://link.springer.com/article/10.1007/s00467-021-05115-7",
    #     "https://onlinelibrary.wiley.com/doi/10.1111/andr.13003",
    #     "https://www.nature.com/articles/s41398-021-01387-7",
    #     "https://science.sciencemag.org/content/372/6543/694.1.full",
    #     "https://journals.sagepub.com/doi/10.1177/00469580211005191",
    #     "https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1008922",
    #     "https://www.frontiersin.org/articles/10.3389/fnume.2021.671914/full",
    #     "https://www.tandfonline.com/doi/full/10.1080/09638237.2021.1898552",
    #     "https://www.mdpi.com/2072-4292/13/10/1955",
    #     "https://iopscience.iop.org/article/10.1088/1361-6528/abfee9/meta",
    #     "https://www.cochranelibrary.com/cdsr/doi/10.1002/14651858.CD013263.pub2/full",
    #     "https://www.nejm.org/doi/full/10.1056/NEJMcibr2034927",
    #     "https://www.thelancet.com/journals/eclinm/article/PIIS2589-5370(20)30464-8/fulltext",
    #     "https://www.bmj.com/content/373/bmj.n922",
    #     "https://www.pnas.org/content/117/48/30071",
    #     "https://jamanetwork.com/journals/jamaneurology/article-abstract/2780249",
    #     "https://www.acpjournals.org/doi/10.7326/G20-0087",
    #     "https://n.neurology.org/content/96/19/e2414.abstract",
    #     "https://doi.apa.org/record/1988-31508-001",
    #     "https://ieeexplore.ieee.org/document/9430520",
    #     "https://dl.acm.org/doi/abs/10.1145/3411764.3445371",
    #     "https://jmir.org/2021/5/e26618",
    #     "https://journals.aps.org/pra/abstract/10.1103/PhysRevA.103.053314",
    #     "https://www.biorxiv.org/content/10.1101/2021.05.14.444134v1",
    #     "https://arxiv.org/abs/2103.11251",
    #     "https://academic.oup.com/glycob/advance-article-abstract/doi/10.1093/glycob/cwab035/6274761#.YKKxIEAvSvs.twitter",
    #     "https://www.jmcc-online.com/article/S0022-2828(21)00101-2/fulltext"
    # ]
    print("start")
    start = time.time()
    r = set([])
    for url in urls:
        r.add(link_url(url))
        print(link_url(url))
    # print(sorted(r))
    print("total link: " + str(time.time() - start))
