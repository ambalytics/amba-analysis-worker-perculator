from src import doi_resolver
import unittest


class TestDoiResolver(unittest.TestCase):

    def test_check_doi_list_valid(self):
        self.assertEqual(doi_resolver.check_doi_list_valid(['10.21105', '10.21105/joss.01556']), '10.21105/joss.01556')
        self.assertEqual(doi_resolver.check_doi_list_valid(['10.21105/joss.01556/123/123', 'joss.01556']), False)

    def test_crossref_url_search(self):
        self.assertEqual(doi_resolver.crossref_url_search('https://www.bmj.com/content/375/bmj.n2635'),
                         '10.1136/bmj.n263')
        self.assertEqual(doi_resolver.crossref_url_search('https://joss.theoj.org/papers/10.21105/joss.01556'), False)

    def test_get_potential_dois_from_text(self):
        text = "https://www.biorxiv.org/content/10.1101/2021.05.14.444134v1"
        self.assertEqual(doi_resolver.get_potential_dois_from_text(text), {None, '10.1101/2021.05.14.444134v1', '10.1101/2021.05.14.444134'})
        text = "https://arxiv.org/abs/2103.11251"
        self.assertEqual(doi_resolver.get_potential_dois_from_text(text), set())
        text = "https://academic.oup.com/glycob/advance-article-abstract/doi/10.1093/glycob/cwab035/6274761#.YKKxIEAvSvs.twitter" \
               "https://www.cochranelibrary.com/cdsr/doi/10.1002/14651858.CD013263.pub2/full" \
               "https://www.nejm.org/doi/full/10.1056/NEJMcibr2034927"
        self.assertEqual(doi_resolver.get_potential_dois_from_text(text), 'aaa')

    def test_link_url(self):
        url = "https://doi.org/10.1242/jeb.224485"
        self.assertEqual(doi_resolver.link_url(url), '10.1242/jeb.224485')
        url = "http://dx.doi.org/10.1016/j.redox.2021.101988"
        self.assertEqual(doi_resolver.link_url(url), '10.1016/j.redox.2021.101988')
        url = "https://www.emerald.com/insight/content/doi/10.1108/INTR-01-2020-0038/full/html"
        self.assertEqual(doi_resolver.link_url(url), '10.1108/INTR-01-2020-0038')
        url = "https://www.sciencedirect.com/science/article/pii/S1934590921001594"
        self.assertEqual(doi_resolver.link_url(url), '10.1016/j.stem.2021.04.002')
        url = "https://link.springer.com/article/10.1007/s00467-021-05115-7"
        self.assertEqual(doi_resolver.link_url(url), '10.1007/s00467-021-05115-7')
        url = "https://onlinelibrary.wiley.com/doi/10.1111/andr.13003"
        self.assertEqual(doi_resolver.link_url(url), '10.1111/andr.13003')
        url = "https://www.nature.com/articles/s41398-021-01387-7"
        self.assertEqual(doi_resolver.link_url(url), '10.1038/s41398-021-01387-7')
        url = "https://science.sciencemag.org/content/372/6543/694.1.full"
        self.assertEqual(doi_resolver.link_url(url), '10.1126/science.abj0016')
        url = "https://journals.sagepub.com/doi/10.1177/00469580211005191"
        self.assertEqual(doi_resolver.link_url(url), '10.1177/00469580211005191')
        url = "https://journals.plos.org/ploscompbiol/article?id=10.1371/journal.pcbi.1008922"
        self.assertEqual(doi_resolver.link_url(url), '10.1371/journal.pcbi.1008922')
        url = "https://www.frontiersin.org/articles/10.3389/fnume.2021.671914/full"
        self.assertEqual(doi_resolver.link_url(url), '10.3389/fnume.2021.671914')
        url = "https://www.tandfonline.com/doi/full/10.1080/09638237.2021.1898552"
        self.assertEqual(doi_resolver.link_url(url), '10.1080/09638237.2021.1898552')
        url = "https://www.mdpi.com/2072-4292/13/10/1955"
        self.assertEqual(doi_resolver.link_url(url), '2072-4292/13/10/1955')
        url = "https://iopscience.iop.org/article/10.1088/1361-6528/abfee9/meta"
        self.assertEqual(doi_resolver.link_url(url), '10.1088/1361-6528/abfee9')
        url = "https://www.cochranelibrary.com/cdsr/doi/10.1002/14651858.CD013263.pub2/full"
        self.assertEqual(doi_resolver.link_url(url), '10.1002/14651858.CD013263')
        url = "https://www.nejm.org/doi/full/10.1056/NEJMcibr2034927"
        self.assertEqual(doi_resolver.link_url(url), '10.1056/NEJMcibr2034927')
        url = "https://www.thelancet.com/journals/eclinm/article/PIIS2589-5370(20)30464-8/fulltext"
        self.assertEqual(doi_resolver.link_url(url), '10.1016/j.eclinm.2020.100720')
        url = "https://www.bmj.com/content/373/bmj.n922"
        self.assertEqual(doi_resolver.link_url(url), '10.1136/bmj.n922')
        url = "https://www.pnas.org/content/117/48/30071"
        self.assertEqual(doi_resolver.link_url(url), '10.1073/pnas.1907375117')
        url = "https://jamanetwork.com/journals/jamaneurology/article-abstract/2780249"
        self.assertEqual(doi_resolver.link_url(url), '10.1001/jamaneurol.2021.1335')
        url = "https://www.acpjournals.org/doi/10.7326/G20-0087"
        self.assertEqual(doi_resolver.link_url(url), '10.7326/G20-0087')
        url = "https://n.neurology.org/content/96/19/e2414.abstract"
        self.assertEqual(doi_resolver.link_url(url), '10.1212/WNL.0000000000011883')
        url = "https://doi.apa.org/record/1988-31508-001"
        self.assertEqual(doi_resolver.link_url(url), '10.1037/0022-3514.54.6.1063')
        url = "https://ieeexplore.ieee.org/document/9430520"
        self.assertEqual(doi_resolver.link_url(url), '10.1109/TNSRE.2021.3080045')
        url = "https://dl.acm.org/doi/abs/10.1145/3411764.3445371"
        self.assertEqual(doi_resolver.link_url(url), '10.1145/3411764.3445371')
        url = "https://jmir.org/2021/5/e26618"
        self.assertEqual(doi_resolver.link_url(url), '10.2196/26618')
        url = "https://journals.aps.org/pra/abstract/10.1103/PhysRevA.103.053314"
        self.assertEqual(doi_resolver.link_url(url), '10.1103/PhysRevA.103.053314')
        url = "https://www.biorxiv.org/content/10.1101/2021.05.14.444134v1"
        self.assertEqual(doi_resolver.link_url(url), '10.1101/2021.05.14.444134')
        url = "https://arxiv.org/abs/2111.06913"
        self.assertEqual(doi_resolver.link_url(url), '10.1007/978-3-030-82681-9')
        url = "https://academic.oup.com/glycob/advance-article-abstract/doi/10.1093/glycob/cwab035/6274761#.YKKxIEAvSvs.twitter"
        self.assertEqual(doi_resolver.link_url(url), '10.1093/glycob/cwab035/6274761')
        url = "https://www.jmcc-online.com/article/S0022-2828(21)00101-2/fulltext"
        self.assertEqual(doi_resolver.link_url(url), '10.1016/j.yjmcc.2021.05.007')

        if __name__ == '__main__':
            unittest.main()
