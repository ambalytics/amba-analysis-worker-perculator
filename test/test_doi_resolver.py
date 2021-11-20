from ..src import doi_resolver

import mock
import unittest
import logging


class DoiResolverTestCase(unittest.TestCase):

    @mock.patch('mymodule.os')
    def test_doi_resolver(self):
        url = "https://jamanetwork.com/journals/jamainternalmedicine/article-abstract/623118"
        assert doi_resolver.link_url(url) == '123'




if __name__ == '__main__':
    unittest.main()