import unittest
from solrpage.page import Page
from urllib.parse import urlparse
import datetime
import collections
import json
import asyncio

class Page(Page):
    storage={}

    @classmethod
    def get_fields_by_response(cls, response):
        try:
            loc_source = urlparse(response.url)
        except Exception as e:
            return {}
        if loc_source.netloc.endswith("ycombinator.com"):
            return {
                "text_keep" : ["css:tr.athing"],
                "link_keep" : [{'css:tr.athing': {"target":"css:a.storylink\href", "title":"css:a.storylink"}}],
                "source_weight" : 42,
                "target_weight" : 42,
            }

    @classmethod
    async def add(cls, session, core, records):
        cls.storage.setdefault("records",[]).append(records)

    @classmethod
    def syncpost(cls,*args, **kwargs):
        cls.storage.setdefault("json",[]).append(kwargs.pop("json",{}))

class YcomResponse:
    url = "http://ycombinator.com"
    text = "<html><body><tr class=\"athing\">Hello, <a class=\"storylink\" href=\"http://example.com\">Mulligan</a></tr></body></html>"


class Tests(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        Page.storage={}

    def testflatten(self):
        self.assertEqual(
            Page.flatten({"id": 1, "other": {"id": 2}}),
            {"id": 1, "other.id": 2}
        )

    def testschema(self):
        Page.schema(
            record={
                "source.netloc":"",
                "source.path":"",
                "source.query":"",
                "weight.something":"",
                "when.something":"",
            },
            rectype="me",
        )
        posted = Page.storage["json"]
        self.assertEqual(posted,[
          {'add-field-type': {'analyzer': {'tokenizer': {'class': 'solr.SimplePatternTokenizerFactory',
                                                         'pattern': '[^.:]+'}},
                              'class': 'solr.TextField',
                              'name': 'hostnames',
                              'positionIncrementGap': '100'}},
          {'add-field-type': {'analyzer': {'tokenizer': {'class': 'solr.SimplePatternTokenizerFactory',
                                                         'pattern': '[^/]+'}},
                              'class': 'solr.TextField',
                              'name': 'paths',
                              'positionIncrementGap': '100'}},
          {'add-field-type': {'analyzer': {'tokenizer': {'class': 'solr.SimplePatternTokenizerFactory',
                                                         'pattern': '[^&]+'}},
                              'class': 'solr.TextField',
                              'name': 'querystring',
                              'positionIncrementGap': '100'}},
          {'add-field': {'name': 'source.netloc',
                         'stored': True,
                         'type': 'hostnames'}},
          {'add-field': {'name': 'source.path',
                         'stored': True,
                         'type': 'paths'}},
          {'add-field': {'name': 'source.query',
                         'stored': True,
                         'type': 'querystring'}},
          {'add-field': {'name': 'weight.something',
                         'stored': True,
                         'type': 'pints'}},
          {'add-field': {'name': 'when.something',
                         'stored': True,
                         'type': 'pdates'}}
        ])

    def testsave(self):
        p = Page.fromresponse(YcomResponse())
        p.now = datetime.datetime(2019,1,1,10,0,0)
        asyncio.new_event_loop().run_until_complete(p.save(None))
        posted = Page.storage["records"]
        self.assertEqual([[{
            'id': '43f3e1132ca600be134f52eff3d7865d53646d28',
            'id.source': ['ff50c40c1ee184a2f5264d05618ae6c9ae813807'],
            'id.source.date': '43f3e1132ca600be134f52eff3d7865d53646d28',
            'source.fragment': '',
            'source.netloc': 'ycombinator.com',
            'source.params': '',
            'source.path': '',
            'source.query': '',
            'source.scheme': 'http',
            'structure': 'body tr a',
            'text': ['Hello, Mulligan'],
            'weight.source': 42,
            'when.date': '2019-01-01T00:00:00z',
            'when.retrieved': '2019-01-01T10:00:00z'
        }], [{
            'id': '4c7a034e3d5a99eb549924687315c6f9b06deb16',
            'id.source': ['ff50c40c1ee184a2f5264d05618ae6c9ae813807'],
            'id.source.date': '43f3e1132ca600be134f52eff3d7865d53646d28',
            'id.source.target.date': '4c7a034e3d5a99eb549924687315c6f9b06deb16',
            'id.target': '47014b13456d9554edd0cf4567c07059ea1c7837',
            'source.fragment': '',
            'source.netloc': 'ycombinator.com',
            'source.params': '',
            'source.path': '',
            'source.query': '',
            'source.scheme': 'http',
            'target.fragment': '',
            'target.netloc': 'example.com',
            'target.params': '',
            'target.path': '',
            'target.query': '',
            'target.scheme': 'http',
            'author': [''],
            'text': [''],
            'title': ['Mulligan'],
            'weight.source': 42,
            'weight.target': 42,
            'when.date': '2019-01-01T00:00:00z',
            'when.retrieved': '2019-01-01T10:00:00z'
        }]], posted)
