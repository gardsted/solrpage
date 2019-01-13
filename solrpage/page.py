import requests
import asyncio
import aiohttp
import datetime
import json
from jaypage.page import Page
import collections


class Page(Page):
    solrurl = "http://example.com"
    schemas = {"text":{},"link":{}}

    @classmethod
    def post(cls, *args, **kwargs):
        # override when testing
        return requests.post(*args, **kwargs)

    @classmethod
    def schematypes(cls, rectype, record):
        # post field types
        for fieldtype in [{
            "name": "hostnames",
            "class": "solr.TextField",
            "positionIncrementGap": "100",
            "analyzer": {
                "tokenizer": {
                    "class": "solr.SimplePatternTokenizerFactory",
                    "pattern": "[^.:]+",
                }
            },
        },{
            "name": "paths",
            "class": "solr.TextField",
            "positionIncrementGap": "100",
            "analyzer": {
                "tokenizer": {
                    "class": "solr.SimplePatternTokenizerFactory",
                    "pattern": "[^/]+",
                }
            },
        },{
            "name": "querystring",
            "class": "solr.TextField",
            "positionIncrementGap": "100",
            "analyzer": {
                "tokenizer": {
                    "class": "solr.SimplePatternTokenizerFactory",
                    "pattern": "[^&]+",
                }
            },
        }]:
            res = cls.post(
                cls.solrurl+"/solr/" + rectype + "/schema",
                json={"add-field-type": fieldtype})

    @classmethod
    def schema(cls, rectype, record):
        cls.schematypes(rectype, record)
        # post fields
        s = cls.schemas[rectype]
        for k,v in record.items():
            s[k] = field = { "name": k, "stored": True, "type": "text_general"}
            if k.startswith("weight."):
                field["type"] = "pints"
            if k.startswith("when."):
                field["type"] = "pdates"
            elif k.endswith(".netloc"):
                field["type"] = "hostnames"
            elif k.endswith(".path"):
                field["type"] = "paths"
            elif k.endswith(".query"):
                field["type"] = "querystring"
            res = cls.post(
                cls.solrurl+"/solr/" + rectype + "/schema",
                json={"add-field": field})

    @classmethod
    def flatten(cls, d, parent_key='', sep='.', rectype=""):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(cls.flatten(v, new_key, sep=sep).items())
            elif isinstance(v, datetime.date):
                items.append((new_key, v.strftime("%Y-%m-%dT%H:%M:%Sz")))
            elif isinstance(v, datetime.datetime):
                items.append((new_key, v.strftime("%Y-%m-%dT%H:%M:%Sz")))
            else:
                items.append((new_key, v))
        retdict = dict(items)
        if rectype and set(retdict) != set(cls.schemas[rectype]):
            cls.schema(rectype, retdict)
        return retdict

    @classmethod
    def add(cls, core, records):
        payload = ",\n".join(['"add": { "doc": %s }' % json.dumps(r) for r in records])
        payload = "{\n%s\n}"% payload
        return cls.post(cls.solrurl + "/solr/"
                             + core + "/update?commitWithin=10000",
                             headers={"Content-Type": "application/json"},
                             data=payload)

    @classmethod
    async def async_add(cls, session, core, records):
        payload = ",\n".join(['"add": { "doc": %s }' % json.dumps(r) for r in records])
        payload = "{\n%s\n}"% payload
        async with session.post(cls.solrurl + "/solr/"
                               + core + "/update?commitWithin=10000",
                               headers={"Content-Type": "application/json"},
                               data=payload) as response:
            return response

    def savepageitem(self):
        records = [self.__class__.flatten(self.pageitem, rectype="text")]
        return self.__class__.add("text", records)

    async def async_savepageitem(self, session):
        records = [self.__class__.flatten(self.pageitem, rectype="text")]
        return await self.__class__.async_add(session, "text", records)

    def savelinkitems(self):
        records = [self.__class__.flatten(r, rectype="link") for r in self.linkitems]
        return self.__class__.add("link", records)

    async def async_savelinkitems(self, session):
        records = [self.__class__.flatten(r, rectype="link") for r in self.linkitems]
        return await self.__class__.async_add(session, "link", records)

    def save(self):
        self.savepageitem()
        self.savelinkitems()

    async def async_save(self, session):
        saved = await self.async_savepageitem(session)
        saved = await self.async_savelinkitems(session)

