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
    def syncpost(self,*args, **kwargs):
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
            res = cls.syncpost(
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
            res = cls.syncpost(
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
        if rectype and set(retdict) != set(cls.schema[rectype]):
            cls.schema(rectype, retdict)
        return retdict

    @classmethod
    async def add(cls, session, core, records):
        payload = ",\n".join(['"add": { "doc": %s }' % json.dumps(r) for r in records])
        payload = "{\n%s\n}"% payload
        # print(payload)
        async with session.post(cls.solrurl + "/solr/"
                               + core + "/update?commitWithin=10000",
                               headers={"Content-Type": "application/json"},
                               data=payload) as response:
            # print(response.text)
            return response

    async def savepageitem(self, session):
        records = [self.__class__.flatten(self.pageitem, "text")]
        return await self.__class__.add(session, "text", records)

    async def savelinkitems(self, session):
        records = [self.__class__.flatten(r, "link") for r in self.linkitems]
        return await self.__class__.add(session, "link", records)

    async def save(self, session):
        saved = await self.savepageitem(session)
        saved = await self.savelinkitems(session)
