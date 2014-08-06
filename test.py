from unicodecsv import DictReader
from datetime import datetime
from whoosh.index import FileIndex
from whoosh.filedb.filestore import RamStorage
from whoosh.fields import TEXT, NUMERIC, Schema
from whoosh.query import Term, And
from whoosh.qparser import QueryParser

f = open('final.csv', 'r')
r = DictReader(f)
parsed = list(r)
for line in parsed:
    line["date"] = datetime.strptime(line["date"], '%d/%m/%y').date()

by_date = sorted(parsed, key=lambda x: x["date"])
total_len = len(by_date)
learning_len = 2 * total_len / 3

to_learn = by_date[:learning_len]
to_test = by_date[learning_len:]

print("learning from {0} to {1}".format(
    to_learn[0]["date"],
    to_learn[-1]["date"]))
print("testing from {0} to {1}".format(
    to_test[0]["date"],
    to_test[-1]["date"]))

known_tenants = set()
for l in to_learn:
    known_tenants.add(l["tenant"])

prev_imports = to_learn

tenant_schema = Schema(name=TEXT(stored=True))
tenant_storage = RamStorage()
tenant_ix = tenant_storage.create_index(tenant_schema)
#import_schema = Schema(
    #tenant=TEXT(stored=True),
    #text=TEXT(stored=True),
    #amount=NUMERIC(stored=True))
#import_ix = FileIndex(RamStorage(), import_schema)

tenant_writer = tenant_ix.writer()
for t in known_tenants:
    tenant_writer.add_document(name=t)
tenant_writer.commit()

#import_writer = import_ix.writer()
#for t in prev_imports:
    #import_writer.add_document(
        #tenant=t["tenant"],
        #text=t["text"],
        #amount=t["amount"])
#import_writer.commit()

tenant_searcher = tenant_ix.searcher()
#import_searcher = import_ix.searcher()

parser = QueryParser("name", tenant_ix.schema)
query = parser.parse(to_test[0]["tenant"])
print(tenant_ix)
print(tenant_searcher.search(query))
import pdb; pdb.set_trace()
