from unicodecsv import DictReader
from datetime import datetime
from whoosh.filedb.filestore import RamStorage
from whoosh.fields import TEXT, ID, NUMERIC, Schema
from whoosh.query import Term, Or
from collections import Counter
import re

f = open('final.csv', 'r')
r = DictReader(f)
parsed = list(r)
for line in parsed:
    line["date"] = datetime.strptime(line["date"], '%d/%m/%y').date()
    line["amount"] = float(line["amount"])

by_date = sorted([x for x in parsed if x['amount'] > 0], key=lambda x: x["date"])
total_len = len(by_date)
learning_len = 2 * total_len / 3

to_learn = by_date[:learning_len]
to_test = by_date[learning_len:]

#import pdb; pdb.set_trace()
print("learning from {0} to {1} with {2} entries".format(
    to_learn[0]["date"],
    to_learn[-1]["date"],
    len(to_learn)))
print("testing from {0} to {1} with {2} entries".format(
    to_test[0]["date"],
    to_test[-1]["date"],
    len(to_test)))

known_tenants_set = set()
for l in to_learn:
    known_tenants_set.add(l["tenant"].lower())
known_tenants = list(known_tenants_set)

prev_imports = to_learn

tenant_schema = Schema(name=TEXT(stored=True), id=NUMERIC(stored=True))
tenant_storage = RamStorage()
tenant_ix = tenant_storage.create_index(tenant_schema)
#import_schema = Schema(
#tenant=TEXT(stored=True),
#text=TEXT(stored=True),
#amount=NUMERIC(stored=True))
#import_ix = FileIndex(RamStorage(), import_schema)

tenant_writer = tenant_ix.writer()
for i in range(0, len(known_tenants)):
    ten = known_tenants[i]
    for w in ten.split():
        tenant_writer.add_document(id=i, name=w.lower())
tenant_writer.commit()

#import_writer = import_ix.writer()
#for t in prev_imports:
    #import_writer.add_document(
        #tenant=t["tenant"],
        #text=t["text"],
        #amount=t["amount"])
#import_writer.commit()

end_result = []
with tenant_ix.searcher() as searcher:
    for toto in to_test:
        result = searcher.search(Or([Term("name", t.lower()) for t in re.split('\W+', toto['name'])]))
        commons = Counter([r["id"] for r in result]).most_common()
        matches = [(known_tenants[c[0]], c[1]) for c in commons]
        end_result.append((toto["name"], matches))

print("\n".join(sorted([unicode(r) for r in end_result if len(r[1]) == 0 or r[1][0][1] <= 1], key=lambda x : x[0] )))


