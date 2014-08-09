from unicodecsv import DictReader
from datetime import datetime
from whoosh.filedb.filestore import RamStorage
from whoosh.fields import TEXT, NUMERIC, Schema
from whoosh.query import Term, Or
from collections import OrderedDict
import re


# import pdb; pdb.set_trace()
def parse_final_csv():
    f = open('final.csv', 'r')
    r = DictReader(f)
    parsed = list(r)
    for line in parsed:
        line["date"] = datetime.strptime(line["date"], '%d/%m/%y').date()
        line["amount"] = float(line["amount"])
    positive = [x for x in parsed if x['amount'] > 0]
    by_date = sorted(positive, key=lambda x: x["date"])
    return by_date

by_date = parse_final_csv()
learning_len = 2 * len(by_date) / 3
to_learn = by_date[:learning_len]
to_test = by_date[learning_len:]


def mock_tenants(prev_imports):
    dedup = list(OrderedDict.fromkeys([l["tenant"].lower() for l in to_learn]))
    result = []
    for i in range(0, len(dedup)):
        result.append({"id": i, "name": dedup[i]})
    return result


known_tenants = mock_tenants(to_learn)


def create_tenant_schema(tenants):
    tenant_schema = Schema(name=TEXT(stored=True), id=NUMERIC(stored=True))
    tenant_storage = RamStorage()
    tenant_ix = tenant_storage.create_index(tenant_schema)
    tenant_writer = tenant_ix.writer()
    for t in tenants:
        tenant_writer.add_document(id=t["id"], name=t["name"].lower())
    tenant_writer.commit()
    return tenant_ix

tenant_ix = create_tenant_schema(known_tenants)

# import_schema = Schema(
# tenant=TEXT(stored=True),
# text=TEXT(stored=True),
# amount=NUMERIC(stored=True))
# import_ix = FileIndex(RamStorage(), import_schema)
# import_writer = import_ix.writer()
# for t in prev_imports:
#     import_writer.add_document(
#         tenant=t["tenant"],
#         text=t["text"],
#         amount=t["amount"])
# import_writer.commit()

end_result = []
with tenant_ix.searcher() as searcher:
    for toto in to_test:
        words = re.split('\W+', toto['name'])
        query = Or([Term("name", t.lower()) for t in words])
        result = searcher.search(query)
        matches = [{
            "tenant": known_tenants[r["id"]],
            "score":r.score
            } for r in result]
        end_result.append({"import": toto["name"], "matches": matches})

get_score = lambda r: 0 if len(r["matches"]) == 0 else r["matches"][0]["score"]
s = sorted(end_result, reverse=True, key=get_score)
print("\n".join(map(unicode, s)))
