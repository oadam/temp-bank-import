from unicodecsv import DictReader
from datetime import datetime

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

print("learning from {0} to {1}".format(to_learn[0]["date"], to_learn[-1]["date"]))
print("testing from {0} to {1}".format(to_test[0]["date"], to_test[-1]["date"]))
