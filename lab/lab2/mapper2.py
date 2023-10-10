#!/usr/bin/python3

import re
import sys

tmp = {}

for line in sys.stdin:
    line = line.strip()

    words = re.split("[ *$&#/\t\n\f\"\'\\,.:;?!\[\](){}<>~\-_]", line.lower())

    for word in words:
        if len(word):
            tmp[word] = tmp.get(word, 0) + 1

for k, v in tmp.items():
    print (k + '\t' + str(v))

