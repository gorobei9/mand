
# XXX
# really need our own context here else someone will set their own params and we will go horribly wrong
# in subtle ways.
# Probably need about $1e9 * 1e6 Turkish Lira (old)/per dollar
# and about 8 decimal places to deal with BitCoin,
# so, fixing at thirty digits, with 10 right of the decimal point should be about right.
# or maybe 30-40 if we really care about historical Zimbabwe data.

import decimal

def num(v):
    return decimal.Decimal(str(v))
