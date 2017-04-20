# coding: utf-8
from datetime import timedelta


def unbend(in_list):
    # FIXME виправити провірку на заглибленість
    """
    розпакувує вкладені списки в одини єдиний глибиною 1
    """
    out_list = list()
    for el in in_list:
        out_list.extend(el)

    return out_list


def clean(string, simbols=None):
    """clean additional characters"""
    clean_list = [u'\t', u'\n', u'\r', u' ']
    _string = string
    if simbols is None:
        for el in clean_list:
            _string = _string.replace(el, u'')
    else:
        clean_list.extend(simbols)
        for el in clean_list:
            _string = _string.replace(el, u'')

    return _string


def add_zero(x):
    """add zero before string"""
    x = str(x)
    if len(x) == 1:
        x = u"0" + x
    return x


def time_to_dict(today, days_delta=0):
    """split date into dict"""

    time_in_br = today - timedelta(days=days_delta)

    _now = {
        u"hour": add_zero(time_in_br.hour) + u":00",
        u"day": add_zero(time_in_br.day),
        u"month": str(time_in_br.month),
        u"year": str(time_in_br.year)
    }

    return _now
