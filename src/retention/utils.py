# y returns the year of date.
def y(date):
    return date.year


#  ym returns an integer in the form YYYYMM.
def ym(date):
    return date.year * 100 + date.month


# yw returns an integer in the form YYYYWW, where WW is the week number.
def yw(date):
    return date.year * 100 + date.isocalendar()[1]


# ymd returns an integer in the form YYYYMMDD.
def ymd(date):
    return date.year * 10000 + date.month * 100 + date.day


# ymdh returns an integer in the form YYYYMMDDHH.
def ymdh(date):
    return date.year * 1000000 + date.month * 10000 + date.day * 100 + date.hour


# always returns timestamp as uniqure number for date
def always(date):
    return date.timestamp()
