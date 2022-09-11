"""

    """

import json
import pandas as pd

from githubdata import GithubData
from mirutil.df_utils import save_as_prq_wo_index as sprq


class GDUrl :
    with open('gdu.json' , 'r') as fi :
        gj = json.load(fi)

    cur = gj['cur']
    src = gj['src']
    src0 = gj['src0']
    src1 = gj['src1']
    trg = gj['trg']

gu = GDUrl()

class ColName :
    namad = 'نماد'
    btick = 'BaseTicker'
    naam = 'نام - ا'
    tick = 'Ticker'
    cname = 'CompanyName'
    ftic = 'FirmTicker'

c = ColName()

def main() :
    pass

    ##

    gds0 = GithubData(gu.src0)
    gds0.overwriting_clone()
    ##
    ds0 = gds0.read_data()
    ##

    gds1 = GithubData(gu.src1)
    gds1.overwriting_clone()
    ##
    ds1 = gds1.read_data()
    ##
    ds1 = ds1[[c.btick , c.ftic]]
    ##
    ds1 = ds1.drop_duplicates()
    ##
    ds1 = ds1.set_index(c.btick)
    ##

    ds0['f'] = ds0[c.ftic].map(ds1[c.ftic])
    ##
    msk = ds0['f'].notna()

    ds0.loc[msk , c.ftic] = ds0.loc[msk , 'f']

    ##

    ds0 = ds0[[c.ftic]]
    ##
    ds0 = ds0.drop_duplicates()
    ##

    gds0.rmdir()
    gds1.rmdir()

    ##

    gds = GithubData(gu.src)
    gds.overwriting_clone()
    ##
    df = gds.read_data()

    ##
    msk = df[c.namad].ne(c.namad)
    df = df[msk]
    ##
    ptr = 'ح' + '\s?\.\s.+'
    msk = df[c.naam].str.fullmatch(ptr)

    df = df[~ msk]
    ##
    ptr = 'ح' + '\..+'
    msk = df[c.naam].str.fullmatch(ptr)

    df = df[~ msk]
    ##
    ptr = 'ح' + '\s.+'
    msk = df[c.naam].str.fullmatch(ptr)

    df = df[~ msk]
    ##
    msk = df[c.namad].eq('سابیک1')
    assert len(msk[msk]) <= 1

    df.loc[msk , c.namad] = 'سآبیک1'

    ##

    ptr = '\D+1'
    msk = df[c.namad].str.fullmatch(ptr)

    df1 = df[msk]
    ##
    df1[c.namad] = df1[c.namad].str[:-1]
    ##

    df2 = ds0.merge(df1 , left_on = c.ftic , right_on = c.namad , how = 'left')
    ##
    df2 = df2.dropna()
    df2 = df2.rename(
            columns = {
                    c.naam : c.cname
                    }
            )
    df2 = df2[[c.cname , c.ftic]]

    ##

    gdt = GithubData(gu.trg)
    gdt.overwriting_clone()
    ##
    dg = gdt.read_data()
    ##

    dg = pd.concat([dg , df2] , axis = 0 , ignore_index = True)
    ##
    dg = dg.drop_duplicates()
    ##

    msk = dg.duplicated(subset = c.cname , keep = False)
    df1 = dg[msk]

    ##
    fp = gdt.data_fp
    sprq(dg , fp)

    ##
    msg = 'added to data by: '
    msg += gu.cur
    ##

    gdt.commit_and_push(msg)

    ##

    gds.rmdir()
    gdt.rmdir()

    ##

##
if __name__ == '__main__' :
    main()

##
