##

"""

    """

##


import pandas as pd

from githubdata import GithubData
from mirutil import funcs as mf

repo_url = 'https://github.com/imahdimir/raw-d-listed-firms-in-TSE'
tic2btic_repo_url = 'https://github.com/imahdimir/d-Ticker-2-BaseTicker-map'
btics_repo_url = 'https://github.com/imahdimir/d-uniq-BaseTickers'


namad = 'نماد'
btick = 'BaseTicker'
naam = 'نام - ا'
tick = 'Ticker'
cname = 'CompanyName'

def main() :

  pass

  ##
  repo = GithubData(repo_url)
  repo.clone_overwrite_last_version()
  ##
  dfpn = repo.local_path / 'بورس اوراق بهادار تهران - لیست شرکت ها.xlsx'
  df = pd.read_excel(dfpn)
  ##
  df[namad] = df[namad].apply(mf.norm_fa_str)
  ##
  msk = df[namad].ne(namad)
  df = df[msk]
  ##
  ptr = 'ح' + '\s?\.\s.+'
  msk = df[naam].str.fullmatch(ptr)
  df = df[~ msk]
  ##
  ptr = 'ح' + '\..+'
  msk = df[naam].str.fullmatch(ptr)
  df = df[~ msk]
  ##
  ptr = 'ح' + '\s.+'
  msk = df[naam].str.fullmatch(ptr)
  df = df[~ msk]
  ##
  msk = df[namad].eq('سابیک1')
  assert len(msk[msk]) <= 1
  df.loc[msk, namad] = 'سآبیک1'
  ##

  tic2btic_repo = GithubData(tic2btic_repo_url)
  tic2btic_repo.clone_overwrite_last_version()
  ##
  mdfpn = tic2btic_repo.data_fps[0]
  mdf = pd.read_parquet(mdfpn)
  mdf = mdf.reset_index()
  mdf = mdf.set_index(tick)
  ##
  df[btick] = df[namad].map(mdf[btick])
  ##
  msk = df[btick].isna()
  df1 = df[msk]

  ##
  btics_repo = GithubData(btics_repo_url)
  btics_repo.clone_overwrite_last_version()
  ##
  bdfpn = btics_repo.data_fps[0]
  bdf = pd.read_parquet(bdfpn)
  bdf = bdf.reset_index()
  ##
  df = df[[btick, naam]]
  ##
  bdf = bdf.merge(df, how = 'left')
  ##
  msk = bdf[cname].notna()
  msk &= bdf[naam].notna()
  msk &= bdf[cname].ne(bdf[naam])

  bdf.loc[msk, 'nsn'] = True
  bdf.loc[~ msk, 'nsn'] = False

  bdf.loc[msk, cname] = bdf[naam]
  ##
  msk = bdf[cname].isna()

  bdf.loc[msk, cname] = bdf[naam]
  ##
  msk = bdf[cname].isna()
  df2 = bdf[msk]

  ##
  bdf = bdf[[btick, cname]]
  bdf = bdf.set_index(btick)
  ##
  bdf.to_parquet(bdfpn)
  ##
  commit_msg = 'added 13 company names from https://github.com/imahdimir/raw-d-listed-firms-in-TSE'
  btics_repo.commit_and_push_to_github_data_target(commit_msg)
  ##

  btics_repo.rmdir()
  repo.rmdir()
  tic2btic_repo.rmdir()

  ##






  ##


##