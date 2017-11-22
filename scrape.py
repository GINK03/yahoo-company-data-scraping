import requests

import os

import sys

import bs4

import re

import json


def _scrape(arr):
  companyid, proxy = arr
  if os.path.exists('controlflags/{}-finished'.format(companyid))  or \
    os.path.exists('controlflags/{}-exception'.format(companyid)):
    return
  try:
    data = {}
    # fundamentals 
    data['fundamentals'] = {}
    url = 'https://profile.yahoo.co.jp/fundamental/?s={}'.format(companyid)
    r = requests.get(url, proxies=proxy)
    soup = bs4.BeautifulSoup( r.text )
    for tr in soup.find_all('tr', {'class':'yjMt'}):
      tds = tr.find_all('td')
      typed = re.sub(r'\s{1,}', ' ', tds[0].text )
      detail = re.sub(r'\s{1,}', ' ', tds[1].text )
      print( typed, detail )
      data['fundamentals'][typed] = detail

    # profile
    url = 'https://profile.yahoo.co.jp/company_profile/{}'.format(companyid)
    r = requests.get(url)
    soup = bs4.BeautifulSoup( r.text )

    data['profile'] = {}
    if soup.find('div', {'id':'right_col'}) is not None:
      for tr in soup.find('div', {'id':'right_col'}).find_all('tr'):
        tds = tr.find_all('td')
        typed = re.sub(r'\s{1,}', ' ', tds[0].text.replace('\n', '') )
        detail = re.sub(r'\s{1,}', ' ', tds[1].text.replace('\n', '') )
        print( typed, detail )
        data['profile'][typed] = detail

    # special
    data['special'] = {}
    url = 'https://profile.yahoo.co.jp/special/{}'.format(companyid)
    r = requests.get(url)
    soup = bs4.BeautifulSoup( r.text )
    if soup.find('p', {'class':'yjMt'}) is not None:
      print( soup.find('p', {'class':'yjMt'}).text )
      data['special'] = soup.find('p', {'class':'yjMt'}).text

    # independent
    data['independent'] = {}
    url = 'https://profile.yahoo.co.jp/independent/{}'.format(companyid)
    r = requests.get(url)
    soup = bs4.BeautifulSoup( r.text )

    if soup.find('table', {'class':'yjMt'}) is not None:
      trs = soup.find('table', {'class':'yjMt'}).find_all('tr')

      heads = [ td.text for td in trs.pop(0).find_all('td') ]
      heads.pop(0)
      for tr in trs:
        tds = tr.find_all('td')
        typed = tds.pop(0).text
        obj = dict(zip(heads, [td.text for td in tds] ) ) 
        print( typed, obj )
        data['independent'][typed] = obj

      print( json.dumps(data, indent=2, ensure_ascii=False) )
      camp_name = re.sub(r'【.*?】', '', soup.find('strong', {'class':'yjL'}).text)
      camp_name = camp_name.replace('(株)', '')
      print( camp_name )
      open('data/{}_{}.json'.format(camp_name, companyid), 'w').write( json.dumps(data, indent=2, ensure_ascii=False) ) 
    open('controlflags/{}-finished'.format(companyid), 'w') 
  except Exception as e:
    open('controlflags/{}-exception'.format(companyid), 'w') 
    print('Deep', e)
import random
proxies = []
for line in open('aws_ips.txt'):
  line = line.strip()
  spot, ip = line.split()
  proxies.append( {'http':'{}:8080'.format(ip), 'https':'{}:8080'.format(ip) } ) 
arrs = [(i,random.choice(proxies)) for i in range(1, 2222)]
random.shuffle(arrs)
#[ _scrape(i) for i in iis] 
import concurrent.futures 
with concurrent.futures.ProcessPoolExecutor(max_workers=50) as exe:
  exe.map( _scrape, arrs )
