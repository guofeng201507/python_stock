# -*- coding: utf-8 -*-
"""
Created on Sat Jun 24 09:32:54 2017

@author: guof
"""

from bs4 import BeautifulSoup
import urllib.request as ur

webpage = ur.urlopen('http://en.wikipedia.org/wiki/Main_Page')
soup = BeautifulSoup(webpage, 'html.parser')
for anchor in soup.find_all('a'):
    print(anchor.get('href', '/'))
