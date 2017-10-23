# -*- coding: utf-8 -*-
"""
Created on Sun Jun 25 20:58:51 2017

@author: guof
"""

import itchat
itchat.login()

friends = itchat.get_friends(update=True)

print(friends)