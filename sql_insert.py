#!/usr/bin/env python
# coding: utf-8

# <h1> Shark Database</h1>

# In[35]:


import pyodbc
import csv
from datetime import datetime, timedelta


# In[39]:


sharkfile = r'c:\data\GSAF5.csv'


# In[40]:


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')


# In[41]:


cur = conn.cursor()


# In[38]:


attack_dates = []
isfatal = []
case = []
country = []
activity = []
age = []
gender = []


# In[32]:


with open(sharkfile, encoding = 'utf-8') as f:
    reader = csv.DictReader(f)
    
    for row in reader:
        attack_dates.append(row['Date'])
        case.append(row['Case Number'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])


# In[33]:


data = zip(attack_dates, case, country, activity, age, gender, isfatal)


# In[24]:


q = 'INSERT INTO louis.shark (attack_date ,case_number ,country ,activity ,age ,gender ,isfatal) values (?,?,?,?,?,?,?)'


# In[34]:


for d in data:
    try:
        cur.execute(q,d)
        conn.commit()
    except:
        conn.rollback()


# In[ ]:




