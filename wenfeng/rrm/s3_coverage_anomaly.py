#!/usr/bin/env python
#
import pyspark

s3_bucket = "s3://mist-aggregated-stats-production/"


# df = sqlContext.read.format('csv').options(header='true', inferSchema='true').load(s3_path)

date_day = "2020-03-04"
hr = '*'

s3_capacity_path = "entity_event/entity_event-production/dt={day}/hr={hr}/CapacityAnomalyEvent_*.seq".format(day=date_day, hr=hr)
s3_coverage_path = "entity_event/entity_event-production/dt={day}/hr={hr}/CoverageAnomalyEvent_*.seq".format(day=date_day, hr=hr)


s3_capacity_path = s3_bucket + s3_capacity_path
s3_coverage_path = s3_bucket + s3_coverage_path

print(s3_capacity_path, "\n", s3_coverage_path)


# In[33]:


rdd_capacity = spark.sparkContext.sequenceFile(s3_capacity_path)
rdd_coverage = spark.sparkContext.sequenceFile(s3_coverage_path)


# In[34]:


rdd_capacity.first()


# In[50]:


import json
# rdd_site = rdd.map(lambda x: json.loads(x[1])).map(lambda x: (x.get("site_id"), x.get("ap_id"))).groupByKey()
# rdd_site = rdd.map(lambda x: json.loads(x[1])).map(lambda x: (x.get("site_id"))).groupBy('site_id')
# df_capacity = rdd_capacity.toDF() #.map(lambda x: json.loads(x[1])).


# In[46]:


# df_capacity


# In[48]:


# rdd_site.take(1)
rdd_capacity_by_site= rdd_capacity.map(lambda x: json.loads(x[1])).map(lambda x: (x.get("site_id"),1)).countByKey()

# rdd_coverage_by_site= rdd_coverage.map(lambda x: json.loads(x[1])).map(lambda x: (x.get("site_id"),1)).countByKey()


# In[72]:


site_capacity_sorted = sorted(rdd_capacity_by_site.items(), key=lambda v:v[1], reverse=True) #, key=lambda(k, v): v)
site_coverage_sorted = sorted(rdd_coverage_by_site.items(), key=lambda v:v[1], reverse=True) #, key=lambda(k, v): v)


# In[77]:


site_capacity_sorted[:10]


# In[76]:


site_coverage_sorted[:10]


# In[39]:


rdd_coverage_by_site.sort()


# In[ ]:




