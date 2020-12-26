---
layout: post
title: Hue Oozie Hdfs
tags: [oozie,hue,hdfs,workflow,jekyll]
image: '/images/hue/hue-icon.png'
---

Hue provides UI for main hadoop features.

- Oozie Workflow Designer
- Launch and Monitor Oozie Workflow , Schedules  ,Bundles 
- Hive Query interface
- Pig Query interface
- HDFS File Browser
- Hadoop Shell Access
- Notebooks with Hive, Pig , Spark , Scale , Java intrepreters

##### Start Hue Server

```sh
cd build/env/bin/
./hue runcpserver

```



##### Oozie Workflow Designer

![]({{ site.baseurl }}/images/hue/oozie-movies.png)

![]({{ site.baseurl }}/images/hue/ozzie-wf-xml.png)

##### Launch Oozie Workflow



![]({{ site.baseurl }}/images/hue/ozzie-submit-movies.png)



##### Monitor Jobs Workflow , Schedules  ,Bundles 

![]({{ site.baseurl }}/images/hue/oozie-jobs.png)



Veify Job launched from Hue in Oozie webUI

![]({{ site.baseurl }}/images/hue/ozzie-dag.png)



##### Hive Query

![]({{ site.baseurl }}/images/hue/hue-hive-sql.png)

##### 

##### HDFS File Browser

![]({{ site.baseurl }}/images/hue/hue-file-browser.png)