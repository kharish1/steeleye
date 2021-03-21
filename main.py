#!/usr/bin/python
# -*- coding: utf-8 -*-
import XML_processor
import os
import multiprocessing

xobj = XML_processor.XML_Processor('https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100')
xobj.getZipURL()
xobj.downloadZipFile()
xobj.unzipFile()
xobj.splitXML('FinInstrmGnlAttrbts',10000)
files = os.listdir("./")
xmlfiles = list(filter(lambda s : s.endswith(".xml"),files))

values = []
for i in xmlfiles :
    values.append(('FinInstrmGnlAttrbts', ['Id', 'FullNm', 'ClssfctnTp',
              'NtnlCcy', 'CmmdtyDerivInd'], ['Issr'],i, -2))

pool = multiprocessing.Pool(multiprocessing.cpu_count())
res = pool.starmap(XML_processor.XMLtoCSV,values)
xobj.cleanUp()
#xobj.upload_to_aws('s3','dummy.csv')
