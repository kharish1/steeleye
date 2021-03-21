#!/usr/bin/python
# -*- coding: utf-8 -*-
'''The class which will do the XML processing, downloads, parses and converts to CSV'''

import requests
import logging
import zipfile
import xml.etree.ElementTree as ET
import os
#import boto3
import XML_Breaker
from xml.sax import parse

logging.basicConfig(filename='newfile.log',
                    format='%(asctime)s %(message)s',
                    filemode='a')
_logger = logging.getLogger()
_logger.setLevel(logging.DEBUG)

class XML_Processor:

    """
    The constructor takes following parameters
    xml_url : The URL from where XML zip file link is to be fetched
    childnum: The zip file link number helps to specify which zip file URL to be fetched
    filetype: The type to lookup before finding the zip file URL link
    dirpath: The location where to keep all the intermediate files
    logfilename: The name of the log file name
    """

    def __init__(
        self,
        xml_url,
        childnum=1,
        filetype='DLTINS',
        dirpath='./',
        ):
        self.xml_url = xml_url
        self.filetype = filetype
        self.childnum = childnum
        self.dirpath = dirpath
        self.logger = _logger


    def getZipURL(self, _timeout=10, deltemp=True):
        """
        This function finds the zip file name URL
        _timeout : The time out while doing GET request on the XML url
        deltemp: To specify if we need to keep intermediate files created by this function 
        """
        try:
            response = requests.get(self.xml_url, timeout=_timeout)
            if response.status_code != 200:
                raise Exception('Unable to GET XML file from URL')
            open('temp.xml', 'w').write(response.text)

            found = False
            count = -1
            for (event, elem) in ET.iterparse('temp.xml',
                    events=('start', )):
                if found:
                    break
                if elem.attrib == {}:
                    count += 1
                if count == self.childnum:
                    if elem.attrib.get('name', '') == 'download_link':
                        self.zipfileURL = elem.text
                    if elem.attrib.get('name', '') == 'file_type' \
                        and elem.text == self.filetype:
                        found = True
            if not found:
                raise Exception('Unable to locate the ZIP file URL for given child={} and filetype={}'.format(self.childnum,
                                self.filetype))
        except Exception as e:
            self.logger.error(e)
            return False
        else:
            self.logger.info('Located the ZIP file URL successfully')
        if deltemp:
            os.remove('temp.xml')
        return True

    def downloadZipFile(self, zname='temp.zip', _timeout=10):
        """
        This function downloads the zipfile from the url that was parsed above
        _timeout: The time out while doing GET request on the XML url
        zname: The file name to be given for the downloaded zip file
        """

        self.zname = zname
        try :
            open(self.dirpath + self.zname)
        except Exception as e:
            self.logger.info("File not yet downloaded, going ahead")
        else :
            self.logger.error("File already Downloaded, skipping download function execution")
            return True

        try:
            response = requests.get(self.zipfileURL, timeout=_timeout)
            if response.status_code != 200:
                raise Exception('Unable to download ZIP file')
            with open(self.dirpath + self.zname, 'wb') as fp:
                fp.write(response.content)
        except Exception as e:
            self.logger.error(e)
            return False
        else:
            self.logger.info('Downloaded the ZIP file from the URL successfully')
        return True

    def unzipFile(self):
        '''This function unzips the above downloaded file'''

        try:
            with zipfile.ZipFile(self.dirpath + self.zname) as zipfp:
                self.xmlfilename = zipfp.namelist()[0]
                try :
                    open(self.dirpath + self.xmlfilename)
                except:
                    self.logger.info("XML file does not exists, unzipping now")
                else :
                    self.logger.error("XML file already unzipped skipping the remaining part of function")
                    return True
                zipfp.extractall(self.dirpath)
        except Exception as e:
            self.logger.error(e)
            return False
        else:
            self.logger.info('Extracted ZIP file successfully')
        return True
    
    def splitXML(self,tagname,tags_perfile):
        try :
            parse(self.dirpath + self.xmlfilename,
            XML_Breaker.XML_Breaker(tagname,
            int(tags_perfile),
            out=XML_Breaker.CycleFile(self.dirpath + self.xmlfilename)))
        except Exception as e:
            self.logger.error(e)
            return False
        else :
            self.logger.info("Finished splitting")
        return True

    def cleanUp(self, files=['xml', 'zip']):
        """
        This function removes all the intermediate files created
        files: type of files to remove in the dirpath initially specified
        """

        try:
            if 'zip' in files:
                os.remove(self.dirpath + self.zname)
            if 'xml' in files:
                files = os.listdir(self.dirpath)
                for file in filter(lambda s : s.endswith(".xml"),files):
                    os.remove(self.dirpath + file)
        except Exception as e:
            self.logger.error(e)
            return False
        else:
            self.logger.info('Successfully deleted all files')
        return True
    
    def upload_to_aws(self, bucket, s3_file):
        "This function takes the destination details on AWS and uploads, all the keys are supplied from Docker secret"
        ACCESS_KEY = '1234' #secret.get('ACCESSKEY') #from docker
        SECRET_KEY = '45678' #secret.get('SECRETKEY') #from docker
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
        try:
            for csvfile in filter(lambda s: s.endswith(".csv"),os.listdir(self.dirpath)):
                s3.upload_file(self.dirpath + csvfile, bucket, s3_file)
        except Exception as e:
            self.logger.error(e)
            return False
        else :
            self.logger.info("Upload successful")
            return True


def XMLtoCSV(
    tgval,
    tgval_children,
    tgval_nonchildren,
    filename,
    rowcount=-1,
    csvname='',
    ):
    """
    Function converts the unzipped XML file to CSV and stores locally
    tgval(type str) = Parent tag to parse
    tgval_children(type list of string) = The children of tgval to parse
    tgval_nonchildren(type list of string) = The tags other than children of tgval to parse
    rowcount = The total number of rows to be generated in the output CSV, default is all 
    csvname : The csv file name with which output is to be saved
    """
    if csvname == '':
        csvname = filename[:-3] + 'csv'
    try :
        open(csvname)
    except :
        _logger.info("CSV file does not exist, going ahead with creating it")
    else :
        _logger.error("CSV file already exists, skipping the function")
        return True
    fp = open(csvname, 'w')
    header = (',' + tgval + '.').join([''] + tgval_children) \
        + ','.join([''] + tgval_nonchildren) + '\n'
    fp.write(header[1:])
    try:
        isParent = False
        row = opstr = ''
        rowcount += 1
        for (event, elem) in ET.iterparse(filename,
                events=('start', 'end')):
            tagval = elem.tag[elem.tag.find('}') + 1:]
            if tagval == tgval and event == 'start':
                isParent = True
            if tagval == tgval and event == 'end':
                isParent = False
                rowcount -= 1
                row = opstr
                opstr = ''
            if isParent and event == 'start':
                if tagval in tgval_children:
                    opstr += str(elem.text) + ','
            if not isParent and tagval in tgval_nonchildren \
                and event == 'start':
                row += str(elem.text)
                fp.write(row + '\n')
            if rowcount == 0:
                break
    except Exception as e:
        _logger.error(e)
    else:
        _logger.info('Converted {} XML file to CSV successfully with {} rows'.format(filename, -rowcount))
    finally:
        fp.close()
    return True
