import unittest
import XML_processor

url="https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
xobj = None
class Test_XML_Processor(unittest.TestCase):
    def test01URL(self):
        temp = XML_processor.XML_Processor("www.google.com",1,logfilename='testing.log')
        self.assertFalse(temp.getZipURL())
    
    def test02ChildNum(self):
        temp = XML_processor.XML_Processor(url,1000,logfilename='testing.log')
        self.assertFalse(temp.getZipURL())
    
    def test03URL(self):
        global xobj
        xobj =  XML_processor.XML_Processor(url,1,logfilename='testing.log')
        ret = xobj.getZipURL()
        self.assertTrue(ret)

    def test04dwldZip(self):
        global xobj
        ret = xobj.downloadZipFile()
        self.assertTrue(ret)
    
    def test05unzip(self):
        global xobj
        ret = xobj.unzipFile()
        self.assertTrue(ret)
    
    def test06XMLtoCSV(self):
        global xobj
        ret = xobj.XMLtoCSV('FinInstrmGnlAttrbts', ['Id', 'FullNm', 'ClssfctnTp',
              'NtnlCcy', 'CmmdtyDerivInd'], ['Issr'], 10)
        self.assertTrue(ret)
    
    def test07clng(self):
        global xobj
        ret = xobj.cleanUp()
        self.assertTrue(ret)

if __name__ == "__main__" : 
    unittest.main()