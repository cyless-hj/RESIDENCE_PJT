import unittest

from datajob.etl.transform.ele_sch_transform import ElementaryShcTransformer
from datajob.etl.transform.mid_sch_transform import MiddleShcTransformer
from datajob.etl.transform.high_sch_transform import HighShcTransformer
from datajob.etl.transform.police_transform import PoliceTransformer
from datajob.etl.transform.hospital_transform import HospitalTransformer
from datajob.etl.transform.pharm_transform import PharmTransformer
from datajob.etl.transform.park_transform import ParkTransformer
from datajob.datamart.park_datamart import ParkDataMart
from datajob.etl.transform.retail_transform import RetailTransformer
from datajob.datamart.retail_datamart import RetailDataMart
from datajob.etl.transform.coliving_transform import ColivingTransformer
from datajob.etl.transform.popu_mz_transform import PopuMZTransformer
from datajob.datamart.coliving_datamart import ColivingDataMart
from datajob.datamart.popu_mz_datamart import PopuMZDataMart


class MTest(unittest.TestCase):

    def test1(self):
        ElementaryShcTransformer.transform()

    def test2(self):
        MiddleShcTransformer.transform()

    def test3(self):
        HighShcTransformer.transform()  

    def test4(self):
        PoliceTransformer.transform()  

    def test5(self):
        HospitalTransformer.transform()

    def test6(self):
        PharmTransformer.transform()

    def test7(self):
        ParkTransformer.transform()

    def test8(self):
        RetailTransformer.transform()

    def test9(self):
        RetailDataMart.save()

    def test10(self):
        ColivingTransformer.transform()
    
    def test11(self):
        PopuMZTransformer.transform()

    def test12(self):
        ColivingDataMart.save()

    def test13(self):
        PopuMZDataMart.save()

    




if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
