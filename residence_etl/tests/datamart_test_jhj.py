import unittest
from datajob.datamart.academy_datamart import AcademyDataMart
from datajob.datamart.animal_hospital_datamart import AnimalHospitalDataMart
from datajob.datamart.area_dong_datamart import AreaDongDataMart
from datajob.datamart.area_gu_datamart import AreaGuDataMart
from datajob.datamart.cctv_datamart import CctvDataMart
from datajob.datamart.child_med_datamart import ChildMedDataMart
from datajob.datamart.department_store_datamart import DepartmentStoreDataMart
from datajob.datamart.elementary_datamart import ElementaryDataMart
from datajob.datamart.fire_sta_datamart import FireStaDataMart
from datajob.datamart.high_school_datamart import HighSchoolDataMart
from datajob.datamart.kindergarten_datamart import KindergartenDataMart
from datajob.datamart.noise_vibration_datamart import NoiseVibrationDataMart
from datajob.datamart.popu_dong_datamart import PopuDongDataMart

from datajob.datamart.popu_gu_datamart import PopuGuDataMart
from datajob.datamart.safe_delivery_datamart import SafeDeliveryDataMart


class MTest(unittest.TestCase):
    def test1(self):
        PopuGuDataMart.save()

    def test2(self):
        PopuDongDataMart.save()

    def test3(self):
        AreaGuDataMart.save()

    def test4(self):
        AreaDongDataMart.save()

    def test5(self):
        AcademyDataMart.save()

    def test6(self):
        KindergartenDataMart.save()

    def test7(self):
        ChildMedDataMart.save()

    def test8(self):
        FireStaDataMart.save()

    def test9(self):
        ElementaryDataMart.save()

    def test10(self):
        HighSchoolDataMart.save()

    def test11(self):
        NoiseVibrationDataMart.save()

    def test12(self):
        DepartmentStoreDataMart.save()

    def test13(self):
        AnimalHospitalDataMart.save()

    def test14(self):
        SafeDeliveryDataMart.save()

    def test15(self):
        CctvDataMart.save()

    def test16(self):
        CctvDataMart.save()

if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
