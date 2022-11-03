import unittest
from datajob.datamart.academy_datamart import AcademyDataMart
from datajob.datamart.animal_hospital_datamart import AnimalHospitalDataMart
from datajob.datamart.area_dong_datamart import AreaDongDataMart
from datajob.datamart.area_gu_datamart import AreaGuDataMart
from datajob.datamart.bike_datamart import BikeDataMart
from datajob.datamart.bus_datamart import BusDataMart
from datajob.datamart.cafe_datamart import CafeDataMart
from datajob.datamart.car_sharing_datamart import CarSharingDataMart
from datajob.datamart.cctv_datamart import CctvDataMart
from datajob.datamart.child_med_datamart import ChildMedDataMart
from datajob.datamart.con_store_datamart import ConStoreDataMart
from datajob.datamart.department_store_datamart import DepartmentStoreDataMart
from datajob.datamart.elementary_datamart import ElementaryDataMart
from datajob.datamart.final_num import FinNumDataMart
from datajob.datamart.fire_sta_datamart import FireStaDataMart
from datajob.datamart.golf_datamart import GolfDataMart
from datajob.datamart.gym_datamart import GymDataMart
from datajob.datamart.high_school_datamart import HighSchoolDataMart
from datajob.datamart.hospital_datamart import HospitalDataMart
from datajob.datamart.kids_cafe_datamart import KidsCafeDataMart
from datajob.datamart.kindergarten_datamart import KindergartenDataMart
from datajob.datamart.leisure_datamart import LeisureDataMart
from datajob.datamart.mcdonalds_datamart import McdonaldsDataMart
from datajob.datamart.middle_datamart import MiddleSchoolDataMart
from datajob.datamart.noise_vibration_datamart import NoiseVibrationDataMart
from datajob.datamart.pharmacy_datamart import PharmacyDataMart
from datajob.datamart.police_datamart import PoliceDataMart
from datajob.datamart.popu_dong_datamart import PopuDongDataMart
from datajob.datamart.popu_gu_datamart import PopuGuDataMart
from datajob.datamart.safe_delivery_datamart import SafeDeliveryDataMart
from datajob.datamart.sport_datamart import SportDataMart
from datajob.datamart.starbucks_datamart import StarbucksDataMart
from datajob.datamart.subway_datamart import SubwayDataMart
from datajob.datamart.park_datamart import ParkDataMart
from datajob.datamart.vegan_datamart import VeganDataMart


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
        PharmacyDataMart.save()

    def test17(self):
        BikeDataMart.save()

    def test18(self):
        CarSharingDataMart.save()

    def test19(self):
        BusDataMart.save()

    def test20(self):
        LeisureDataMart.save()

    def test21(self):
        KidsCafeDataMart.save()

    def test22(self):
        StarbucksDataMart.save()

    def test23(self):
        ConStoreDataMart.save()

    def test24(self):
        McdonaldsDataMart.save()

    def test25(self):
        CafeDataMart.save()

    def test26(self):
        GymDataMart.save()

    def test27(self):
        GolfDataMart.save()

    def test28(self):
        SubwayDataMart.save()

    def test29(self):
        SportDataMart.save()

    def test30(self):
        MiddleSchoolDataMart.save()

    def test31(self):
        PoliceDataMart.save()

    def test32(self):
        HospitalDataMart.save()

    def test33(self):
        FinNumDataMart.save()

    def test34(self):
        ParkDataMart.save()

    def test35(self):
        VeganDataMart.save()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
