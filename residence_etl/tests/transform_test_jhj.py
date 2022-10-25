import unittest
from datajob.etl.transform.academy import AcademyTransformer
from datajob.etl.transform.animal_hospital_transform import AnimalTransformer
from datajob.etl.transform.area_dong_transform import AreaDongTransformer
from datajob.etl.transform.area_gu import AreaGuTransformer
from datajob.etl.transform.bike_transform import BikeTransformer
from datajob.etl.transform.bus_transform import BusTransformer
from datajob.etl.transform.cafe_transform import CafeTransformer
from datajob.etl.transform.car_sharing import CarSharingTransformer
from datajob.etl.transform.category import CategoryTransformer
from datajob.etl.transform.cctv_transform import CctvTransformer
from datajob.etl.transform.child_med_transform import ChildMedTransformer
from datajob.etl.transform.con_store_transform import ConstoreTransformer
from datajob.etl.transform.department_store_transform import DepartmentstoreTransformer
from datajob.etl.transform.fire_sta_transform import FireTransformer
from datajob.etl.transform.golf_transform import GolfTransformer
from datajob.etl.transform.gym_transform import GymTransformer
from datajob.etl.transform.kids_cafe_transform import KidsCafeTransformer
from datajob.etl.transform.kindergarten_transform import KinderTransformer
from datajob.etl.transform.leisure_transform import LeisureTransformer
from datajob.etl.transform.loc import LocTransformer
from datajob.etl.transform.mcdonalds_transform import McdonaldsTransformer
from datajob.etl.transform.noise_vibration_transform import NoiseVibrationTransformer
from datajob.etl.transform.popu_dong_transform import PopuDongTransformer
from datajob.etl.transform.popu_gu_transform import PopuGuTransformer
from datajob.etl.transform.safe_delivery_transform import SafeTransformer
from datajob.etl.transform.sports_transform import SportsTransformer
from datajob.etl.transform.starbucks_transform import StarbucksTransformer
from datajob.etl.transform.subway_transform import SubwayTransformer


class MTest(unittest.TestCase):
    def test1(self):
        LocTransformer.transform()

    def test2(self):
        CategoryTransformer.transform()

    def test3(self):
        AcademyTransformer.transform()

    def test4(self):
        AnimalTransformer.transform()

    def test5(self):
        CarSharingTransformer.transform()

    def test6(self):
        AreaGuTransformer.transform()

    def test7(self):
        AreaDongTransformer.transform()

    def test8(self):
        PopuDongTransformer.transform()

    def test9(self):
        PopuGuTransformer.transform()

    def test10(self):
        BikeTransformer.transform()

    def test11(self):
        BusTransformer.transform()

    def test12(self):
        CafeTransformer.transform()

    def test13(self):
        StarbucksTransformer.transform()

    def test14(self):
        McdonaldsTransformer.transform()

    def test15(self):
        ConstoreTransformer.transform()

    def test16(self):
        DepartmentstoreTransformer.transform()
    
    def test17(self):
        CctvTransformer.transform()

    def test18(self):
        FireTransformer.transform()

    def test19(self):
        SafeTransformer.transform()
    
    def test20(self):
        ChildMedTransformer.transform()

    def test21(self):
        KinderTransformer.transform()
    
    def test22(self):
        LeisureTransformer.transform()
    
    def test23(self):
        GolfTransformer.transform()

    def test24(self):
        GymTransformer.transform()

    def test25(self):
        KidsCafeTransformer.transform()

    def test26(self):
        NoiseVibrationTransformer.transform()

    def test27(self):
        SubwayTransformer.transform()
    
    def test28(self):
        SportsTransformer.transform()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
