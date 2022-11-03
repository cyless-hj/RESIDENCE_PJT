import unittest
from datajob.etl.extract.academy_extract import AcademyExtractor
from datajob.etl.extract.animal_hospital_extract import AnimalHospitalExtractor
from datajob.etl.extract.bike_extract import BikeExtractor
from datajob.etl.extract.bus_extract import BusExtractor
from datajob.etl.extract.car_sharing_extract import CarSharingExtractor
from datajob.etl.extract.child_med_extract import ChildMedExtractor
from datajob.etl.extract.golf_extract import GolfExtractor
from datajob.etl.extract.gym_extract import GymExtractor
from datajob.etl.extract.kids_cafe_extract import KidsCafeExtractor
from datajob.etl.extract.kindergarten_extract import KindergartenExtractor
from datajob.etl.extract.pharmacy_extract import PharmacyExtractor
from datajob.etl.extract.safe_delivery_extract import SafeDeliveryExtractor
from datajob.etl.extract.vegan_extract import VeganExtractor


class MTest(unittest.TestCase):
    def test1(self):
        BikeExtractor.extract_data()

    def test2(self):
        AcademyExtractor.extract_data()

    def test3(self):
        BusExtractor.extract_data()

    def test4(self):
        CarSharingExtractor.extract_data()

    def test5(self):
        ChildMedExtractor.extract_data()

    def test6(self):
        GolfExtractor.extract_data()

    def test7(self):
        GymExtractor.extract_data()

    def test8(self):
        KidsCafeExtractor.extract_data()

    def test9(self):
        KindergartenExtractor.extract_data()

    def test10(self):
        SafeDeliveryExtractor.extract_data()

    def test11(self):
        PharmacyExtractor.extract_data()

    def test12(self):
        AnimalHospitalExtractor.extract_data()

    def test13(self):
        VeganExtractor.extract_data()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
