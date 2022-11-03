import sys
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

from datajob.etl.transform.academy_transform import AcademyTransformer
from datajob.etl.transform.animal_hospital_transform import AnimalTransformer
from datajob.etl.transform.area_dong_transform import AreaDongTransformer
from datajob.etl.transform.area_gu_transform import AreaGuTransformer
from datajob.etl.transform.bike_transform import BikeTransformer
from datajob.etl.transform.bus_transform import BusTransformer
from datajob.etl.transform.cafe_transform import CafeTransformer
from datajob.etl.transform.car_sharing_transform import CarSharingTransformer
from datajob.etl.transform.category_transform import CategoryTransformer
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
from datajob.etl.transform.loc_transform import LocTransformer
from datajob.etl.transform.mcdonalds_transform import McdonaldsTransformer
from datajob.etl.transform.noise_vibration_transform import NoiseVibrationTransformer
from datajob.etl.transform.popu_dong_transform import PopuDongTransformer
from datajob.etl.transform.popu_gu_transform import PopuGuTransformer
from datajob.etl.transform.safe_delivery_transform import SafeTransformer
from datajob.etl.transform.sports_transform import SportsTransformer
from datajob.etl.transform.starbucks_transform import StarbucksTransformer
from datajob.etl.transform.subway_transform import SubwayTransformer

from datajob.etl.transform.ele_sch_transform import ElementaryShcTransformer
from datajob.etl.transform.mid_sch_transform import MiddleShcTransformer
from datajob.etl.transform.high_sch_transform import HighShcTransformer
from datajob.etl.transform.police_transform import PoliceTransformer
from datajob.etl.transform.hospital_transform import HospitalTransformer
from datajob.etl.transform.pharm_transform import PharmTransformer

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
from datajob.datamart.final_num import SubSampleDataMart
from datajob.etl.transform.park_transform import ParkTransformer
from datajob.datamart.park_datamart import ParkDataMart
from datajob.etl.extract.vegan_extract import VeganExtractor
from datajob.etl.transform.vegan_transform import VeganTransformer
from datajob.datamart.vegan_datamart import VeganDataMart
from datajob.etl.transform.coliving_transform import ColivingTransformer
from datajob.etl.transform.popu_mz_transform import PopuMZTransformer
from datajob.etl.transform.retail_transform import RetailTransformer
from residence_etl.datajob.datamart.coliving_datamart import ColivingDataMart
from residence_etl.datajob.datamart.popu_mz_datamart import PopuMZDataMart
from residence_etl.datajob.datamart.retail_datamart import RetailDataMart

works = {
    'extract':{
        'Bike':BikeExtractor.extract_data,
        'Academy': AcademyExtractor.extract_data,
        'Bus':BusExtractor.extract_data,
        'CarSharing':CarSharingExtractor.extract_data,
        'ChildMed':ChildMedExtractor.extract_data,
        'Golf':GolfExtractor.extract_data,
        'Gym':GymExtractor.extract_data,
        'KidsCafe': KidsCafeExtractor.extract_data,
        'Kindergarten':KindergartenExtractor.extract_data,
        'SafeDelivery':SafeDeliveryExtractor.extract_data,
        'Pharmacy':PharmacyExtractor.extract_data,
        'AnimalHospital':AnimalHospitalExtractor.extract_data,
        'Vegan':VeganExtractor.extract_data
    },
    'transform':{
        'Loc':LocTransformer.transform,
        'Category':CategoryTransformer.transform,
        'Academy':AcademyTransformer.transform,
        'Animal':AnimalTransformer.transform,
        'CarSharing':CarSharingTransformer.transform,
        'AreaGu':AreaGuTransformer.transform,
        'AreaDong':AreaDongTransformer.transform,
        'PopuDong':PopuDongTransformer.transform,
        'PopuGu':PopuGuTransformer.transform,
        'Bike':BikeTransformer.transform,
        'Bus':BusTransformer.transform,
        'Cafe':CafeTransformer.transform,
        'Starbucks':StarbucksTransformer.transform,
        'Mcdonalds':McdonaldsTransformer.transform,
        'Constore':ConstoreTransformer.transform,
        'Departmentstore':DepartmentstoreTransformer.transform,
        'Cctv':CctvTransformer.transform,
        'Fire':FireTransformer.transform,
        'Safe':SafeTransformer.transform,
        'ChildMed':ChildMedTransformer.transform,
        'Kinder':KinderTransformer.transform,
        'Leisure':LeisureTransformer.transform,
        'Golf':GolfTransformer.transform,
        'Gym':GymTransformer.transform,
        'KidsCafe':KidsCafeTransformer.transform,
        'NoiseVibration':NoiseVibrationTransformer.transform,
        'Subway':SubwayTransformer.transform,
        'Sports':SportsTransformer.transform,
        'ElementaryShc':ElementaryShcTransformer.transform,
        'MiddleShc':MiddleShcTransformer.transform,
        'HighShc':HighShcTransformer.transform,
        'Police':PoliceTransformer.transform,
        'Hospital':HospitalTransformer.transform,
        'Pharm':PharmTransformer.transform,
        'Park':ParkTransformer.transform,
        'Vegan':VeganTransformer.transform,
        'Coliving':ColivingTransformer.transform,
        'PopuMZ':PopuMZTransformer.transform,
        'Retail':RetailTransformer.transform
    },
    'datamart':{
        'PopuGu':PopuGuDataMart.save,
        'PopuDong':PopuDongDataMart.save,
        'AreaGu':AreaGuDataMart.save,
        'AreaDong':AreaDongDataMart.save,
        'Academy':AcademyDataMart.save,
        'Kindergarten':KindergartenDataMart.save,
        'ChildMed':ChildMedDataMart.save,
        'FireSta':FireStaDataMart.save,
        'Elementary':ElementaryDataMart.save,
        'HighSchool':HighSchoolDataMart.save,
        'NoiseVibration':NoiseVibrationDataMart.save,
        'DepartmentStore':DepartmentStoreDataMart.save,
        'AnimalHospital':AnimalHospitalDataMart.save,
        'SafeDelivery':SafeDeliveryDataMart.save,
        'Cctv':CctvDataMart.save,
        'Pharmacy':PharmacyDataMart.save,
        'Bike':BikeDataMart.save,
        'CarSharing':CarSharingDataMart.save,
        'Bus':BusDataMart.save,
        'Leisure':LeisureDataMart.save,
        'KidsCafe':KidsCafeDataMart.save,
        'Starbucks':StarbucksDataMart.save,
        'ConStore':ConStoreDataMart.save,
        'Mcdonalds':McdonaldsDataMart.save,
        'Cafe':CafeDataMart.save,
        'Gym':GymDataMart.save,
        'Golf':GolfDataMart.save,
        'Subway':SubwayDataMart.save,
        'Sport':SportDataMart.save,
        'MiddleSchool':MiddleSchoolDataMart.save,
        'Police':PoliceDataMart.save,
        'Hospital':HospitalDataMart.save,
        'Park':ParkDataMart.save,
        'Vegan':VeganDataMart.save,
        'Coliving':ColivingDataMart.save,
        'PopuMZ':PopuMZDataMart.save,
        'Retail':RetailDataMart.save
        }

}

if __name__ == "__main__":
    args = sys.argv
    print(args)

    # main.py 작업(extract, transform, datamart) 저장할 위치(테이블)
    # 매개변수 2개
    if len(args) != 3:
        raise Exception('2개의 전달인자가 필요합니다.')

    if args[1] not in works.keys():
        raise Exception('첫번째 전달인자가 이상함 >> ' + str(works.keys()))

    if args[2] not in works[args[1]].keys():
        raise Exception('두번째 전달인자가 이상함 >> ' + str(works[args[1]].keys()))

    work = works[args[1]][args[2]]
    work()
