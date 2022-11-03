import unittest

from datajob.datamart.final_num import SubSampleDataMart

class MTest(unittest.TestCase):
    def test1(self):
        SubSampleDataMart.save()

if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
