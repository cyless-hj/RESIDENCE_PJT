import unittest
from datajob.etl.transform.ex import CoronaPatientTrasformer


class MTest(unittest.TestCase):
    def test1(self):
        CoronaPatientTrasformer.transform()


if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
