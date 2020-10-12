from unittest import TestCase


class TestDataHandler(TestCase):
    # def test__get_time_series_df(self):
    #     self.fail()

    def test__void(self):
        x = ["x1", "x2", "x3"]
        y = ["p1", "p2", "p3"]

        for x, y in zip(x, y):
            print(x + ": " + y)

        assert True
