import unittest


def add(x, y):
    """Simple add function for testing."""
    return x + y


class TestExample(unittest.TestCase):
    def test_add_function(self):
        """Test basic addition functionality."""
        self.assertEqual(add(2, 3), 5)
        self.assertEqual(add(-1, 1), 0)
        self.assertEqual(add(0, 0), 0)


if __name__ == "__main__":
    unittest.main()
