import unittest
import os

def run_tests():
    """
    Discover and run all tests in the current directory.
    """
    test_dir = os.path.dirname(os.path.abspath(__file__))  # Get the absolute path of the tests folder
    print(f"Discovering tests in: {test_dir}")  # Debug: Print the test directory

    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=test_dir, pattern="test_*.py")

    if not suite.countTestCases():
        print("❌ No tests were found. Ensure test files start with 'test_' and contain valid test methods.")
        exit(1)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    if result.wasSuccessful():
        print("\n✅ All tests passed successfully!")
        exit(0)
    else:
        print("\n❌ Some tests failed. Check the output above for details.")
        exit(1)

if __name__ == "__main__":
    run_tests()