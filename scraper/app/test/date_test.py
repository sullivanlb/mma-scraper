import sys
import os
import unittest
from unittest.mock import patch

# Add project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datetime import datetime
import pendulum

# Assuming the utils.py file is in scraper/app/
from scraper.app.utils import parse_listing_date

class TestParseListingDate(unittest.TestCase):

    def test_empty_and_invalid_date(self):
        self.assertIsNone(parse_listing_date(None))
        self.assertIsNone(parse_listing_date(""))
        self.assertIsNone(parse_listing_date("not a date"))
        self.assertIsNone(parse_listing_date("12345"))

    def test_standard_date_formats(self):
        # YYYY-MM-DD (January is EST, UTC-5)
        expected_utc = pendulum.datetime(2025, 1, 18, 5, 0, 0, tz='UTC')
        self.assertEqual(expected_utc, parse_listing_date("2025-01-18"))
        
        # MM/DD/YYYY
        self.assertEqual(expected_utc, parse_listing_date("1/18/2025"))

        # Month Day, YYYY
        self.assertEqual(expected_utc, parse_listing_date("January 18, 2025"))

    def test_date_with_time_and_timezone(self):
        # With day of week, month, day, time, and timezone (June is EDT, UTC-4)
        expected_utc = pendulum.datetime(2025, 6, 28, 23, 0, 0, tz='UTC')
        self.assertEqual(expected_utc, parse_listing_date("Saturday, June 28, 7:00 PM ET"))
        
        # Month Day at HH:MM AM/PM (June is EDT, UTC-4)
        with patch('pendulum.now', return_value=pendulum.datetime(2025, 1, 1, tz='America/New_York')):
             self.assertEqual(expected_utc, parse_listing_date("June 28 at 7:00 PM"))

    def test_abbreviated_and_alternative_formats(self):
        # Abbreviated month with time (September is EDT, UTC-4)
        expected_utc = pendulum.datetime(2025, 9, 13, 22, 0, 0, tz='UTC')
        self.assertEqual(expected_utc, parse_listing_date("Sat Sep 13, 6pm, 2025"))

        # Dotted format with time (June is EDT, UTC-4)
        expected_utc = pendulum.datetime(2025, 6, 28, 22, 30, 0, tz='UTC')
        self.assertEqual(expected_utc, parse_listing_date("Saturday 06.28.2025 at 06:30 PM"))

    @patch('pendulum.now', return_value=pendulum.datetime(2025, 3, 15, tz='America/New_York'))
    def test_date_without_year(self, mock_now):
        # Date is in the future relative to mocked now (August is EDT, UTC-4)
        expected_utc = pendulum.datetime(2025, 8, 10, 4, 0, 0, tz='UTC')
        self.assertEqual(expected_utc, parse_listing_date("August 10"))

    @patch('pendulum.now', return_value=pendulum.datetime(2025, 10, 1, tz='America/New_York'))
    def test_date_without_year_wraps_to_next_year(self, mock_now):
        # Date is in the past relative to mocked now, so should be for the next year (February is EST, UTC-5)
        expected_utc = pendulum.datetime(2026, 2, 20, 5, 0, 0, tz='UTC')
        self.assertEqual(expected_utc, parse_listing_date("February 20"))

    def test_messy_strings(self):
        # Extra spaces (January is EST, UTC-5)
        expected_utc = pendulum.datetime(2025, 1, 18, 5, 0, 0, tz='UTC')
        self.assertEqual(expected_utc, parse_listing_date("  January   18,    2025  "))
        
        # Missing comma
        self.assertEqual(expected_utc, parse_listing_date("January 18 2025"))

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)