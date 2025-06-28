import pendulum

# Test different date formats
test_formats = [
    "11.02.2024 at 05:00 PM",
    "11/02/2024 at 05:00 PM",
    "2024-11-02 at 05:00 PM",
    "November 2, 2024 at 05:00 PM",
    "Nov 2, 2024 at 05:00 PM",
    "Saturday, November 2, 2024 at 05:00 PM",
    "Saturday Nov 2, 2024 at 05:00 PM",
]

print("Testing different date formats:")
for date_str in test_formats:
    print(f"\nInput: {date_str}")
    try:
        parsed_et = pendulum.parse(date_str, tz='America/New_York')
        print(f"Parsed in ET: {parsed_et}")
        parsed_utc = parsed_et.in_timezone('UTC')
        print(f"Converted to UTC: {parsed_utc}")
    except Exception as e:
        print(f"Error: {e}") 