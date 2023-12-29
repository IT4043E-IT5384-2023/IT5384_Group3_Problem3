import re

def split_time_string(time_string):
    if time_string == "":
        time_string = "0 days 00:00:00"
    days_match = re.search(r"(.*?) days", time_string)
    print(days_match)
    days = days_match.group(1)
    time_part = time_string[days_match.end():]

    # Split time part
    hours, minutes, seconds = time_part.split(":")

    # Convert to integers
    days = int(days)
    hours = int(hours)
    minutes = int(minutes)
    seconds = int(seconds)

    return days, hours, minutes, seconds

time_string = ""
days, hours, minutes, seconds = split_time_string(time_string)

print(days)  # Output: 0
print(hours)  # Output: 0
print(minutes)  # Output: 26
print(seconds)  # Output: 51
