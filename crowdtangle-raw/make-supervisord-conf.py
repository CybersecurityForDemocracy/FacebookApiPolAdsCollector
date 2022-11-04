items = {
    'ua': 'spFrfvaX56pXvKsRGt8UN1LK33S0U8l0TwjxmaYY 0k5FPEX3EV7crkAMrbaeWqFjwchdHyJH71SK7nM0 DJptgsv6WmQcROcj7glNOTxq3Op1kZct1jYTlfkh FQJbW7DgbIjvNy9nZTmVR7MfyB2GcaCdL3y6RUFM AqLpFjbDh6MQgzWZWvqLkLSRAWUxb12yw9t0yQSa jCU3gMBo7fdagceaV08XUDMcgfsDOb6XCXHxfQDw EF8o2IrxaVAeZiGAJ3wM6ZIGhPQRIi6C0fwMEwPw tIf6WmSHM667px7AtZbskngL85nWBRWb7w27FmZ7 TsvmgPssQvJhcdUlqky552KD5gJw5mLXq4AZj5YZ'.split(),
    'ru': 'eMogRPH9yU6DgDwZrAyRPzJTzhIOdYRVzMjlOumu ywdFLjuJHqegg80rIPmJ8bWYQGbFMQHsj0y1uvxr yqeXV81sdVV2zl4XC47nn26p15sw2emDc1ypfRXb'.split(),
    'other': 'EMahqn2XAaxSVGk8HLyoo7BKvmEPEuvofggptejc 0lE7XLNypvq1BmuIypVLOFQzkMuaF2XkwkRHflSc ancWbxBrn0SzI9SbE8r1r47zwXfymdfPgEEcN8c1 6uAgDi7qngJ9ydvtpuXrkYPPmx4MmXi1Ui8ts8J8 fD8YtNb1FL9YQwnaNoI6wJKtkAAkESDdAX27ALiZ qCaBF6FChgjtzNLCMlr8X2mPFJIMTDHi7a25axDh DqvmxClea3103SgBMTW9OrBI92rKnshxvUEeJ9Vr'.split(),
}

print("[supervisord]")
print("childlogdir=%(here)s/logs")
print()

for k in items:
    for i, t in enumerate(items[k]):
        print(f"[program:fetch-{k}-{i:02d}]")
        print(f"command=poetry run python fetch-and-save.py --tag %(program_name)s --directory collected {t}")
        print(f"autorestart=true")
        print()
