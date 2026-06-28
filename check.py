import sys

try:
    with open('dashboard/js/app.js', 'r', encoding='utf-8') as f:
        text = f.read()
except Exception as e:
    print('Read error:', e)
    sys.exit(1)

level = 0
for i, c in enumerate(text):
    if c == '{':
        level += 1
    elif c == '}':
        level -= 1
    if level < 0:
        print(f'Extra }} at index {i}')
        sys.exit(1)

print('Level at end:', level)
