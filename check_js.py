import subprocess
import sys
try:
    import esprima
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'esprima'])
    import esprima

with open('dashboard/js/app.js', 'r', encoding='utf-8') as f:
    text = f.read()

try:
    esprima.parseScript(text)
    print('No syntax errors!')
except Exception as e:
    print('SyntaxError:', e)
