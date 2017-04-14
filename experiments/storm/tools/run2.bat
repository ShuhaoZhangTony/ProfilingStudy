REM echo off


set /a app=4
:loop1
java -Xmx10g -Xms10g -XX:+UseG1GC cacheHitSimulator2 %app%

set /a "app = app + 1"
if %app% LEQ 10 goto loop1

