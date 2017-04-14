REM echo off

set /a m=0
set /a start=0
set /a end=40
:loop2 
set /a app=3
:loop1
java -Xmx10g -Xms10g -XX:+UseG1GC cacheHitSimulator %start% %end% %app% %m%

set /a "app = app + 1"
if %app% LEQ 3 goto loop1

set /a "start = start + 40"
set /a "end = end + 40"
if %end% LEQ 10000 goto loop2

