for /l %%x in (1, 1, 1000) do (
   echo %%x
   java -jar memtest.jar %%x >> memtest_results.txt
)



