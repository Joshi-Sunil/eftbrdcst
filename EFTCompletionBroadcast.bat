echo off
echo Are you sure you want to run the EFT broadcast?
pause

date /t
time /t

schtasks /run /tn EFTBrdcst /s af9cs1g1

echo If not successful, contact Brion(614-933-5015 ) @ brion.jones@mckesson.com
pause

echo on
