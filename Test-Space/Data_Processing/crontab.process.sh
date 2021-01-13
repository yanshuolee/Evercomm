logtime=$(date "+%Y-%m-%d")

year=$(date "+%Y")
month=$(date "+%m")
day=$(date "+%d")

echo "MAILTO=dwyang@evercomm.com
#
20 19 * * * python3 -u /home/ecoetl/solarInverterTodataplatform_v1.py > /home/ecoetl/logs/solar_$logtime.log
15 19 * * * python3 -u /home/ecoetl/ainToEnvironment_v1.py > /home/ecoetl/logs/env_$logtime.log
#" > ./process.crontab

cat ./process.crontab |crontab
rm ./process.crontab
