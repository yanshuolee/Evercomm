logtime=$(date "+%Y-%m-%d")

year=$(date "+%Y")
month=$(date "+%m")
day=$(date "+%d")

echo "
#
20 19 * * * python3 -u /home/ecoetl/solarInverterTodataplatform_v1.py > /home/ecoetl/logs/solar_$logtime.log
15 19 * * * python3 -u /home/ecoetl/ainToEnvironment_v1.py > /home/ecoetl/logs/env_$logtime.log
*/2 * * * * python3 -u /home/ecoetl/AcbeltoDataplatform.py >> /home/ecoetl/logs/integration_$logtime.log
@reboot sh /home/ecoetl/api/SunEdgeDataFetch.sh &
0 */1 * * * cd /home/ecoetl/api && bash KillSunEdgeDataFetch.sh &
0 3 * * * cd /home/ecoetl/logs && rm SunEdgeDataFetch.log.*
12 * * * * cd /home/ecoetl/api && java -jar GetInso.jar &
0 3 * * * cd /home/ecoetl/logs && rm GetInso.log.*
#
#  update time
30 0 * * * bash /home/ecoetl/crontab.process.sh
#" > ./process.crontab

cat ./process.crontab |crontab
rm ./process.crontab
