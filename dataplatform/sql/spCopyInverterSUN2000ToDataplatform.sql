USE iotmgmt;

DELIMITER $$
CREATE DEFINER=`ecoadmin`@`%` PROCEDURE `spCopyInverterSUN2000ToDataplatform`()
BEGIN

replace into dataplatform.solarInverter (`ts`,`gatewayId`,`ieee`,`receivedSync`,`energyProducedToday`,
        `energyProducedLifeTime`,`totalOperationHourLifeTime`,`internalTemperature`,
        `dcVoltage`,`totalDCPower`,`phaseAVoltage`,`phaseBVoltage`,`phaseCVoltage`,
        `phaseACurrent`,`phaseBCurrent`,`phaseCCurrent`,`totalApparentPower`,
        `totalActivePower`,`reactivePower`,`powerFactor`,`gridFrequency`,`operationState`,
        `faultAlarmCode`,`dailyOperationTime`,`monthlyEnergy`,`dcCurrent`, `groundResistance`)
  select 
  ts, 
  gatewayId, 
  ieee, 
  date_format(receivedSync, '%Y-%m-%d %H:%i:00') as receivedSync,
  dailyKWh, 
  lifeTimeKwh, 
  lifeTimeHour,
  CONCAT('{"temp1":', (Temperature ->"$[0]"), '}') as temperature ,
CONCAT(
  '{"voltage1":', (dcVoltage ->"$[0]"), 
  ',"voltage2":', (dcVoltage ->"$[1]"), 
  ',"voltage3":', (dcVoltage ->"$[2]"), 
  ',"voltage4":', (dcVoltage ->"$[3]"), 
  ',"voltage5":', (dcVoltage ->"$[4]"), 
  ',"voltage6":', (dcVoltage ->"$[5]"), 
  ',"voltage7":', (dcVoltage ->"$[6]"), 
  ',"voltage8":', (dcVoltage ->"$[7]"), '}') as dcVoltage,
  dcPower, 
  acVoltageA,
  acVoltageB,
  acVoltageC,
  acCurrentA,
  acCurrentB,
  acCurrentC,
  apparentPower,
  acPower,
  reactivePower,
  pf,
  gridFrequency,
  operationState,
  CONCAT (
  '{"errorcode1":', errorCode->"$[0]",
  ',"errorcode2":', errorCode->"$[1]",
  ',"errorcode3":', errorCode->"$[2]",
  ',"errorcode4":', errorCode->"$[3]",
  ',"errorcode5":', errorCode->"$[4]",
  ',"errorcode6":', errorCode->"$[5]",
  ',"errorcode7":', errorCode->"$[6]",
  ',"errorcode8":', errorCode->"$[7]",
  ',"errorcode9":', errorCode->"$[8]",
  ',"errorcode10":', errorCode->"$[9]",
  ',"errorcode11":', errorCode->"$[10]",
  '}'
  ) as faultAlarmCode,
  -- errorCode,
  dailyOperationMinute,
  monthlyKWh,
  CONCAT(
  '{"current1":', round((dcCurrent->"$[0]"), 2), 
  ',"current2":', round((dcCurrent->"$[1]"), 2),
  ',"current3":', round((dcCurrent->"$[2]"), 2),
  ',"current4":', round((dcCurrent->"$[3]"), 2),
  ',"current5":', round((dcCurrent->"$[4]"), 2),
  ',"current6":', round((dcCurrent->"$[5]"), 2),
  ',"current7":', round((dcCurrent->"$[6]"), 2),
  ',"current8":', round((dcCurrent->"$[7]"), 2),'}') as dcCurrent,
  CONCAT (
  '{"data1":', groundResistance->"$[0]", '}'
  ) as groundResistance
  from iotmgmt.solarInverter2
  where gatewayId in (44,45)
  and receivedSync >= now() - interval 10 minute
  and (date_format(receivedSync, '%i') % 2) = 0
  group by ieee, date_format(receivedSync, '%Y'), date_format(receivedSync, '%m'), date_format(receivedSync, '%d'), date_format(receivedSync, '%H'), date_format(receivedSync, '%i');
END$$
DELIMITER ;
