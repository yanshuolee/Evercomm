USE iotmgmt;

DELIMITER $$
CREATE DEFINER=`ecoadmin`@`%` PROCEDURE `spCopyInverterAEC5KWToDataplatform`()
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
  ',"voltage2":', (dcVoltage ->"$[1]"), '}') as dcVoltage,
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
  '}'
  ) as faultAlarmCode,
  -- errorCode,
  dailyOperationMinute,
  monthlyKWh,
  CONCAT(
  '{"current1":', round((dcCurrent->"$[0]"), 2), 
  ',"current2":', round((dcCurrent->"$[1]"), 2),'}') as dcCurrent,
  CONCAT (
  '{"data1":', groundResistance->"$[0]", ',' ,
  '"data2":', groundResistance->"$[1]",
  '}'
  ) as groundResistance
  from iotmgmt.solarInverter2
  where gatewayId in (40,42)
  and receivedSync >= now() - interval 10 minute
  and (date_format(receivedSync, '%i') % 2) = 0
  group by ieee, date_format(receivedSync, '%Y'), date_format(receivedSync, '%m'), date_format(receivedSync, '%d'), date_format(receivedSync, '%H'), date_format(receivedSync, '%i');
END$$
DELIMITER ;
