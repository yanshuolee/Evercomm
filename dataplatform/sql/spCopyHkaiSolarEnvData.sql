USE iotmgmt;

DELIMITER $$
CREATE DEFINER=`ecoadmin`@`%` PROCEDURE `spCopyHkaiSolarEnvData`()
BEGIN

declare vTs datetime(3);
declare vGatewayId int;
declare vIeee varchar(16);
declare vReceivedSync datetime;
declare vain1 int;
declare vain2 int;
declare vain3 int;
declare vain4 int;
declare vain5 int;
declare vValue1 double;
declare vValue2 double;
declare vValue3 double;
declare vValue4 double;
declare vValue5 double;

declare finished int;


declare curQuery cursor for
  SELECT 
	`ts`, `gatewayId`, `ieee`, date_format(`receivedSync`, '%Y-%m-%d %H:%i:00') as `receivedSync`, 
    `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,
	`value1`, `value2`, `value3`, `value4`, `value5`
	FROM
		iotmgmt.ain
	WHERE
		ieee IN (
          select ieee 
          from iotmgmt.TDevice 
          where gatewayId in (40,41,42,44,45) 
          and deviceAttribId = 9)  -- 日照計 attribId = 9
        and receivedSync >= now() - interval 10 minute
        and (date_format(receivedSync, '%i') % 2) = 0
        group by ieee, date_format(receivedSync, '%Y'), date_format(receivedSync, '%m'), date_format(receivedSync, '%d'), date_format(receivedSync, '%H'), date_format(receivedSync, '%i');

declare curQueryThermistor cursor for
  SELECT 
	`ts`, `gatewayId`, `ieee`, date_format(`receivedSync`, '%Y-%m-%d %H:%i:00'),
    `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,
	`value1`, `value2`, `value3`, `value4`, `value5`
	FROM
		iotmgmt.ain
	WHERE
		ieee IN (
          select ieee
          from iotmgmt.TDevice 
          where gatewayId in (40,41,42,44,45) 
          and deviceAttribId in (6,10))  -- 環境溫度計 attribId = 6, 太陽能板溫度 attribId = 10
        and receivedSync >= now() - interval 10 minute
        and (date_format(receivedSync, '%i') % 2) = 0
        group by ieee, date_format(receivedSync, '%Y'), date_format(receivedSync, '%m'), date_format(receivedSync, '%d'), date_format(receivedSync, '%H'), date_format(receivedSync, '%i');

-- declare NOT FOUND handler
DECLARE CONTINUE HANDLER 
	FOR NOT FOUND SET finished = 1;

set finished = 0;
/* Call ain calculation per row */
-- Insert Radiator data into dataplatform
open curQuery;
readLoop: loop
	fetch curQuery into vTs, vGatewayId, vIeee, vReceivedSync, vain1, vain2, vain3, vain4, vain5,
	vValue1, vValue2, vValue3, vValue4, vValue5;
	if finished = 1 then leave readLoop; end if;
    
    -- skip calculation if data already calculated
	-- if vain1 is not null and vain2 is not null and vain3 is not null and vain4 is not null and vain5 is not null then
	--  CALL iotmgmt.spCalculateAinValues(vIeee, vain1, vain2, vain3, vain4, vain5,
	-- 									vValue1, vValue2, vValue3, vValue4, vValue5, @msg);
	-- end if;
    
	REPLACE INTO `dataplatform`.`environment` (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,
                                              `value1`, `value2`, `value3`, `value4`, `value5`)
	VALUES (vTs, vGatewayId, vIeee, vReceivedSync, vain1, vain2, vain3, vain4, vain1,
	vValue1, vValue2, vValue3, vValue4, vValue1);
end loop readLoop;
close curQuery;

set finished = 0;
/* Call ain calculation per row */
-- Insert temperature data into dataplatform
open curQueryThermistor;
readLoop2: loop
	fetch curQueryThermistor into vTs, vGatewayId, vIeee, vReceivedSync, vain1, vain2, vain3, vain4, vain5,
	vValue1, vValue2, vValue3, vValue4, vValue5;
	if finished = 1 then leave readLoop2; end if;
	
	-- CALL iotmgmt.spCalculateAinValues(vIeee, vain1, vain2, vain3, vain4, vain5,
	-- 								  vValue1, vValue2, vValue3, vValue4, vValue5, @msg);
    
	REPLACE INTO `dataplatform`.`environment` (`ts`,`gatewayId`, `ieee`, `receivedSync`, `ain1`, `ain2`, `ain3`, `ain4`, `ain5`,
                                              `value1`, `value2`, `value3`, `value4`, `value5`)
	VALUES (vTs, vGatewayId, vIeee, vReceivedSync, vain1, vain2, vain3, vain4, vain5,
	vValue1, vValue2, vValue3, vValue4, vValue5);
end loop readLoop2;
close curQueryThermistor;

END$$
DELIMITER ;
