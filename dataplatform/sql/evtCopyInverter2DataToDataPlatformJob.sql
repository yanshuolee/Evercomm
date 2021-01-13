USE iotmgmt;

DROP EVENT IF EXISTS `copyInverter2DataToDataPlatformJob`;
delimiter //
CREATE DEFINER=`ecoadmin`@`%` EVENT `copyInverter2DataToDataPlatformJob` 
ON SCHEDULE EVERY 2 MINUTE 
STARTS '2020-11-16 13:04:00' 
ON COMPLETION PRESERVE 
ENABLE 
DO 
begin
call iotmgmt.spCopyInverterSUN2000ToDataplatform();
call iotmgmt.spCopyInverterAEC5KWToDataplatform();
end //
delimiter ;