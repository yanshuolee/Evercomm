USE iotmgmt;

DROP EVENT IF EXISTS `copyAinDataToDataPlatformJob`;
delimiter //
CREATE DEFINER=`ecoadmin`@`%` EVENT `copyAinDataToDataPlatformJob`
ON SCHEDULE EVERY 2 MINUTE 
STARTS '2020-11-16 13:04:00' 
ON COMPLETION PRESERVE 
ENABLE 
DO 
begin
call iotmgmt.spCopyHkaiSolarEnvData();
end //
delimiter ;