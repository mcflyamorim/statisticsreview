Clear-Host

$ScriptPath = split-path -parent $MyInvocation.MyCommand.Definition

& "$ScriptPath\ExportStatisticsChecksToExcel.ps1" -SQLInstance "DELLFABIANO\SQL2022" -LogFilePath "C:\temp\tmp" -Force_sp_GetStatisticInfo_Execution
