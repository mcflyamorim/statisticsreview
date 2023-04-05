Clear-Host

$ScriptPath = split-path -parent $MyInvocation.MyCommand.Definition

& "$ScriptPath\ExportStatisticsChecksToExcel.ps1" -SQLInstance "DELLFABIANO\SQL2019" -LogFilePath "C:\temp\"
